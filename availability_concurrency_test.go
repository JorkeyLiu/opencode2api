package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The availability RW gate: singles share read locks, batches exclude.
func TestAvailRWGateSemantics(t *testing.T) {
	_, _, _, _ = bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	manager2, _, _, _ := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	gw := manager2.current.Load().gateway
	// A held single (read) still admits another single read.
	gw.bulkMu.RLock()
	if !gw.bulkMu.TryRLock() {
		gw.bulkMu.RUnlock()
		t.Fatal("second single TryRLock must succeed while a single is active")
	}
	gw.bulkMu.RUnlock()
	// But a batch write must fail while singles are active.
	if gw.bulkMu.TryLock() {
		gw.bulkMu.Unlock()
		gw.bulkMu.RUnlock()
		t.Fatal("batch TryLock must fail while singles are active")
	}
	gw.bulkMu.RUnlock()
	// A held batch (write) rejects both singles and batches.
	gw.bulkMu.Lock()
	if gw.bulkMu.TryRLock() {
		gw.bulkMu.RUnlock()
		gw.bulkMu.Unlock()
		t.Fatal("single TryRLock must fail while a batch is active")
	}
	if gw.bulkMu.TryLock() {
		gw.bulkMu.Unlock()
		gw.bulkMu.Unlock()
		t.Fatal("batch TryLock must fail while a batch is active")
	}
	gw.bulkMu.Unlock()
}

// All six endpoints share the fail-fast 409 bulk_busy behavior across the
// gate in both directions.
func TestAvailBatchSingleMutualExclusion(t *testing.T) {
	_, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	gw := admin.manager.current.Load().gateway
	seedBulkProbeCatalog(gw)
	fp := credentialFingerprint(TierZen, gw.authCreds[0].key)
	gw.cfg.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "c1", Name: "c1", BaseURL: "https://api.example.com", APIKey: "k", Model: "cm"},
	}}
	singles := []struct{ path, body string }{
		{"/api/availability/check-node", `{"pool":"shared","index":0}`},
		{"/api/availability/check-credential", `{"fingerprint":"` + fp + `"}`},
		{"/api/availability/check-custom", `{"id":"c1"}`},
	}
	batches := []string{
		"/api/availability/check",
		"/api/availability/check-credentials",
		"/api/availability/check-customs",
	}
	// Held singles (read) reject every batch with 409 bulk_busy.
	gw.bulkMu.RLock()
	for _, path := range batches {
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, path, `{}`, token, csrf))
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "bulk_busy") {
			gw.bulkMu.RUnlock()
			t.Fatalf("%s during single must be 409 bulk_busy, got %d %s", path, rec.Code, rec.Body.String())
		}
	}
	gw.bulkMu.RUnlock()
	// Held batch (write) rejects every single and every batch with 409.
	gw.bulkMu.Lock()
	for _, s := range singles {
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, s.path, s.body, token, csrf))
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "bulk_busy") {
			gw.bulkMu.Unlock()
			t.Fatalf("%s during batch must be 409 bulk_busy, got %d %s", s.path, rec.Code, rec.Body.String())
		}
	}
	for _, path := range batches {
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, path, `{}`, token, csrf))
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "bulk_busy") {
			gw.bulkMu.Unlock()
			t.Fatalf("%s during batch must be 409 bulk_busy, got %d %s", path, rec.Code, rec.Body.String())
		}
	}
	gw.bulkMu.Unlock()
}

// Independent singles overlap: two custom singles on different channels run
// concurrently and both succeed (never 409 solely due to concurrency).
func TestOverlappingCustomSinglesSucceed(t *testing.T) {
	var entered atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered.Add(1)
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}]}`))
	}))
	defer srv.Close()
	_, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	gw := admin.manager.current.Load().gateway
	gw.cfg.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "c1", Name: "c1", BaseURL: srv.URL, APIKey: "k1", Model: "m1", Protocol: ProtocolChat},
		{ID: "c2", Name: "c2", BaseURL: srv.URL, APIKey: "k2", Model: "m2", Protocol: ProtocolChat},
	}}
	gw.customClient = srv.Client()
	type outcome struct {
		code int
		body string
	}
	results := make([]outcome, 2)
	var wg sync.WaitGroup
	for i, id := range []string{"c1", "c2"} {
		wg.Add(1)
		go func(idx int, cid string) {
			defer wg.Done()
			rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"id":"`+cid+`"}`, token, csrf))
			results[idx] = outcome{code: rec.Code, body: rec.Body.String()}
		}(i, id)
	}
	// Wait until both probes are in flight, then release together.
	deadline := time.Now().Add(5 * time.Second)
	for entered.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if entered.Load() < 2 {
		close(release)
		wg.Wait()
		t.Fatalf("both singles must overlap in flight, entered=%d results=%+v", entered.Load(), results)
	}
	close(release)
	wg.Wait()
	for i, r := range results {
		if r.code != http.StatusOK {
			t.Fatalf("single %d must succeed concurrently, got %d %s", i, r.code, r.body)
		}
		if strings.Contains(r.body, "bulk_busy") {
			t.Fatalf("single %d must not report bulk_busy: %s", i, r.body)
		}
	}
	snap := gw.bulkSnapshot.Load()
	if snap == nil {
		t.Fatal("snapshot must exist after concurrent singles")
	}
	seen := map[string]bool{}
	for _, c := range snap.Custom {
		seen[customAvailabilityKey(c)] = true
	}
	if !seen["c1"] || !seen["c2"] {
		t.Fatalf("both custom rows must be preserved, got %+v", snap.Custom)
	}
}

// Mixed singles overlap: a node single and a custom single run together.
func TestOverlappingMixedSinglesSucceed(t *testing.T) {
	var zenEntered atomic.Int32
	zenRelease := make(chan struct{})
	zenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		zenEntered.Add(1)
		select {
		case <-zenRelease:
		case <-time.After(10 * time.Second):
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer zenSrv.Close()
	var customEntered atomic.Int32
	customRelease := make(chan struct{})
	customSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customEntered.Add(1)
		select {
		case <-customRelease:
		case <-time.After(10 * time.Second):
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}]}`))
	}))
	defer customSrv.Close()
	_, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	gw := admin.manager.current.Load().gateway
	gw.cfg.Upstream.Zen = zenSrv.URL
	gw.cfg.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "c1", Name: "c1", BaseURL: customSrv.URL, APIKey: "k1", Model: "m1", Protocol: ProtocolChat},
	}}
	gw.customClient = customSrv.Client()
	// Point the direct proxy transport at the blocking zen server.
	if pool := gw.pools["shared"]; pool != nil {
		for _, px := range pool.items {
			if px != nil {
				px.client.Transport = &blockingRoundTripper{
					entered: &zenEntered,
					release: zenRelease,
					status:  200,
					body:    bulkChatSuccessBody("bulk-free-model"),
				}
			}
		}
	}
	seedBulkProbeCatalog(gw)
	type outcome struct {
		code int
		body string
	}
	var nodeOut, customOut outcome
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"shared","index":0}`, token, csrf))
		nodeOut = outcome{code: rec.Code, body: rec.Body.String()}
	}()
	go func() {
		defer wg.Done()
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"id":"c1"}`, token, csrf))
		customOut = outcome{code: rec.Code, body: rec.Body.String()}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for (zenEntered.Load() < 1 || customEntered.Load() < 1) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if zenEntered.Load() < 1 || customEntered.Load() < 1 {
		close(zenRelease)
		close(customRelease)
		wg.Wait()
		t.Fatalf("mixed singles must overlap: zen=%d custom=%d node=%+v custom=%+v",
			zenEntered.Load(), customEntered.Load(), nodeOut, customOut)
	}
	close(zenRelease)
	close(customRelease)
	wg.Wait()
	if nodeOut.code != http.StatusOK {
		t.Fatalf("node single must succeed concurrently, got %d %s", nodeOut.code, nodeOut.body)
	}
	if customOut.code != http.StatusOK {
		t.Fatalf("custom single must succeed concurrently, got %d %s", customOut.code, customOut.body)
	}
}

type blockingRoundTripper struct {
	entered *atomic.Int32
	release <-chan struct{}
	status  int
	body    string
}

func (b *blockingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	b.entered.Add(1)
	select {
	case <-b.release:
	case <-time.After(10 * time.Second):
	}
	resp := &http.Response{
		StatusCode: b.status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(b.body)),
		Request:    req,
	}
	return resp, nil
}

// Batch started while a single is active returns 409 (live, not gate-only).
func TestLiveBatchRejectedWhileSingleActive(t *testing.T) {
	release := make(chan struct{})
	var entered atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered.Add(1)
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}]}`))
	}))
	defer srv.Close()
	_, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	gw := admin.manager.current.Load().gateway
	gw.cfg.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "c1", Name: "c1", BaseURL: srv.URL, APIKey: "k1", Model: "m1", Protocol: ProtocolChat},
	}}
	gw.customClient = srv.Client()
	done := make(chan int, 1)
	go func() {
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"id":"c1"}`, token, csrf))
		done <- rec.Code
	}()
	deadline := time.Now().Add(5 * time.Second)
	for entered.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if entered.Load() < 1 {
		close(release)
		t.Fatal("single must enter blocking send")
	}
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-customs", `{}`, token, csrf))
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "bulk_busy") {
		close(release)
		<-done
		t.Fatalf("batch during active single must be 409 bulk_busy, got %d %s", rec.Code, rec.Body.String())
	}
	close(release)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("released single must succeed, got %d", code)
	}
}

// Single started while a batch is active returns 409 (live, not gate-only).
func TestLiveSingleRejectedWhileBatchActive(t *testing.T) {
	release := make(chan struct{})
	var entered atomic.Int32
	zenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered.Add(1)
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer zenSrv.Close()
	_, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	gw := admin.manager.current.Load().gateway
	gw.cfg.Upstream.Zen = zenSrv.URL
	gw.cfg.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "c1", Name: "c1", BaseURL: "https://api.example.com", APIKey: "k", Model: "cm"},
	}}
	seedBulkProbeCatalog(gw)
	if pool := gw.pools["shared"]; pool != nil {
		for _, px := range pool.items {
			if px != nil {
				px.client.Transport = &blockingRoundTripper{
					entered: &entered,
					release: release,
					status:  200,
					body:    bulkChatSuccessBody("bulk-free-model"),
				}
			}
		}
	}
	done := make(chan int, 1)
	go func() {
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{}`, token, csrf))
		done <- rec.Code
	}()
	deadline := time.Now().Add(5 * time.Second)
	for entered.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if entered.Load() < 1 {
		close(release)
		t.Fatal("batch must enter blocking sends")
	}
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"id":"c1"}`, token, csrf))
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "bulk_busy") {
		close(release)
		<-done
		t.Fatalf("single during active batch must be 409 bulk_busy, got %d %s", rec.Code, rec.Body.String())
	}
	close(release)
	<-done
}

// Concurrent batches stay single-flight: the second batch fails fast.
func TestConcurrentBatchesSingleFlight(t *testing.T) {
	release := make(chan struct{})
	var entered atomic.Int32
	zenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered.Add(1)
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer zenSrv.Close()
	_, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	gw := admin.manager.current.Load().gateway
	gw.cfg.Upstream.Zen = zenSrv.URL
	seedBulkProbeCatalog(gw)
	if pool := gw.pools["shared"]; pool != nil {
		for _, px := range pool.items {
			if px != nil {
				px.client.Transport = &blockingRoundTripper{
					entered: &entered,
					release: release,
					status:  200,
					body:    bulkChatSuccessBody("bulk-free-model"),
				}
			}
		}
	}
	done := make(chan int, 1)
	go func() {
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{}`, token, csrf))
		done <- rec.Code
	}()
	deadline := time.Now().Add(5 * time.Second)
	for entered.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if entered.Load() < 1 {
		close(release)
		t.Fatal("first batch must enter blocking sends")
	}
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credentials", `{}`, token, csrf))
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "bulk_busy") {
		close(release)
		<-done
		t.Fatalf("second batch must be 409 bulk_busy, got %d %s", rec.Code, rec.Body.String())
	}
	close(release)
	<-done
}

// Concurrent single merges must not lose independent rows.
func TestConcurrentSingleMergesPreserved(t *testing.T) {
	_, admin, _, _ := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-1", "zen-key-2"}, nil)
	gw := admin.manager.current.Load().gateway
	base := time.Now().UTC()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ts := base.Add(time.Duration(n) * time.Millisecond)
			// Distinct pool-qualified rows, mirroring real scoped merges
			// where each index redacts to a distinct proxy node.
			row := bulkNodeAvailability{
				Pool: "shared", Index: n,
				ProxyNode:   "node-" + string(rune('0'+n)),
				Transport:   "healthy",
				Anonymous:   "success",
				LastChecked: &ts,
			}
			gw.mergeScopedSnapshotRows(ts, []bulkNodeAvailability{row}, nil)
		}(i)
	}
	for _, id := range []string{"c1", "c2", "c3", "c4"} {
		wg.Add(1)
		go func(cid string) {
			defer wg.Done()
			ts := time.Now().UTC()
			gw.mergeCustomSnapshotRow(ts, bulkCustomAvailability{
				ID: cid, Name: cid, BaseURL: "https://example.com",
				Model: "m", Status: "available", Reason: "success",
				LastChecked: &ts,
			})
		}(id)
	}
	// Credential rows for two distinct credentials merge concurrently.
	creds := append([]credentialRef(nil), gw.credentials()...)
	for _, c := range creds {
		wg.Add(1)
		go func(cr credentialRef) {
			defer wg.Done()
			ts := time.Now().UTC()
			gw.mergeCredentialSnapshotRow(ts, bulkCredentialAvailability{
				Tier: string(TierZen), KeyTail: cr.display,
				Fingerprint: credentialFingerprint(TierZen, cr.key),
				Pool:        gw.authPoolName(),
				Status:      "available", Reason: "success",
				LastChecked: &ts, CredID: cr.id,
			})
		}(c)
	}
	wg.Wait()
	snap := gw.bulkSnapshot.Load()
	if snap == nil {
		t.Fatal("snapshot must exist after concurrent merges")
	}
	if len(snap.Nodes) < 8 {
		t.Fatalf("scoped merges lost rows: got %d want >=8", len(snap.Nodes))
	}
	seenCustom := map[string]bool{}
	for _, c := range snap.Custom {
		seenCustom[customAvailabilityKey(c)] = true
	}
	for _, id := range []string{"c1", "c2", "c3", "c4"} {
		if !seenCustom[id] {
			b, _ := json.Marshal(snap.Custom)
			t.Fatalf("custom row %s lost: %s", id, string(b))
		}
	}
	if len(snap.Credentials) < len(creds) {
		t.Fatalf("credential merges lost rows: got %d want %d", len(snap.Credentials), len(creds))
	}
}

// Same-key singles overlap: two node singles on the same pool+index run
// concurrently and both are accepted (200), never 409 solely due to
// concurrency. Per-button same-tab duplicate protection stays a frontend
// concern.
func TestOverlappingSameNodeSinglesSucceed(t *testing.T) {
	var entered atomic.Int32
	release := make(chan struct{})
	_, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	gw := admin.manager.current.Load().gateway
	seedBulkProbeCatalog(gw)
	if pool := gw.pools["shared"]; pool != nil {
		for _, px := range pool.items {
			if px != nil {
				px.client.Transport = &blockingRoundTripper{
					entered: &entered,
					release: release,
					status:  200,
					body:    bulkChatSuccessBody("bulk-free-model"),
				}
			}
		}
	}
	type outcome struct {
		code int
		body string
	}
	results := make([]outcome, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"shared","index":0}`, token, csrf))
			results[idx] = outcome{code: rec.Code, body: rec.Body.String()}
		}(i)
	}
	deadline := time.Now().Add(5 * time.Second)
	for entered.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if entered.Load() < 2 {
		close(release)
		wg.Wait()
		t.Fatalf("same-node singles must overlap in flight, entered=%d results=%+v", entered.Load(), results)
	}
	close(release)
	wg.Wait()
	for i, r := range results {
		if r.code != http.StatusOK {
			t.Fatalf("same-node single %d must succeed concurrently, got %d %s", i, r.code, r.body)
		}
		if strings.Contains(r.body, "bulk_busy") {
			t.Fatalf("same-node single %d must not report bulk_busy: %s", i, r.body)
		}
	}
}

// Same-key singles overlap: two credential singles on the same fingerprint
// run concurrently and both are accepted (200).
func TestOverlappingSameCredentialSinglesSucceed(t *testing.T) {
	var entered atomic.Int32
	release := make(chan struct{})
	_, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	gw := admin.manager.current.Load().gateway
	seedBulkProbeCatalog(gw)
	fp := credentialFingerprint(TierZen, gw.authCreds[0].key)
	if pool := gw.pools["shared"]; pool != nil {
		for _, px := range pool.items {
			if px != nil {
				px.client.Transport = &blockingRoundTripper{
					entered: &entered,
					release: release,
					status:  200,
					body:    bulkChatSuccessBody("bulk-free-model"),
				}
			}
		}
	}
	body := `{"fingerprint":"` + fp + `"}`
	type outcome struct {
		code int
		body string
	}
	results := make([]outcome, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", body, token, csrf))
			results[idx] = outcome{code: rec.Code, body: rec.Body.String()}
		}(i)
	}
	deadline := time.Now().Add(5 * time.Second)
	for entered.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if entered.Load() < 2 {
		close(release)
		wg.Wait()
		t.Fatalf("same-credential singles must overlap in flight, entered=%d results=%+v", entered.Load(), results)
	}
	close(release)
	wg.Wait()
	for i, r := range results {
		if r.code != http.StatusOK {
			t.Fatalf("same-credential single %d must succeed concurrently, got %d %s", i, r.code, r.body)
		}
		if strings.Contains(r.body, "bulk_busy") {
			t.Fatalf("same-credential single %d must not report bulk_busy: %s", i, r.body)
		}
	}
}

// Same-key singles overlap: two custom singles on the same channel run
// concurrently and both are accepted (200).
func TestOverlappingSameCustomSinglesSucceed(t *testing.T) {
	var entered atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered.Add(1)
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}]}`))
	}))
	defer srv.Close()
	_, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	gw := admin.manager.current.Load().gateway
	gw.cfg.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "c1", Name: "c1", BaseURL: srv.URL, APIKey: "k1", Model: "m1", Protocol: ProtocolChat},
	}}
	gw.customClient = srv.Client()
	type outcome struct {
		code int
		body string
	}
	results := make([]outcome, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"id":"c1"}`, token, csrf))
			results[idx] = outcome{code: rec.Code, body: rec.Body.String()}
		}(i)
	}
	deadline := time.Now().Add(5 * time.Second)
	for entered.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if entered.Load() < 2 {
		close(release)
		wg.Wait()
		t.Fatalf("same-custom singles must overlap in flight, entered=%d results=%+v", entered.Load(), results)
	}
	close(release)
	wg.Wait()
	for i, r := range results {
		if r.code != http.StatusOK {
			t.Fatalf("same-custom single %d must succeed concurrently, got %d %s", i, r.code, r.body)
		}
		if strings.Contains(r.body, "bulk_busy") {
			t.Fatalf("same-custom single %d must not report bulk_busy: %s", i, r.body)
		}
	}
}
