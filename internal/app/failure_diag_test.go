package app

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func urlWrap(op string, err error) error {
	return &url.Error{Op: op, URL: "http://upstream.example/v1/chat/completions", Err: err}
}

func TestClassifyFailureDiagShapes(t *testing.T) {
	dialTimeout := &net.OpError{Op: "dial", Net: "tcp", Err: timeoutTestErr{}}
	cases := []struct {
		name       string
		resp       *http.Response
		err        error
		hint       string
		gate       error
		wantStage  string
		wantReason string
	}{
		{"success_empty", responseWithStatus(200), nil, FailureStageRequest, nil, "", ""},
		{"http400_empty", responseWithStatus(400), nil, FailureStageRequest, nil, "", ""},
		{"http429_empty", responseWithStatus(429), nil, FailureStageRequest, nil, "", ""},
		{"http503_empty", responseWithStatus(503), nil, FailureStageRequest, nil, "", ""},
		{"header_timeout_wrapped", nil, urlWrap("Post", errors.New(`Post "http://x": net/http: timeout awaiting response headers`)), FailureStageRequest, nil, FailureStageResponseHeaders, FailureReasonResponseHeaderTimeout},
		{"header_timeout_http2", nil, urlWrap("Post", errors.New(`rpc error: http2: timeout awaiting response headers`)), FailureStageRequest, nil, FailureStageResponseHeaders, FailureReasonResponseHeaderTimeout},
		{"dial_timeout", nil, urlWrap("Post", dialTimeout), FailureStageRequest, nil, FailureStageConnect, FailureReasonConnectTimeout},
		{"conn_refused", nil, urlWrap("Post", errors.New("dial tcp 127.0.0.1:1: connect: connection refused")), FailureStageRequest, nil, FailureStageConnect, FailureReasonConnectRefused},
		{"dns", nil, urlWrap("Post", &net.DNSError{Err: "no such host", Name: "bad.example"}), FailureStageRequest, nil, FailureStageDNS, FailureReasonDNSError},
		{"tls_phrase", nil, urlWrap("Post", errors.New("tls: handshake failure")), FailureStageRequest, nil, FailureStageTLS, FailureReasonTLSError},
		{"tls_cert", nil, urlWrap("Post", &tls.CertificateVerificationError{Err: errors.New("expired")}), FailureStageRequest, nil, FailureStageTLS, FailureReasonTLSError},
		{"reset", nil, urlWrap("Post", errors.New("read tcp: connection reset by peer")), FailureStageRequest, nil, FailureStageRequest, FailureReasonConnectionReset},
		{"eof", nil, urlWrap("Post", io.ErrUnexpectedEOF), FailureStageRequest, nil, FailureStageRequest, FailureReasonUnexpectedEOF},
		{"unknown", nil, errors.New("boom"), FailureStageRequest, nil, FailureStageUnknown, FailureReasonUnknown},
		{"generic_timeout_not_connect", nil, urlWrap("Post", timeoutTestErr{}), FailureStageRequest, nil, FailureStageRequest, FailureReasonTransportTimeout},
		{"socks_neutral", nil, urlWrap("Post", errors.New("socks connect tcp 1.2.3.4:443: unknown error")), FailureStageRequest, nil, FailureStageRequest, FailureReasonTransportError},
		{"sentinel_startup", nil, errors.New("upstream stream startup failure"), FailureStageStreamStartup, nil, FailureStageStreamStartup, FailureReasonStreamStartupError},
		{"gate_cause_unified", nil, errors.New("upstream stream startup failure"), FailureStageStreamStartup, errors.New("parse boom"), FailureStageStreamStartup, FailureReasonStreamStartupError},
		{"custom_synth", nil, errors.New("custom fallback transport failed"), FailureStageRequest, nil, FailureStageRequest, FailureReasonTransportError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyFailureDiag(context.Background(), tc.resp, tc.err, tc.hint, tc.gate)
			if got.Stage != tc.wantStage || got.Reason != tc.wantReason {
				t.Fatalf("got %+v want stage=%q reason=%q", got, tc.wantStage, tc.wantReason)
			}
		})
	}
}

type timeoutTestErr struct{}

func (timeoutTestErr) Error() string { return "i/o timeout" }
func (timeoutTestErr) Timeout() bool { return true }

// TestClassifyFailureDiagCancelVsBudget covers the lifecycle priority:
// stream budget expiry fact, own non-stream deadline fact, caller cancel,
// and the parent/own collision staying a caller cancel.
func TestClassifyFailureDiagCancelVsBudget(t *testing.T) {
	// Caller cancel: parent gone, no budget.
	parent, parentCancel := context.WithCancel(context.Background())
	child, cancel := context.WithCancel(parent)
	parentCancel()
	defer cancel()
	got := classifyFailureDiag(child, nil, child.Err(), FailureStageRequest, nil)
	if got.Reason != FailureReasonCallerCancelled {
		t.Fatalf("caller cancel got %+v", got)
	}

	// Stream budget expiry fact wins over the bare Canceled reading.
	bctx, bcancel, bstop := newStreamStartupBudget(context.Background(), time.Hour)
	defer bcancel()
	defer bstop()
	if !streamStartupBudgetFromCtx(bctx).expire() {
		t.Fatalf("budget must expire on demand")
	}
	berr := streamStartupBudgetErr(bctx)
	got = classifyFailureDiag(bctx, nil, berr, FailureStageStreamStartup, nil)
	if got.Reason != FailureReasonRequestBudgetExhausted || got.Stage != FailureStageStreamStartup {
		t.Fatalf("stream budget got %+v", got)
	}

	// Own non-stream deadline: derived deadline passed, parent still live.
	p2 := context.Background()
	c2, cancel2 := context.WithDeadline(p2, time.Now().Add(-time.Millisecond))
	defer cancel2()
	c2 = withNonstreamDeadline(c2, p2, time.Millisecond)
	if c2.Err() != context.DeadlineExceeded {
		t.Fatalf("own deadline must read DeadlineExceeded, got %v", c2.Err())
	}
	got = classifyFailureDiag(c2, nil, c2.Err(), FailureStageRequest, nil)
	if got.Reason != FailureReasonRequestBudgetExhausted {
		t.Fatalf("own deadline got %+v", got)
	}

	// Collision: parent also done, conservatively a caller cancel.
	p3, p3Cancel := context.WithCancel(context.Background())
	c3, cancel3 := context.WithDeadline(p3, time.Now().Add(-time.Millisecond))
	defer cancel3()
	p3Cancel()
	c3 = withNonstreamDeadline(c3, p3, time.Millisecond)
	got = classifyFailureDiag(c3, nil, c3.Err(), FailureStageRequest, nil)
	if got.Reason != FailureReasonCallerCancelled {
		t.Fatalf("collision must stay caller cancel, got %+v", got)
	}
}

// TestFailureDiagSanitize enforces the projection boundary: success clears,
// real 400/429/503 clear, hostile strings collapse to unknown, empty stays
// empty for old records.
func TestFailureDiagSanitize(t *testing.T) {
	if s, r := sanitizeFailureDiag(true, 0, FailureStageConnect, FailureReasonConnectRefused); s != "" || r != "" {
		t.Fatalf("success must clear, got %q/%q", s, r)
	}
	for _, st := range []int{400, 429, 503} {
		if s, r := sanitizeFailureDiag(false, st, FailureStageConnect, FailureReasonConnectRefused); s != "" || r != "" {
			t.Fatalf("status %d must clear, got %q/%q", st, s, r)
		}
	}
	malicious := "Authorization: Bearer sk-live-secret\nbody{\"x\":1}<script>"
	if s, r := sanitizeFailureDiag(false, 0, malicious, malicious); s != FailureStageUnknown || r != FailureReasonUnknown {
		t.Fatalf("hostile input must collapse, got %q/%q", s, r)
	}
	if s, r := sanitizeFailureDiag(false, 0, "", ""); s != "" || r != "" {
		t.Fatalf("empty must stay empty, got %q/%q", s, r)
	}
	if s, r := sanitizeFailureDiag(false, 502, FailureStageStreamStartup, FailureReasonStreamStartupError); s != FailureStageStreamStartup || r != FailureReasonStreamStartupError {
		t.Fatalf("startup pseudo-502 must keep diag, got %q/%q", s, r)
	}
}

// TestFailureDiagNoRawLeak proves a secret-laden transport error only ever
// surfaces as whitelist enums.
func TestFailureDiagNoRawLeak(t *testing.T) {
	secret := "sk-live-super-secret-value"
	header := "X-Api-Key: " + secret
	err := urlWrap("Post", fmt.Errorf("proxy %s refused body{\"k\":%q}", header, secret))
	got := classifyFailureDiag(context.Background(), nil, err, FailureStageRequest, nil)
	if !validFailureStage(got.Stage) || !validFailureReason(got.Reason) {
		t.Fatalf("must be whitelist enums, got %+v", got)
	}
	for _, f := range []string{got.Stage, got.Reason} {
		if strings.Contains(f, secret) || strings.Contains(f, header) {
			t.Fatalf("secret leaked into diagnosis output")
		}
	}
	rec := UpstreamAttempt{
		RequestID: "req-1", Model: "m", Tier: string(TierZen), Protocol: "chat",
		ClientSessionHash: "hash", KeyID: "key", Channel: "key", Attempt: 1,
		Proxy: "node", ProxyPool: "pool", Status: 0, DurationMS: 5,
		Success: false, FailureClass: AttemptClassTransportFailure,
		FailureStage: got.Stage, FailureReason: got.Reason,
	}
	rec.FailureStage, rec.FailureReason = sanitizeFailureDiag(rec.Success, rec.Status, rec.FailureStage, rec.FailureReason)
	args := upstreamAttemptFailedInfoArgs(rec)
	joined := fmt.Sprint(args...)
	if strings.Contains(joined, secret) {
		t.Fatalf("secret leaked into INFO args")
	}
	foundReq, foundStage, foundReason, foundEvent := false, false, false, false
	for i := 0; i+1 < len(args); i += 2 {
		if k, ok := args[i].(string); ok {
			switch k {
			case "event":
				if args[i+1] == "upstream_attempt_failed" {
					foundEvent = true
				}
			case "request_id":
				if args[i+1] == "req-1" {
					foundReq = true
				}
			case "failure_stage":
				if args[i+1] == got.Stage {
					foundStage = true
				}
			case "failure_reason":
				if args[i+1] == got.Reason {
					foundReason = true
				}
			case "error":
				t.Fatalf("raw error must never ride the INFO event")
			}
		}
	}
	if !foundEvent || !foundReq || !foundStage || !foundReason {
		t.Fatalf("INFO args must carry event/request/stage/reason: %v", args)
	}
}

// TestMonitorHistoryDiagPropagation covers the shared record path: failures
// keep enums, success clears injected diag, history persists the same, and
// old lines without the fields still decode compatibly.
func TestMonitorHistoryDiagPropagation(t *testing.T) {
	m := NewMonitor()
	now := time.Now().UTC()
	m.RecordAttempt(UpstreamAttempt{
		Time: now, RequestID: "r1", Model: "m", Tier: "zen", KeyID: "K", Channel: "key",
		Proxy: "direct", Status: 0, DurationMS: 3, Success: false,
		FailureClass: AttemptClassTransportFailure, FailureStage: FailureStageConnect, FailureReason: FailureReasonConnectRefused,
	})
	m.RecordAttempt(UpstreamAttempt{
		Time: now, RequestID: "r2", Model: "m", Tier: "zen", KeyID: "K", Channel: "key",
		Proxy: "direct", Status: 200, DurationMS: 3, Success: true,
		FailureClass: AttemptClassSuccess, FailureStage: FailureStageConnect, FailureReason: FailureReasonConnectRefused,
	})
	m.RecordAttempt(UpstreamAttempt{
		Time: now, RequestID: "r3", Model: "m", Tier: "zen", KeyID: "K", Channel: "key",
		Proxy: "direct", Status: 503, DurationMS: 3, Success: false,
		FailureClass: AttemptClassUpstreamFailure, FailureStage: FailureStageConnect, FailureReason: FailureReasonConnectRefused,
	})
	recent := m.Snapshot().Upstream.Recent
	byID := map[string]UpstreamAttempt{}
	for _, a := range recent {
		byID[a.RequestID] = a
	}
	if got := byID["r1"]; got.FailureStage != FailureStageConnect || got.FailureReason != FailureReasonConnectRefused {
		t.Fatalf("failure must keep diag: %+v", got)
	}
	if got := byID["r2"]; got.FailureStage != "" || got.FailureReason != "" {
		t.Fatalf("success must clear diag: %+v", got)
	}
	if got := byID["r3"]; got.FailureStage != "" || got.FailureReason != "" {
		t.Fatalf("real 503 must keep status-only: %+v", got)
	}
	// Old schema (missing fields) still decodes compatibly to empty.
	var old historyAttemptLine
	if err := json.Unmarshal([]byte(`{"v":1,"kind":"attempt","time":"`+now.UTC().Format(time.RFC3339Nano)+`","request_id":"old","model":"m","attempt":1,"channel":"key","duration_ms":1,"success":false}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.FailureStage != "" || old.FailureReason != "" {
		t.Fatalf("old schema must zero-fill: %+v", old)
	}
}

func TestHistoryDiagBoundaryAndCompat(t *testing.T) {
	store, cfgPath := openTestHistory(t, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 7, MaxBytesMB: 128})
	dir := ResolveHistoryDir(cfgPath, "h")
	now := time.Now().UTC()
	store.EnqueueAttempt(UpstreamAttempt{
		Time: now, RequestID: "h1", Model: "m", Tier: "zen", KeyID: "K", Channel: "key",
		Proxy: "direct", Status: 0, DurationMS: 3, Success: false,
		FailureClass: AttemptClassTransportFailure, FailureStage: FailureStageDNS, FailureReason: FailureReasonDNSError,
	})
	store.EnqueueAttempt(UpstreamAttempt{
		Time: now, RequestID: "h2", Model: "m", Tier: "zen", KeyID: "K", Channel: "key",
		Proxy: "direct", Status: 0, DurationMS: 3, Success: false,
		FailureClass: AttemptClassTransportFailure, FailureStage: "evil", FailureReason: "evil",
	})
	store.EnqueueAttempt(UpstreamAttempt{
		Time: now, RequestID: "h3", Model: "m", Tier: "zen", KeyID: "K", Channel: "key",
		Proxy: "direct", Status: 200, DurationMS: 3, Success: true,
		FailureClass: AttemptClassSuccess, FailureStage: FailureStageDNS, FailureReason: FailureReasonDNSError,
	})
	waitForFile(t, dir, "attempts-")
	page, err := queryHistoryAttempts(context.Background(), store, historyQueryFilter{
		From: now.Add(-time.Hour), To: now.Add(time.Hour), Limit: 10,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	byID := map[string]historyAttemptLine{}
	for _, item := range page.Items {
		if v, ok := item.(historyAttemptLine); ok {
			byID[v.RequestID] = v
		} else {
			t.Fatalf("unexpected item type %T", item)
		}
	}
	if got := byID["h1"]; got.FailureStage != FailureStageDNS || got.FailureReason != FailureReasonDNSError {
		t.Fatalf("history must persist diag: %+v", got)
	}
	if got := byID["h2"]; got.FailureStage != FailureStageUnknown || got.FailureReason != FailureReasonUnknown {
		t.Fatalf("hostile history input must collapse: %+v", got)
	}
	if got := byID["h3"]; got.FailureStage != "" || got.FailureReason != "" {
		t.Fatalf("history success must clear: %+v", got)
	}
}

// TestGateCancelSingleDiagObservation proves the cancelled gate path keeps
// exactly one observation with a cancel/budget diagnosis, no scheduler
// write, and no second record.
func TestGateCancelSingleDiagObservation(t *testing.T) {
	var customHits atomic.Int32
	gw, monitor := cancelStreamGateway(t, &customHits)
	pool := gw.pools["a"]
	proxyRaw := pool.items[0].name
	ident := targetIdentity(TierZen, anonymousSchedulerCredentialID, "a", proxyRaw, "m")
	cands := gw.scheduler.orderCandidates(gw.scheduler.buildAnonymousCandidates(pool, "m", time.Now().UnixNano()), "ses_diag_gate_cancel")
	cand := cands[0]
	ctx, cancel := cancelStreamCtx()
	var calls atomic.Int32
	postStub(t, gw, "a", 0, &calls, nil, func(*http.Request) (*http.Response, error) {
		cancel()
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	ids := clientSessionIDs("ses_diag_gate_cancel")
	out := gw.executeAttempt(ctx, anonOnlyStreamRoute(), TierZen, gw.cfg.Upstream.Zen, ProtocolChat, streamBodies()[TierZen], ids, cand, "rs-test", "anonymous", "anonymous", true, 1)
	if out.Resp != nil || out.Err == nil {
		t.Fatalf("cancel must return its error: resp=%v err=%v", out.Resp, out.Err)
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 1 {
		t.Fatalf("cancelled send must keep exactly one observation: %d", len(recent))
	}
	got := recent[0]
	if got.Success || got.FailureReason != FailureReasonCallerCancelled {
		t.Fatalf("cancel observation must carry caller_cancelled: %+v", got)
	}
	if _, _, ok := gw.scheduler.targetCooldownStatus(ident); ok {
		t.Fatalf("cancel must not cool its target")
	}
}

// TestDelayedHeadersBudgetRealBranch drives a real headers delay past a real
// startup budget and proves the observation reads budget expiry, never a
// duration-guessed cause and never caller_cancelled.
func TestDelayedHeadersBudgetRealBranch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(400 * time.Millisecond)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(chatTextSSE("hi") + chatDoneSSE()))
	}))
	defer srv.Close()
	parent := context.Background()
	ctx, cancel, stop := newStreamStartupBudget(parent, 60*time.Millisecond)
	defer cancel()
	defer stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_, doErr := srv.Client().Do(req)
	if doErr == nil {
		t.Fatalf("delayed headers must fail under the budget")
	}
	if !streamStartupExpired(ctx) {
		t.Fatalf("budget controller must own the expiry fact")
	}
	got := classifyFailureDiag(ctx, nil, streamStartupBudgetErr(ctx), FailureStageStreamStartup, nil)
	if got.Reason != FailureReasonRequestBudgetExhausted || got.Stage != FailureStageStreamStartup {
		t.Fatalf("delayed headers must read budget expiry: %+v", got)
	}
}

// TestStartupGateTypicalDiag proves the pre-commit startup pseudo-outcome
// keeps its 502 status with a startup diagnosis and no double record.
func TestStartupGateTypicalDiag(t *testing.T) {
	var customHits atomic.Int32
	gw, monitor := cancelStreamGateway(t, &customHits)
	pool := gw.pools["a"]
	cands := gw.scheduler.orderCandidates(gw.scheduler.buildAnonymousCandidates(pool, "m", time.Now().UnixNano()), "ses_diag_startup")
	cand := cands[0]
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	ids := clientSessionIDs("ses_diag_startup")
	out := gw.executeAttempt(ctx, anonOnlyStreamRoute(), TierZen, gw.cfg.Upstream.Zen, ProtocolChat, streamBodies()[TierZen], ids, cand, "rs-test", "anonymous", "anonymous", true, 1)
	if out.Err == nil || !isStreamStartupFailureErr(out.Err) {
		t.Fatalf("empty stream must be a startup failure: %+v", out.Err)
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 1 {
		t.Fatalf("startup failure must record exactly once: %d", len(recent))
	}
	got := recent[0]
	if got.Status != 502 || got.FailureStage != FailureStageStreamStartup || got.FailureReason != FailureReasonStreamStartupError {
		t.Fatalf("startup outcome must keep 502 with startup diag: %+v", got)
	}
}

// --- Per-attempt INFO diagnostics (shared record authority) ---

func captureDiagLogger(buf *bytes.Buffer) *slog.Logger {
	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelInfo)
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: lvl}))
}

func diagCaptureGateway(t *testing.T, buf *bytes.Buffer, pools map[string][]string, routing ProxyRoutingConfig, maxAttempts int) (*Gateway, *Monitor) {
	t.Helper()
	cfg := testGatewayConfig(pools, routing)
	cfg.Retry.MaxAttempts = maxAttempts
	cfg.Retry.TransientRetryIntervalSeconds = 0
	monitor := NewMonitor()
	gw, err := NewGateway(cfg, captureDiagLogger(buf), monitor)
	if err != nil {
		t.Fatal(err)
	}
	return gw, monitor
}

func countDiagEvent(buf *bytes.Buffer, event string) int {
	return strings.Count(buf.String(), event)
}

func diagAttemptLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			continue
		}
		if m["event"] == "upstream_attempt_failed" {
			out = append(out, m)
		}
	}
	return out
}

func requireSharedAttemptSchema(t *testing.T, m map[string]any) {
	t.Helper()
	for _, k := range []string{"request_id", "model", "channel", "attempt", "proxy_pool", "proxy_node", "status", "failure_class", "failure_stage", "failure_reason", "duration_ms", "anonymous"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("attempt INFO missing %q: %v", k, m)
		}
	}
	for _, k := range []string{"error", "body", "headers", "Authorization", "authorization", "cookie", "secret", "password"} {
		for fk := range m {
			if strings.EqualFold(fk, k) {
				t.Fatalf("raw field %q must never ride the attempt event", fk)
			}
		}
	}
}

// Native recover-success: 2 real failures (transport + 503) then success.
// Success emits no INFO; the 2 failures each emit exactly one.
func TestUpstreamAttemptFailedLogRecoverSuccess(t *testing.T) {
	var buf bytes.Buffer
	gw, monitor := diagCaptureGateway(t, &buf,
		map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"}, 2)
	var calls atomic.Int32
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("dial tcp: connection refused")
		}
		return responseWithBody(503, `{"error":"busy"}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, _, err := gw.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), emptySessionIDs(), 0)
	if err != nil {
		t.Fatalf("recover must succeed: %v", err)
	}
	drainAndClose(resp.Body)
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 3 {
		t.Fatalf("want 3 observations (2 fail + success), got %d: %+v", len(recent), recent)
	}
	if got := countDiagEvent(&buf, "upstream_attempt_failed"); got != 2 {
		t.Fatalf("want 2 attempt INFO, got %d: %s", got, buf.String())
	}
	if got := countDiagEvent(&buf, "request_failed"); got != 0 {
		t.Fatalf("recover path must not emit final request_failed INFO: %s", buf.String())
	}
	lines := diagAttemptLines(t, &buf)
	if len(lines) != 2 {
		t.Fatalf("want 2 parsed attempt lines, got %d", len(lines))
	}
	for _, m := range lines {
		requireSharedAttemptSchema(t, m)
	}
	// Stage/reason consistency with monitor: transport keeps enums, real 503
	// stays status-only (empty diag).
	var failed []UpstreamAttempt
	for _, a := range recent {
		if !a.Success {
			failed = append(failed, a)
		}
	}
	if len(failed) != 2 {
		t.Fatalf("want 2 failed monitor records, got %+v", recent)
	}
	if failed[0].FailureStage == "" || failed[0].FailureReason == "" {
		t.Fatalf("transport failure must carry diag: %+v", failed[0])
	}
	if failed[1].Status != 503 || failed[1].FailureStage != "" || failed[1].FailureReason != "" {
		t.Fatalf("real 503 must stay status-only: %+v", failed[1])
	}
	matched := 0
	for _, m := range lines {
		for _, a := range failed {
			if m["status"] == float64(a.Status) && m["failure_stage"] == a.FailureStage && m["failure_reason"] == a.FailureReason && m["failure_class"] == a.FailureClass {
				matched++
				break
			}
		}
	}
	if matched != 2 {
		t.Fatalf("log stage/reason must match monitor records: logs=%v failed=%+v", lines, failed)
	}
}

// Multi-failure final error: every failed attempt logs once, no final
// duplicate request_failed INFO remains (only the original Warn at the HTTP
// boundary, which doUpstreamTiers never emits).
func TestUpstreamAttemptFailedLogFinalErrorNoDuplicate(t *testing.T) {
	var buf bytes.Buffer
	gw, monitor := diagCaptureGateway(t, &buf,
		map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"}, 1)
	anonOnly := modelRoute{ID: "m", Tier: TierZen, Protocol: ProtocolChat,
		Protocols: map[Tier]Protocol{TierZen: ProtocolChat}, Anonymous: true}
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp: connection refused")
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp: connection reset by peer")
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	_, _, _, err := gw.doUpstreamTiers(ctx, anonOnly, routeBodies(), emptySessionIDs(), 0)
	if err == nil {
		t.Fatal("all failing must return error")
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 2 {
		t.Fatalf("want 2 failed observations, got %+v", recent)
	}
	if got := countDiagEvent(&buf, "upstream_attempt_failed"); got != 2 {
		t.Fatalf("each failed attempt must log once, got %d: %s", got, buf.String())
	}
	if got := countDiagEvent(&buf, "request_failed"); got != 0 {
		t.Fatalf("final request_failed INFO must be gone (Warn stays at boundary): %s", buf.String())
	}
	for _, m := range diagAttemptLines(t, &buf) {
		requireSharedAttemptSchema(t, m)
	}
}

// Custom fallback shares the same record authority: one custom failure logs
// once with the shared schema, custom success logs nothing for custom.
func TestUpstreamAttemptFailedLogCustomFailureAndSuccess(t *testing.T) {
	// Failure case: native 429 exhaustion then custom transport failure.
	var failBuf bytes.Buffer
	failCustomHits := &atomic.Int32{}
	customFailURL := "https://custom-fail.example"
	cfg := testGatewayConfig(map[string][]string{"a": {"direct"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	cfg.Anonymous = true
	cfg.Keys = []string{"zen-key-custom-11111"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: customFailURL, APIKey: "k1", Model: "cm-x"}}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	failMonitor := NewMonitor()
	failGW, err := NewGateway(normalized, captureDiagLogger(&failBuf), failMonitor)
	if err != nil {
		t.Fatal(err)
	}
	for _, pool := range []string{"a", "z"} {
		postStub(t, failGW, pool, 0, nil, nil, func(*http.Request) (*http.Response, error) {
			r := responseWithBody(429, `{"error":"slow"}`)
			r.Header.Set("Retry-After", "9")
			return r, nil
		})
	}
	failGW.customClient = &http.Client{Transport: &failRoundTripper{err: errors.New("dial tcp: connection refused"), hits: failCustomHits}}
	route := anonAuthRoute()
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	_, _, _, failErr := failGW.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs("ses_diag_custom_fail", "req-custom-fail"), 0, ex)
	if failErr == nil {
		t.Fatal("custom transport failure must return error")
	}
	if failCustomHits.Load() != 1 {
		t.Fatalf("custom must be attempted once, hits=%d", failCustomHits.Load())
	}
	failLines := diagAttemptLines(t, &failBuf)
	if len(failLines) != 3 {
		t.Fatalf("want 2 native + 1 custom failure INFO, got %d: %s", len(failLines), failBuf.String())
	}
	var customLines []map[string]any
	for _, m := range failLines {
		requireSharedAttemptSchema(t, m)
		if m["tier"] == string(TierCustom) {
			customLines = append(customLines, m)
		}
	}
	if len(customLines) != 1 {
		t.Fatalf("want exactly 1 custom-tier failure INFO: %v", failLines)
	}

	// Success case: same exhaustion but custom 200 succeeds; custom emits no
	// failure INFO (native 429s still each log once).
	var okBuf bytes.Buffer
	customOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-x")))
	}))
	defer customOK.Close()
	cfg2 := testGatewayConfig(map[string][]string{"a": {"direct"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	cfg2.Anonymous = true
	cfg2.Keys = []string{"zen-key-custom-11111"}
	cfg2.Retry.MaxAttempts = 1
	cfg2.Retry.TransientRetryIntervalSeconds = 0
	cfg2.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: customOK.URL, APIKey: "k1", Model: "cm-x"}}}
	normalized2, err := NormalizeConfig("config.json", cfg2)
	if err != nil {
		t.Fatal(err)
	}
	okMonitor := NewMonitor()
	okGW, err := NewGateway(normalized2, captureDiagLogger(&okBuf), okMonitor)
	if err != nil {
		t.Fatal(err)
	}
	for _, pool := range []string{"a", "z"} {
		postStub(t, okGW, pool, 0, nil, nil, func(*http.Request) (*http.Response, error) {
			r := responseWithBody(429, `{"error":"slow"}`)
			r.Header.Set("Retry-After", "9")
			return r, nil
		})
	}
	resp, eff, _, err := okGW.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs("ses_diag_custom_ok", "req-custom-ok"), 0, ex)
	if err != nil {
		t.Fatalf("custom success must not error: %v", err)
	}
	drainAndClose(resp.Body)
	if eff.Tier != TierCustom {
		t.Fatalf("effective route must be custom, got %+v", eff)
	}
	okLines := diagAttemptLines(t, &okBuf)
	if len(okLines) != 2 {
		t.Fatalf("custom success must leave only the 2 native failure INFO, got %d: %s", len(okLines), okBuf.String())
	}
	for _, m := range okLines {
		requireSharedAttemptSchema(t, m)
		if m["tier"] == string(TierCustom) {
			t.Fatalf("custom success must not log a custom failure INFO: %v", m)
		}
	}
}

// Gate cancel and startup outcomes each keep exactly one record and one log.
func TestUpstreamAttemptFailedLogGateCancelAndStartup(t *testing.T) {
	// Cancelled gate send.
	var cancelBuf bytes.Buffer
	cancelMonitor := NewMonitor()
	cfg := testGatewayConfig(map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	cancelGW, err := NewGateway(normalized, captureDiagLogger(&cancelBuf), cancelMonitor)
	if err != nil {
		t.Fatal(err)
	}
	pool := cancelGW.pools["a"]
	cands := cancelGW.scheduler.orderCandidates(cancelGW.scheduler.buildAnonymousCandidates(pool, "m", time.Now().UnixNano()), "ses_diag_log_cancel")
	ctx, cancel := cancelStreamCtx()
	postStub(t, cancelGW, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		cancel()
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	ids := clientSessionIDs("ses_diag_log_cancel")
	out := cancelGW.executeAttempt(ctx, anonOnlyStreamRoute(), TierZen, cancelGW.cfg.Upstream.Zen, ProtocolChat, streamBodies()[TierZen], ids, cands[0], "rs-test", "anonymous", "anonymous", true, 1)
	if out.Resp != nil || out.Err == nil {
		t.Fatalf("cancel must return its error: %+v", out)
	}
	if got := len(cancelMonitor.Snapshot().Upstream.Recent); got != 1 {
		t.Fatalf("cancel must record exactly once, got %d", got)
	}
	if got := countDiagEvent(&cancelBuf, "upstream_attempt_failed"); got != 1 {
		t.Fatalf("cancel must log exactly once, got %d: %s", got, cancelBuf.String())
	}
	cancelLines := diagAttemptLines(t, &cancelBuf)
	if len(cancelLines) != 1 || cancelLines[0]["failure_reason"] != FailureReasonCallerCancelled {
		t.Fatalf("cancel log must carry caller_cancelled: %v", cancelLines)
	}

	// Pre-commit startup pseudo-502.
	var startupBuf bytes.Buffer
	startupMonitor := NewMonitor()
	startupGW, err := NewGateway(normalized, captureDiagLogger(&startupBuf), startupMonitor)
	if err != nil {
		t.Fatal(err)
	}
	scands := startupGW.scheduler.orderCandidates(startupGW.scheduler.buildAnonymousCandidates(startupGW.pools["a"], "m", time.Now().UnixNano()), "ses_diag_log_startup")
	sctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	postStub(t, startupGW, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	sout := startupGW.executeAttempt(sctx, anonOnlyStreamRoute(), TierZen, startupGW.cfg.Upstream.Zen, ProtocolChat, streamBodies()[TierZen], clientSessionIDs("ses_diag_log_startup"), scands[0], "rs-test", "anonymous", "anonymous", true, 1)
	if sout.Err == nil || !isStreamStartupFailureErr(sout.Err) {
		t.Fatalf("startup must fail: %+v", sout.Err)
	}
	if got := len(startupMonitor.Snapshot().Upstream.Recent); got != 1 {
		t.Fatalf("startup must record exactly once, got %d", got)
	}
	if got := countDiagEvent(&startupBuf, "upstream_attempt_failed"); got != 1 {
		t.Fatalf("startup must log exactly once, got %d: %s", got, startupBuf.String())
	}
	startupLines := diagAttemptLines(t, &startupBuf)
	if len(startupLines) != 1 || startupLines[0]["status"] != float64(502) || startupLines[0]["failure_stage"] != FailureStageStreamStartup {
		t.Fatalf("startup log must keep 502 with startup diag: %v", startupLines)
	}
}

// Monitor disabled (nil) still logs; history disabled (default NewMonitor
// without a store) never suppresses the log.
func TestUpstreamAttemptFailedLogMonitorNilAndHistoryDisabled(t *testing.T) {
	var buf bytes.Buffer
	cfg := testGatewayConfig(map[string][]string{"a": {"direct"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	gw, err := NewGateway(cfg, captureDiagLogger(&buf), nil)
	if err != nil {
		t.Fatal(err)
	}
	route := anonAuthRoute()
	ids := emptySessionIDs()
	proxy := &proxyTransport{name: "direct", pool: "a"}
	gw.recordUpstreamAttemptWithClass(route, ProtocolChat, ids, 1, "anonymous", "anonymous", true, proxy, nil, errors.New("dial timeout"), time.Millisecond, attemptClassification{Class: AttemptClassTransportFailure}, false, false, false, badRequestDiag{}, classifyFailureDiag(context.Background(), nil, errors.New("dial timeout"), FailureStageRequest, nil))
	if got := countDiagEvent(&buf, "upstream_attempt_failed"); got != 1 {
		t.Fatalf("monitor nil must still log the failure, got %d: %s", got, buf.String())
	}
	// Success never logs, even with monitor nil.
	gw.recordUpstreamAttemptWithClass(route, ProtocolChat, ids, 2, "anonymous", "anonymous", true, proxy, responseWithBody(200, `{"ok":true}`), nil, time.Millisecond, attemptClassification{Class: AttemptClassSuccess}, false, false, false, badRequestDiag{}, failureDiag{})
	if got := countDiagEvent(&buf, "upstream_attempt_failed"); got != 1 {
		t.Fatalf("success must not log, got %d: %s", got, buf.String())
	}

	var hbuf bytes.Buffer
	hmonitor := NewMonitor()
	if got := hmonitor.HistoryStatus(); got.Active {
		t.Fatalf("test monitor must start history-disabled")
	}
	hgw, err := NewGateway(cfg, captureDiagLogger(&hbuf), hmonitor)
	if err != nil {
		t.Fatal(err)
	}
	hgw.recordUpstreamAttemptWithClass(route, ProtocolChat, ids, 1, "anonymous", "anonymous", true, proxy, nil, errors.New("dial timeout"), time.Millisecond, attemptClassification{Class: AttemptClassTransportFailure}, false, false, false, badRequestDiag{}, classifyFailureDiag(context.Background(), nil, errors.New("dial timeout"), FailureStageRequest, nil))
	if got := countDiagEvent(&hbuf, "upstream_attempt_failed"); got != 1 {
		t.Fatalf("history disabled must not suppress the log: %s", hbuf.String())
	}
	if got := len(hmonitor.Snapshot().Upstream.Recent); got != 1 {
		t.Fatalf("history-disabled monitor must still record: %d", got)
	}
}

// Safety whitelist on the real record path: secret-laden errors, userinfo
// URLs, keys, and bodies never enter the event; the log shares the sanitized
// monitor stage/reason.
func TestUpstreamAttemptFailedLogSafetyAndConsistency(t *testing.T) {
	var buf bytes.Buffer
	monitor := NewMonitor()
	cfg := testGatewayConfig(map[string][]string{"a": {"direct"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	gw, err := NewGateway(cfg, captureDiagLogger(&buf), monitor)
	if err != nil {
		t.Fatal(err)
	}
	secret := "sk-live-super-secret-value"
	bodySecret := `{"k":"` + secret + `"}`
	proxy := &proxyTransport{name: "http://user:" + secret + "@evil.example:8080", pool: "a"}
	route := anonAuthRoute()
	ids := requestIDs{Session: "ses-safe", Request: "req-safe", Project: "prj"}
	sendErr := fmt.Errorf("proxy rejected Authorization: Bearer %s body%s: %w", secret, bodySecret, errors.New("dial tcp: connection refused"))
	fdiag := classifyFailureDiag(context.Background(), nil, sendErr, FailureStageRequest, nil)
	gw.recordUpstreamAttemptWithClass(route, ProtocolChat, ids, 1, "anonymous", "anonymous", true, proxy, nil, sendErr, 3*time.Millisecond, classifyUpstreamAttempt(nil, sendErr), false, false, false, badRequestDiag{}, fdiag)
	lines := diagAttemptLines(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("want 1 safety INFO, got %s", buf.String())
	}
	joined := buf.String()
	for _, leak := range []string{secret, "Authorization", bodySecret} {
		if strings.Contains(joined, leak) {
			t.Fatalf("new event must not contain %q: %s", leak, joined)
		}
	}
	if strings.Contains(joined, "user:"+secret) || strings.Contains(joined, "user:sk-live") {
		t.Fatalf("proxy userinfo must be stripped: %s", joined)
	}
	if !strings.Contains(joined, "***") && !strings.Contains(joined, "%2A%2A%2A") {
		t.Fatalf("proxy userinfo must be redacted to *** (escaped %%2A allowed): %s", joined)
	}
	requireSharedAttemptSchema(t, lines[0])
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 1 {
		t.Fatalf("monitor must hold the same attempt: %+v", recent)
	}
	if lines[0]["failure_stage"] != recent[0].FailureStage || lines[0]["failure_reason"] != recent[0].FailureReason || lines[0]["failure_class"] != recent[0].FailureClass || lines[0]["status"] != float64(recent[0].Status) {
		t.Fatalf("log must share monitor stage/reason/class/status: log=%v monitor=%+v", lines[0], recent[0])
	}
}
