package main

import (
	"context"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

// affinityRank returns proxy names in production soft-affinity order for one
// credential+pool: score descending, proxy name tie-break.
func affinityRank(credID, pool string, names []string) []string {
	out := append([]string(nil), names...)
	sort.SliceStable(out, func(i, j int) bool {
		si := proxyAffinityScore(credID, pool, out[i])
		sj := proxyAffinityScore(credID, pool, out[j])
		if si != sj {
			return si > sj
		}
		return out[i] < out[j]
	})
	return out
}

func stubPoolHits(gw *Gateway, body string) []*atomic.Int32 {
	pool := gw.pools["shared"]
	hits := make([]*atomic.Int32, len(pool.items))
	for i, proxy := range pool.items {
		h := &atomic.Int32{}
		hits[i] = h
		proxy.client.Transport = &stubTransport{hits: h, status: 200, body: body}
		proxy.healthy.Store(true)
	}
	return hits
}

func hitsByProxyName(gw *Gateway, hits []*atomic.Int32) map[string]int32 {
	out := map[string]int32{}
	pool := gw.pools["shared"]
	for i, proxy := range pool.items {
		out[proxy.name] = hits[i].Load()
	}
	return out
}

func TestCredentialCheckAffinityFirstDespiteShuffledIndexes(t *testing.T) {
	proxies := []string{
		"http://127.0.0.1:8081",
		"http://127.0.0.1:8082",
		"http://127.0.0.1:8083",
		"http://127.0.0.1:8084",
		"http://127.0.0.1:8085",
	}
	// Two gateways with reversed config order share the same credential ID and
	// pool identity, so affinity ranking per proxy name is identical while
	// pool indexes are shuffled.
	mk := func(order []string) *Gateway {
		manager, _, _, _ := credentialAdmin(t,
			map[string][]string{"shared": order},
			ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
			[]string{"zen-key-affinity-1"}, nil)
		return manager.current.Load().gateway
	}
	reversed := append([]string(nil), proxies...)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	for _, order := range [][]string{proxies, reversed} {
		gw := mk(order)
		seedBulkProbeCatalog(gw)
		body := bulkChatSuccessBody("bulk-free-model")
		hits := stubPoolHits(gw, body)
		cred := gw.authCreds[0]
		names := make([]string, 0, len(gw.pools["shared"].items))
		for _, p := range gw.pools["shared"].items {
			names = append(names, p.name)
		}
		ranked := affinityRank(cred.id, "shared", names)
		// bulkMaxNodesPerCredential caps to the first 4 affinity-ordered nodes.
		wantTested := map[string]bool{}
		for i := 0; i < bulkMaxNodesPerCredential && i < len(ranked); i++ {
			wantTested[ranked[i]] = true
		}
		pinsBefore := gw.scheduler.pins.count()
		sessBefore := gw.scheduler.routeSessions.count()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		resp, status, _, _ := gw.runCredentialCheck(ctx, TierZen, cred, "shared")
		cancel()
		if status != 200 {
			t.Fatalf("order=%v status=%d resp=%+v", order, status, resp)
		}
		if resp.TestedNodes != bulkMaxNodesPerCredential {
			t.Fatalf("order=%v tested=%d want %d", order, resp.TestedNodes, bulkMaxNodesPerCredential)
		}
		got := hitsByProxyName(gw, hits)
		for name := range wantTested {
			if got[name] == 0 {
				t.Fatalf("order=%v affinity-top proxy %q must be tested, hits=%v ranked=%v", order, name, got, ranked)
			}
		}
		truncated := ranked[len(ranked)-1]
		if !wantTested[truncated] && got[truncated] != 0 {
			t.Fatalf("order=%v affinity-last proxy %q must be truncated, hits=%v ranked=%v", order, truncated, got, ranked)
		}
		// The preferred eligible proxy must be first in affinity order even
		// when its pool index is not zero.
		preferred := ranked[0]
		if got[preferred] == 0 {
			t.Fatalf("order=%v preferred proxy %q must be first/tested, hits=%v", order, preferred, got)
		}
		if pinsBefore != gw.scheduler.pins.count() {
			t.Fatalf("order=%v pins mutated %d->%d", order, pinsBefore, gw.scheduler.pins.count())
		}
		if sessBefore != gw.scheduler.routeSessions.count() {
			t.Fatalf("order=%v route sessions mutated %d->%d", order, sessBefore, gw.scheduler.routeSessions.count())
		}
	}
}

func TestCredentialCheckExclusionWinsOverAffinity(t *testing.T) {
	manager, _, _, _ := credentialAdmin(t,
		map[string][]string{"shared": {"http://127.0.0.1:8081", "http://127.0.0.1:8082", "http://127.0.0.1:8083"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-affinity-2"}, nil)
	gw := manager.current.Load().gateway
	seedBulkProbeCatalog(gw)
	cred := gw.authCreds[0]
	names := []string{}
	for _, p := range gw.pools["shared"].items {
		names = append(names, p.name)
	}
	ranked := affinityRank(cred.id, "shared", names)
	preferred := ranked[0]
	// Cool the affinity-preferred proxy: eligibility must win over affinity.
	gw.scheduler.noteProxy429Failure(TierZen, "shared", preferred, AttemptClassRateLimited, 429, 0, time.Now().UnixNano())
	body := bulkChatSuccessBody("bulk-free-model")
	hits := stubPoolHits(gw, body)
	pinsBefore := gw.scheduler.pins.count()
	sessBefore := gw.scheduler.routeSessions.count()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	resp, status, _, _ := gw.runCredentialCheck(ctx, TierZen, cred, "shared")
	cancel()
	if status != 200 {
		t.Fatalf("status=%d resp=%+v", status, resp)
	}
	got := hitsByProxyName(gw, hits)
	if got[preferred] != 0 {
		t.Fatalf("cooled preferred proxy %q must not be sent to, hits=%v ranked=%v", preferred, got, ranked)
	}
	for _, name := range ranked[1:] {
		if got[name] == 0 {
			t.Fatalf("remaining eligible proxy %q must be tested, hits=%v", name, got)
		}
	}
	if pinsBefore != gw.scheduler.pins.count() {
		t.Fatalf("pins mutated %d->%d", pinsBefore, gw.scheduler.pins.count())
	}
	if sessBefore != gw.scheduler.routeSessions.count() {
		t.Fatalf("route sessions mutated %d->%d", sessBefore, gw.scheduler.routeSessions.count())
	}
}

func TestCredentialCheckNoPinOrSessionMutation(t *testing.T) {
	manager, _, _, _ := credentialAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-affinity-3"}, nil)
	gw := manager.current.Load().gateway
	seedBulkProbeCatalog(gw)
	stubPoolHits(gw, bulkChatSuccessBody("bulk-free-model"))
	cred := gw.authCreds[0]
	pinsBefore := gw.scheduler.pins.count()
	sessBefore := gw.scheduler.routeSessions.count()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, status, _, _ := gw.runCredentialCheck(ctx, TierZen, cred, "shared"); status != 200 {
		t.Fatalf("status=%d", status)
	}
	if got := gw.scheduler.pins.count(); got != pinsBefore {
		t.Fatalf("diagnostic must not create session pins: %d->%d", pinsBefore, got)
	}
	if got := gw.scheduler.routeSessions.count(); got != sessBefore {
		t.Fatalf("diagnostic must not create route-session overrides: %d->%d", sessBefore, got)
	}
	// No generation/establishment state may appear for the probe model.
	if _, ok := gw.scheduler.pinGet("bulk-free-model", "bulk-free-model"); ok {
		t.Fatal("diagnostic must not leave pins readable by probe model")
	}
}
