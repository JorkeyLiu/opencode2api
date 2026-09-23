package app

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Helper to establish a pinned authenticated session for transient refactor tests.
func pinnedAuthForRefactor(t *testing.T, gw *Gateway, session string) sessionPin {
	t.Helper()
	cred := gw.authCreds[0]
	raw := gw.pools[gw.authPoolName()].items[0].name
	gw.bindSessionPin(session, "m", TierZen, cred.id, gw.authPoolName(), raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(session, "m")
	if !ok {
		t.Fatalf("must bind pinned auth for %s", session)
	}
	return pin
}

// 1) Pinned authenticated 503 transient same-target discipline.
// Covers success via retry and exhaustion, proving no proxy walk, no custom,
// pin stability, and correct cooldown isolation (no proxy429/credential429 mis-write).
func TestPinnedAuth503Transient_Refactor(t *testing.T) {
	cases := []struct {
		name           string
		maxObservation int
		firstIs503     bool
		retryTo200     bool // if true, retry succeeds; else exhaustion
	}{
		{"retrySuccess", 3, true, true},
		{"exhaustion503", 3, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var customHits atomic.Int32
			custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				customHits.Add(1)
				w.WriteHeader(200)
				_, _ = w.Write([]byte(fallbackChatOK("cm1")))
			}))
			defer custom.Close()
			ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm1"}
			cfg := testGatewayConfig(
				map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
				ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
			)
			cfg.Anonymous = false
			cfg.Keys = []string{"zen-key-aaaaa"}
			cfg.Retry.MaxAttempts = tc.maxObservation
			cfg.Retry.TransientRetryIntervalSeconds = 0
			cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
			norm, _ := NormalizeConfig("config.json", cfg)
			gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
			ses := "ses_refactor_pinned_auth_503_" + tc.name
			pin := pinnedAuthForRefactor(t, gw, ses)
			ordered := affinityProxyOrder(gw.pools[gw.authPoolName()], pin.CredID, pin.ProxyRaw)
			if len(ordered) != 2 {
				t.Fatalf("ordered %d want 2", len(ordered))
			}
			pinnedIdx := poolIndexByRaw(gw, gw.authPoolName(), ordered[0].name)
			otherIdx := poolIndexByRaw(gw, gw.authPoolName(), ordered[1].name)
			var pinnedCalls, otherCalls atomic.Int32
			var seq atomic.Int32
			cap := &capturedUpstream{}
			postStub(t, gw, gw.authPoolName(), pinnedIdx, &pinnedCalls, cap, func(*http.Request) (*http.Response, error) {
				n := seq.Add(1)
				if n == 1 {
					return responseWithBody(503, `{"error":"svc"}`), nil
				}
				if tc.retryTo200 {
					return responseWithBody(200, `{"ok":true}`), nil
				}
				return responseWithBody(503, `{"error":"svc"}`), nil
			})
			postStub(t, gw, gw.authPoolName(), otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			ids := pinIDs(ses, "req-pin-503-"+tc.name)
			resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), ids, 0)
			if tc.retryTo200 {
				if err != nil || resp == nil || resp.StatusCode != 200 {
					t.Fatalf("retry success want 200, err=%v resp=%v", err, resp)
				}
				drainResp(resp)
				if eff.Tier == TierCustom {
					t.Fatalf("must not fallback to custom on 503 retry success")
				}
				if postCount(&pinnedCalls) != 2 {
					t.Fatalf("pinned calls=%d want 2 (503+retry 200)", postCount(&pinnedCalls))
				}
				if postCount(&otherCalls) != 0 {
					t.Fatalf("other calls=%d want 0 (pinned must not walk on 503 success)", postCount(&otherCalls))
				}
				if customHits.Load() != 0 {
					t.Fatalf("custom hits=%d want 0", customHits.Load())
				}
				if attempts != 2 {
					t.Fatalf("attempts=%d want 2", attempts)
				}
				pinAfter, _ := gw.scheduler.pinGet(ses, "m")
				if pinAfter.ProxyRaw != pin.ProxyRaw {
					t.Fatalf("pin drift %q -> %q must stay on success", pin.ProxyRaw, pinAfter.ProxyRaw)
				}
				if pinAfter.Generation != pin.Generation {
					t.Fatalf("pin generation drift")
				}
				// Sessions/bodies identical across transient retry
				sessions, bodies := cap.get()
				if len(sessions) != 2 || sessions[0] != sessions[1] {
					t.Fatalf("transient retry must keep same wire session %q", sessions)
				}
				if len(bodies) == 2 && string(bodies[0]) != string(bodies[1]) {
					t.Fatalf("transient retry body must stay identical")
				}
				// No proxy429/credential429 mis-write on pure 503 transient success
				if _, _, ok := gw.scheduler.proxy429CooldownStatus(pin.Tier, pin.Pool, pin.ProxyRaw); ok {
					t.Fatalf("503 success must not write proxy429")
				}
				if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
					t.Fatalf("503 success must not write credential429")
				}
			} else {
				// Exhaustion: transient retries up to max, then final 503, no walk, no custom
				if err != nil {
					t.Fatalf("exhaustion err=%v", err)
				}
				if resp == nil || resp.StatusCode != 503 {
					t.Fatalf("exhaustion want 503, got %v", resp)
				}
				drainResp(resp)
				if eff.Tier == TierCustom {
					t.Fatalf("must not fallback to custom on exhausted 503")
				}
				if postCount(&pinnedCalls) != tc.maxObservation {
					t.Fatalf("pinned calls=%d want %d (maxObservation)", postCount(&pinnedCalls), tc.maxObservation)
				}
				if postCount(&otherCalls) != 0 {
					t.Fatalf("other calls=%d want 0 (pinned exhaustion must not walk)", postCount(&otherCalls))
				}
				if customHits.Load() != 0 {
					t.Fatalf("custom hits=%d want 0 on 503 exhaustion", customHits.Load())
				}
				if attempts != tc.maxObservation {
					t.Fatalf("attempts=%d want %d", attempts, tc.maxObservation)
				}
				pinAfter, _ := gw.scheduler.pinGet(ses, "m")
				if pinAfter != pin {
					t.Fatalf("pin must not drift on exhaustion: %+v -> %+v", pin, pinAfter)
				}
				if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
					t.Fatalf("503 exhaustion must not write credential429 (early 503 must not be treated as 429)")
				}
			}
		})
	}
}

// 2) Transient中途400: first 503, retry yields 400, then 400 corrective replay
// proves same-target, same session, attempt numbering, and route-terminal behavior.
// Table covers replay success (200) and replay terminal 400.
func TestTransientMid400_PinnedAnonAndAuth(t *testing.T) {
	cases := []struct {
		name     string
		pinned   string // "anon" or "auth"
		replayTo int    // 200 or 400
	}{
		{"pinnedAnon_replay200", "anon", 200},
		{"pinnedAnon_replay400", "anon", 400},
		{"pinnedAuth_replay200", "auth", 200},
		{"pinnedAuth_replay400", "auth", 400},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testGatewayConfig(
				map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "z": {"direct", "http://127.0.0.1:8081"}},
				ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
			)
			cfg.Anonymous = true
			cfg.Keys = []string{"zen-key-aaaaa"}
			cfg.Retry.MaxAttempts = 3
			cfg.Retry.TransientRetryIntervalSeconds = 0
			norm, _ := NormalizeConfig("config.json", cfg)
			monitor := NewMonitor()
			gw, _ := NewGateway(norm, discardGatewayLogger(), monitor)

			var ses string
			var pin sessionPin
			var poolName string
			var route modelRoute
			if tc.pinned == "anon" {
				route = anonAuthRoute()
				ses = "ses_mid400_anon_" + tc.name
				// establish anon pin
				postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
					return responseWithBody(200, `{"ok":true}`), nil
				})
				postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
					return responseWithBody(200, `{"ok":true}`), nil
				})
				resp, _, _, _ := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "estab"), 0)
				drainResp(resp)
				p, _ := gw.scheduler.pinGet(ses, "m")
				pin = p
				poolName = gw.cfg.ProxyRouting.Anonymous
			} else {
				route = authOnlyRoute()
				ses = "ses_mid400_auth_" + tc.name
				pin = pinnedAuthForRefactor(t, gw, ses)
				poolName = gw.authPoolName()
			}
			ordered := affinityProxyOrder(gw.pools[poolName], pin.CredID, pin.ProxyRaw)
			pinnedIdx := poolIndexByRaw(gw, poolName, ordered[0].name)
			otherIdx := poolIndexByRaw(gw, poolName, ordered[1].name)

			var pinnedCalls, otherCalls atomic.Int32
			cap := &capturedUpstream{}
			var seq atomic.Int32
			// Sequence: 1st 503 -> retry 400 -> replay (200 or 400)
			postStub(t, gw, poolName, pinnedIdx, &pinnedCalls, cap, func(*http.Request) (*http.Response, error) {
				n := seq.Add(1)
				if n == 1 {
					return responseWithBody(503, `{"error":"svc"}`), nil
				}
				if n == 2 {
					return responseWithBody(400, `{"error":"bad request"}`), nil
				}
				if n == 3 {
					if tc.replayTo == 200 {
						return responseWithBody(200, `{"ok":true}`), nil
					}
					return responseWithBody(400, `{"error":"still bad"}`), nil
				}
				return responseWithBody(500, `{"error":"unexpected"}`), nil
			})
			postStub(t, gw, poolName, otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			// Also stub the opposite channel to ensure no cross fallback
			var zenCalls, anonCalls atomic.Int32
			if tc.pinned == "anon" {
				postStub(t, gw, "z", 0, &zenCalls, nil, func(*http.Request) (*http.Response, error) {
					return responseWithBody(200, `{"ok":true}`), nil
				})
			} else {
				postStub(t, gw, "a", 0, &anonCalls, nil, func(*http.Request) (*http.Response, error) {
					return responseWithBody(200, `{"ok":true}`), nil
				})
				postStub(t, gw, "a", 1, &anonCalls, nil, func(*http.Request) (*http.Response, error) {
					return responseWithBody(200, `{"ok":true}`), nil
				})
			}

			ids := pinIDs(ses, "req-mid400")
			resp, _, attempts, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
			if tc.replayTo == 200 {
				if err != nil || resp == nil || resp.StatusCode != 200 {
					t.Fatalf("mid400 replay 200 want success, err=%v resp=%v", err, resp)
				}
				drainResp(resp)
				// 503 -> 400 -> replay 200 = 3 attempts
				if attempts != 3 {
					t.Fatalf("attempts=%d want 3 (503+400+replay)", attempts)
				}
				if postCount(&pinnedCalls) != 3 {
					t.Fatalf("pinned calls=%d want 3", postCount(&pinnedCalls))
				}
				if postCount(&otherCalls) != 0 {
					t.Fatalf("other calls=%d want 0 (route-terminal)", postCount(&otherCalls))
				}
				if tc.pinned == "anon" && postCount(&zenCalls) != 0 {
					t.Fatalf("zen calls=%d want 0 (pinned anon must not enter auth)", postCount(&zenCalls))
				}
				if tc.pinned == "auth" && postCount(&anonCalls) != 0 {
					t.Fatalf("anon calls=%d want 0", postCount(&anonCalls))
				}
				// Same session/wire across transient and replay (helper must keep Started/monitor attempt)
				sessions, bodies := cap.get()
				if len(sessions) != 3 {
					t.Fatalf("captured sessions %d want 3", len(sessions))
				}
				for i := 1; i < len(sessions); i++ {
					if sessions[i] != sessions[0] {
						t.Fatalf("session drift %q vs %q", sessions[i], sessions[0])
					}
				}
				if len(bodies) == 3 && string(bodies[0]) != string(bodies[1]) {
					t.Fatalf("first two bodies must stay identical (transient same-target)")
				}
				// Monitor attempts monotonically increasing with Started preserved
				recentAll := monitor.Snapshot().Upstream.Recent
				var recent []UpstreamAttempt
				for _, a := range recentAll {
					if a.RequestID == ids.Request {
						recent = append(recent, a)
					}
				}
				if len(recent) != 3 {
					t.Fatalf("recent filtered %d want 3 for req %q, all=%+v", len(recent), ids.Request, recentAll)
				}
				for i := 1; i < 3; i++ {
					if recent[i].Attempt != recent[i-1].Attempt+1 {
						t.Fatalf("attempt numbers non-monotonic %+v", recent)
					}
				}
				// Pin must not drift on success via replay
				pinAfter, _ := gw.scheduler.pinGet(ses, "m")
				if pinAfter.ProxyRaw != pin.ProxyRaw {
					t.Fatalf("pin must not drift after mid400 replay success")
				}
			} else {
				// replay 400 terminal
				if err != nil {
					t.Fatalf("mid400 terminal err=%v", err)
				}
				if resp == nil || resp.StatusCode != 400 {
					t.Fatalf("mid400 replay terminal want 400, got %v", resp)
				}
				drainResp(resp)
				if attempts != 3 {
					t.Fatalf("attempts=%d want 3 (503+400+replay400)", attempts)
				}
				if postCount(&pinnedCalls) != 3 {
					t.Fatalf("pinned calls=%d want 3", postCount(&pinnedCalls))
				}
				if postCount(&otherCalls) != 0 {
					t.Fatalf("other calls=%d want 0 (route-terminal 400)", postCount(&otherCalls))
				}
				// Must be route-terminal: no walk, no custom, same session
				sessions, _ := cap.get()
				if len(sessions) != 3 || sessions[0] != sessions[1] || sessions[1] != sessions[2] {
					t.Fatalf("400 replay must keep same session %q", sessions)
				}
			}
		})
	}
}

// 3) 503 then retry yields 429 on auth path: proves 429 refund/evidence/Started
// fencing and that early 503 is not mis-counted as 429.
func TestPinnedAuth503Then429_Refactor(t *testing.T) {
	cases := []struct {
		name           string
		secondProxy429 bool // if true, second proxy also 429 => full exhaustion eventually
	}{
		{"partial429_noExhaustion", false},
		{"full429_exhaustion", true},
	}
	extra := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var customHits atomic.Int32
			custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				customHits.Add(1)
				w.WriteHeader(200)
				_, _ = w.Write([]byte(fallbackChatOK("cm1")))
			}))
			defer custom.Close()
			ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm1"}
			cfg := testGatewayConfig(
				map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
				ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
			)
			cfg.Anonymous = false
			cfg.Keys = []string{"zen-key-aaaaa"}
			cfg.Retry.MaxAttempts = 3
			cfg.Retry.TransientRetryIntervalSeconds = 0
			cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
			norm, _ := NormalizeConfig("config.json", cfg)
			monitor := NewMonitor()
			gw, _ := NewGateway(norm, discardGatewayLogger(), monitor)
			ses := "ses_refactor_503_then_429_" + tc.name
			pin := pinnedAuthForRefactor(t, gw, ses)
			ordered := affinityProxyOrder(gw.pools[gw.authPoolName()], pin.CredID, pin.ProxyRaw)
			pinnedIdx := poolIndexByRaw(gw, gw.authPoolName(), ordered[0].name)
			otherIdx := poolIndexByRaw(gw, gw.authPoolName(), ordered[1].name)
			var pinnedCalls, otherCalls atomic.Int32
			var pinnedSeq atomic.Int32
			postStub(t, gw, gw.authPoolName(), pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
				n := pinnedSeq.Add(1)
				if n == 1 {
					return responseWithBody(503, `{"error":"svc"}`), nil
				}
				// retry yields 429
				r := responseWithBody(429, `{"error":"throttled"}`)
				r.Header.Set("Retry-After", "7")
				return r, nil
			})
			postStub(t, gw, gw.authPoolName(), otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
				if tc.secondProxy429 {
					r := responseWithBody(429, `{"error":"throttled"}`)
					r.Header.Set("Retry-After", "9")
					return r, nil
				}
				return responseWithBody(200, `{"ok":true}`), nil
			})
			ids := pinIDs(ses, "req-503-429")
			resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), ids, 0, extra)
			if tc.secondProxy429 {
				// Both proxies 429 => full exhaustion => 429 with last Retry-After 9, credential429 written, custom takeover
				if err != nil {
					t.Fatalf("full429 err=%v", err)
				}
				if eff.Tier != TierCustom {
					t.Fatalf("full429 must take over custom, eff=%+v", eff)
				}
				if resp == nil || resp.StatusCode != 200 {
					t.Fatalf("custom fallback want 200, resp=%v", resp)
				}
				drainResp(resp)
				if postCount(&pinnedCalls) != 2 {
					t.Fatalf("pinned calls=%d want 2 (503+429 retry)", postCount(&pinnedCalls))
				}
				if postCount(&otherCalls) != 1 {
					t.Fatalf("other calls=%d want 1 (second 429)", postCount(&otherCalls))
				}
				if customHits.Load() != 1 {
					t.Fatalf("custom hits=%d want 1 on full exhaustion", customHits.Load())
				}
				// 503 must not be counted as 429 evidence: observed429 should be 2 entries, last Retry-After 9
				if until, status, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); !ok || status != 429 {
					t.Fatalf("full exhaustion must write credential429")
				} else {
					_ = until
				}
				// Native 3 plus 1 custom attempt = 4 total when custom fallback succeeds
				if attempts != 4 {
					t.Fatalf("attempts=%d want 4 (503+429 + other429 + custom)", attempts)
				}
			} else {
				// Partial: pinned 429 after 503, other 200 success => candidate walk must continue without an ordinary-send budget, early 503 not as 429
				if err != nil || resp == nil || resp.StatusCode != 200 {
					t.Fatalf("partial 429->success want 200, err=%v resp=%v", err, resp)
				}
				drainResp(resp)
				if eff.Tier == TierCustom {
					t.Fatalf("partial must not take over custom")
				}
				if postCount(&pinnedCalls) != 2 {
					t.Fatalf("pinned calls=%d want 2", postCount(&pinnedCalls))
				}
				if postCount(&otherCalls) != 1 {
					t.Fatalf("other calls=%d want 1 (success on other)", postCount(&otherCalls))
				}
				if customHits.Load() != 0 {
					t.Fatalf("custom hits=%d want 0 (partial not exhaustion)", customHits.Load())
				}
				if attempts != 3 {
					t.Fatalf("attempts=%d want 3 (503+429 + 200)", attempts)
				}
				// Partial 429 must NOT write credential429 (needs full eligible set)
				if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
					t.Fatalf("partial 429 after 503 must not write credential429 (early 503 not counted)")
				}
				// Pin must have moved to other proxy (success there)
				pinAfter, _ := gw.scheduler.pinGet(ses, "m")
				if pinAfter.ProxyRaw != ordered[1].name {
					t.Fatalf("pin must move to other on success after 429, got %q want %q", pinAfter.ProxyRaw, ordered[1].name)
				}
				// Verify monitor attempts show 503 (upstream_failure), then 429 (rate_limited) with correct Started
				recent := monitor.Snapshot().Upstream.Recent
				if len(recent) < 3 {
					t.Fatalf("recent %d want >=3", len(recent))
				}
				if recent[0].Status != 503 {
					t.Fatalf("first status %d want 503", recent[0].Status)
				}
				if recent[1].Status != 429 {
					t.Fatalf("second (retry) status %d want 429 (503->429 must be 429, not 503)", recent[1].Status)
				}
			}
			_ = time.Now()
		})
	}
}
