package app

import (
	"net/http"
	"testing"
)

// Canonical domain decision table: the single domain authority owns all
// fallback/exhaustion policy. Optimal bad-node repair: pinned valid-binding
// object-unavailable (single L1-final with no Consumed prerequisite, full
// walk, stable credential-401) and valid zero-send (all actual proxies
// filtered incl. pre-cooled 429) allow custom; tombstone/cancel/400 never do.
// Full live-429 stays the distinct credential429 proof.
func TestDecideDomainRecovery(t *testing.T) {
	mkUnbound := func(entered bool, frozen, unavailable int, recovered bool) unboundDomainEvidence {
		return unboundDomainEvidence{Domain: "d", Entered: entered, Frozen: frozen, Unavailable: unavailable, Recovered400: recovered}
	}
	cases := []struct {
		name  string
		in    domainRecoveryInput
		want  domainExhaustionKind
		allow bool
	}{
		// Unbound AND: every domain must exhaust independently (unchanged).
		{name: "unbound single exhausted allow", in: domainRecoveryInput{UnboundDomains: []unboundDomainEvidence{mkUnbound(true, 1, 1, false)}}, want: domainUnboundExhausted, allow: true},
		{name: "unbound two domains both exhausted allow", in: domainRecoveryInput{UnboundDomains: []unboundDomainEvidence{{Domain: "anonymous", Entered: true, Frozen: 2, Unavailable: 2}, {Domain: "cred", Entered: true, Frozen: 1, Unavailable: 1}}}, want: domainUnboundExhausted, allow: true},
		{name: "unbound partial deny", in: domainRecoveryInput{UnboundDomains: []unboundDomainEvidence{mkUnbound(true, 2, 1, false)}}, want: domainNone, allow: false},
		{name: "unbound unentered deny", in: domainRecoveryInput{UnboundDomains: []unboundDomainEvidence{mkUnbound(false, 1, 1, false)}}, want: domainNone, allow: false},
		{name: "unbound zero frozen deny", in: domainRecoveryInput{UnboundDomains: []unboundDomainEvidence{mkUnbound(true, 0, 0, false)}}, want: domainNone, allow: false},
		{name: "unbound empty slice deny", in: domainRecoveryInput{UnboundDomains: []unboundDomainEvidence{}}, want: domainNone, allow: false},
		{name: "unbound nil domains deny", in: domainRecoveryInput{}, want: domainNone, allow: false},
		{name: "unbound recovered400 per-domain deny", in: domainRecoveryInput{UnboundDomains: []unboundDomainEvidence{mkUnbound(true, 1, 1, true)}}, want: domainNone, allow: false},
		{name: "unbound recovered400 global deny", in: domainRecoveryInput{Recovered400: true, UnboundDomains: []unboundDomainEvidence{mkUnbound(true, 1, 1, false)}}, want: domainUnboundExhausted, allow: false},
		{name: "unbound cancelled deny allowCustom false but kind stays", in: domainRecoveryInput{Cancelled: true, UnboundDomains: []unboundDomainEvidence{mkUnbound(true, 1, 1, false)}}, want: domainUnboundExhausted, allow: false},
		{name: "unbound committed deny", in: domainRecoveryInput{Committed: true, UnboundDomains: []unboundDomainEvidence{mkUnbound(true, 1, 1, false)}}, want: domainUnboundExhausted, allow: false},
		{name: "unbound credential not merged deny", in: domainRecoveryInput{UnboundDomains: []unboundDomainEvidence{mkUnbound(true, 1, 1, false), mkUnbound(true, 2, 1, false)}}, want: domainNone, allow: false},
		// Pinned full live429 allow vs deny (distinct credential429 proof).
		{name: "pinned full live429 allow", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 2, TerminalStatus: 429}}, want: domainPinnedFullLive429, allow: true},
		{name: "pinned full single allow", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 1, ObservedLive429: 1, TerminalStatus: 429}}, want: domainPinnedFullLive429, allow: true},
		{name: "pinned full exceed allow", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 3, TerminalStatus: 429}}, want: domainPinnedFullLive429, allow: true},
		{name: "pinned full partial deny", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 1, TerminalStatus: 429}}, want: domainNone, allow: false},
		{name: "pinned full zero eligible deny", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 0, ObservedLive429: 0, TerminalStatus: 429}}, want: domainNone, allow: false},
		{name: "pinned full no evidence deny", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 0, TerminalStatus: 429}}, want: domainNone, allow: false},
		{name: "pinned full terminal 400 deny", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 2, TerminalStatus: 400}}, want: domainNone, allow: false},
		{name: "pinned full terminal 401 deny", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 2, TerminalStatus: 401}}, want: domainNone, allow: false},
		{name: "pinned full terminal 403 deny", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 2, TerminalStatus: 403}}, want: domainNone, allow: false},
		{name: "pinned full terminal 503 deny", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 2, TerminalStatus: 503}}, want: domainNone, allow: false},
		{name: "pinned full terminal transport deny", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 2, TerminalStatus: 0}}, want: domainNone, allow: false},
		{name: "pinned full recovered400 deny allow", in: domainRecoveryInput{Recovered400: true, Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 2, TerminalStatus: 429}}, want: domainPinnedFullLive429, allow: false},
		{name: "pinned full cancelled deny allow", in: domainRecoveryInput{Cancelled: true, Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 2, TerminalStatus: 429}}, want: domainPinnedFullLive429, allow: false},
		{name: "pinned full committed deny", in: domainRecoveryInput{Committed: true, Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 2, TerminalStatus: 429}}, want: domainPinnedFullLive429, allow: false},
		// Pinned object-unavailable: no Consumed prerequisite (optimal repair).
		{name: "pinned object allow 403 full walk", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, Attempted: 2, Consumed: true, FinalIsObjectUnavailable: true, FinalIsLive429: false}}, want: domainPinnedObjectUnavailable, allow: true},
		{name: "pinned object allow transport full walk", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, Attempted: 2, Consumed: true, FinalIsObjectUnavailable: true, FinalIsLive429: false}}, want: domainPinnedObjectUnavailable, allow: true},
		{name: "pinned object allow no consumed", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, Attempted: 2, Consumed: false, FinalIsObjectUnavailable: true}}, want: domainPinnedObjectUnavailable, allow: true},
		{name: "pinned object allow single initial non429", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 1, Attempted: 1, Consumed: false, FinalIsObjectUnavailable: true}}, want: domainPinnedObjectUnavailable, allow: true},
		{name: "pinned object allow credential401 single", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 1, Attempted: 1, FinalIsObjectUnavailable: true, Credential401: true}}, want: domainPinnedObjectUnavailable, allow: true},
		{name: "pinned object allow credential401 multi no full walk", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 3, Attempted: 1, FinalIsObjectUnavailable: true, Credential401: true}}, want: domainPinnedObjectUnavailable, allow: true},
		{name: "pinned object deny attempted < eligible non401", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, Attempted: 1, Consumed: true, FinalIsObjectUnavailable: true}}, want: domainNone, allow: false},
		{name: "pinned object deny non-unavailable", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, Attempted: 2, Consumed: true, FinalIsObjectUnavailable: false}}, want: domainNone, allow: false},
		{name: "pinned object deny 429 final", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, Attempted: 2, Consumed: true, FinalIsObjectUnavailable: false, FinalIsLive429: true}}, want: domainNone, allow: false},
		{name: "pinned object deny cancelled", in: domainRecoveryInput{Cancelled: true, Pinned: &pinnedDomainEvidence{Eligible: 2, Attempted: 2, Consumed: true, FinalIsObjectUnavailable: true}}, want: domainPinnedObjectUnavailable, allow: false},
		{name: "pinned object deny recovered400", in: domainRecoveryInput{Recovered400: true, Pinned: &pinnedDomainEvidence{Eligible: 2, Attempted: 2, Consumed: true, FinalIsObjectUnavailable: true}}, want: domainPinnedObjectUnavailable, allow: false},
		{name: "pinned object deny zero eligible without flag", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 0, Attempted: 0, Consumed: true, FinalIsObjectUnavailable: true}}, want: domainNone, allow: false},
		{name: "pinned object deny ordinary", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, Attempted: 2, Consumed: true, FinalIsObjectUnavailable: false}}, want: domainNone, allow: false},
		// Pinned zero-send valid exhaustion (incl. pre-cooled 429).
		{name: "pinned zero-send allow", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 0, ZeroSendExhausted: true}}, want: domainPinnedZeroSend, allow: true},
		{name: "pinned zero-send deny cancelled", in: domainRecoveryInput{Cancelled: true, Pinned: &pinnedDomainEvidence{Eligible: 0, ZeroSendExhausted: true}}, want: domainPinnedZeroSend, allow: false},
		{name: "pinned zero-send deny recovered400", in: domainRecoveryInput{Recovered400: true, Pinned: &pinnedDomainEvidence{Eligible: 0, ZeroSendExhausted: true}}, want: domainPinnedZeroSend, allow: false},
		{name: "pinned zero-send deny without flag", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 0}}, want: domainNone, allow: false},
		// Proof kinds distinct: same eligible/terminal but different proofs.
		{name: "proof distinct full vs object", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 2, TerminalStatus: 429, Attempted: 2, Consumed: true, FinalIsObjectUnavailable: true}}, want: domainPinnedFullLive429, allow: true},
		// No raw status-driven shortcut: single 503 without full walk must not allow.
		{name: "no shortcut single 503 partial", in: domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 2, ObservedLive429: 0, TerminalStatus: 503, Attempted: 1, Consumed: false, FinalIsObjectUnavailable: true}}, want: domainNone, allow: false},
		{name: "no shortcut unbound single domain partial", in: domainRecoveryInput{UnboundDomains: []unboundDomainEvidence{mkUnbound(true, 2, 1, false)}}, want: domainNone, allow: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decideDomainRecovery(tc.in)
			if got.Kind != tc.want || got.AllowCustom != tc.allow {
				t.Fatalf("decideDomainRecovery(%+v) = %+v, want Kind=%v Allow=%v", tc.in, got, tc.want, tc.allow)
			}
			// Verify no raw status shortcut: allow must be false when Kind is None.
			if got.Kind == domainNone && got.AllowCustom {
				t.Fatalf("no exhaustion must never allow custom: %+v", got)
			}
		})
	}
}

// Verify leaf evidence classifiers remain pure and status-correct.
func TestDomainDecisionLeafEvidence(t *testing.T) {
	if !finalNon429ObjectUnavailable(responseWithBody(401, `x`), nil) {
		t.Fatalf("401 must be unavailable")
	}
	if finalNon429ObjectUnavailable(responseWithBody(429, `x`), nil) {
		t.Fatalf("429 must not be finalNon429")
	}
	if !unboundObjectUnavailable(responseWithBody(429, `x`), nil) {
		t.Fatalf("unbound 429 must count")
	}
	if unboundObjectUnavailable(responseWithBody(400, `x`), nil) {
		t.Fatalf("400 must not count")
	}
	_ = http.StatusTooManyRequests
}
