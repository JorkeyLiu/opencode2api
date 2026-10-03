package app

import (
	"errors"
	"net/http"
	"testing"
)

// Shared leaf: non-429 final-unavailable classification. 401/403, L1-final
// 408/425/5xx (incl. 503), true transport, and stream-startup sentinel count;
// 400 (corrective replay), ordinary 4xx, 2xx, nil/nil, and 429 never count.
func TestFinalNon429ObjectUnavailable(t *testing.T) {
	transportErr := errors.New("boom")
	startupErr := errors.New("upstream stream startup failure")
	cases := []struct {
		name string
		resp *http.Response
		err  error
		want bool
	}{
		{name: "401", resp: responseWithBody(401, `{"error":"u"}`), want: true},
		{name: "403", resp: responseWithBody(403, `{"error":"f"}`), want: true},
		{name: "408 L1-final", resp: responseWithBody(408, `{"error":"t"}`), want: true},
		{name: "425 L1-final", resp: responseWithBody(425, `{"error":"e"}`), want: true},
		{name: "500 L1-final", resp: responseWithBody(500, `{"error":"b"}`), want: true},
		{name: "502 L1-final", resp: responseWithBody(502, `{"error":"b"}`), want: true},
		{name: "503 L1-final no separate loop", resp: responseWithBody(503, `{"error":"u"}`), want: true},
		{name: "599 L1-final", resp: responseWithBody(599, `{"error":"b"}`), want: true},
		{name: "true transport", err: transportErr, want: true},
		{name: "stream startup sentinel", err: startupErr, want: true},
		{name: "400 corrective replay never unavailable", resp: responseWithBody(400, `{"error":"bad"}`), want: false},
		{name: "ordinary 404 never unavailable", resp: responseWithBody(404, `{"error":"n"}`), want: false},
		{name: "ordinary 422 never unavailable", resp: responseWithBody(422, `{"error":"n"}`), want: false},
		{name: "2xx never unavailable", resp: responseWithBody(200, `{"ok":true}`), want: false},
		{name: "429 never unavailable at leaf", resp: responseWithBody(429, `{"error":"t"}`), want: false},
		{name: "nil nil never unavailable", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := finalNon429ObjectUnavailable(tc.resp, tc.err); got != tc.want {
				t.Fatalf("leaf=%v want %v (status=%v err=%v)", got, tc.want, statusOf(tc.resp), tc.err)
			}
		})
	}
}

func statusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

// Unbound wrapper additionally counts live 429 per frozen candidate.
func TestUnboundObjectUnavailableLive429Counts(t *testing.T) {
	if !unboundObjectUnavailable(responseWithBody(429, `{"error":"t"}`), nil) {
		t.Fatalf("unbound live 429 must count")
	}
	if unboundObjectUnavailable(responseWithBody(400, `{"error":"bad"}`), nil) {
		t.Fatalf("unbound 400 must never count")
	}
	if unboundObjectUnavailable(responseWithBody(404, `{"error":"n"}`), nil) {
		t.Fatalf("unbound ordinary 4xx must never count")
	}
	if !unboundObjectUnavailable(responseWithBody(503, `{"error":"u"}`), nil) {
		t.Fatalf("unbound L1-final 503 must count")
	}
}

// Pinned consumption via the single domain authority: 429 stays on its own
// full-live-429 proof, consumption requires consumed+full attempt set.
func TestPinnedConsumptionWrapper429AndGates(t *testing.T) {
	r429 := responseWithBody(429, `{"error":"t"}`)
	r403 := responseWithBody(403, `{"error":"f"}`)
	r400 := responseWithBody(400, `{"error":"bad"}`)
	r404 := responseWithBody(404, `{"error":"n"}`)
	transportErr := errors.New("boom")
	decide := func(eligible, attempted int, consumed, cancelled bool, resp *http.Response, err error) domainRecoveryResult {
		return decideDomainRecovery(domainRecoveryInput{Cancelled: cancelled, Pinned: &pinnedDomainEvidence{Eligible: eligible, Attempted: attempted, Consumed: consumed, FinalIsObjectUnavailable: finalNon429ObjectUnavailable(resp, err), FinalIsLive429: resp != nil && resp.StatusCode == 429}})
	}
	if decide(2, 2, true, false, r429, nil).AllowCustom {
		t.Fatalf("pinned 429 must not qualify via consumption (separate full-live-429 gate owns it)")
	}
	if !decide(2, 2, true, false, r403, nil).AllowCustom {
		t.Fatalf("pinned consumed+full 403 must qualify")
	}
	if !decide(2, 2, true, false, nil, transportErr).AllowCustom {
		t.Fatalf("pinned consumed+full transport must qualify")
	}
	// Optimal repair: Consumed is observability only, never a prerequisite.
	if !decide(2, 2, false, false, r403, nil).AllowCustom {
		t.Fatalf("pinned full 403 without consumed must qualify")
	}
	if !decide(1, 1, false, false, r403, nil).AllowCustom {
		t.Fatalf("single-proxy non-429 must qualify")
	}
	if decide(2, 1, true, false, r403, nil).AllowCustom {
		t.Fatalf("partial eligible unattempted must not qualify")
	}
	if decide(2, 2, true, true, r403, nil).AllowCustom {
		t.Fatalf("cancelled must never qualify")
	}
	if decide(2, 2, true, false, r400, nil).AllowCustom {
		t.Fatalf("pinned exact-400 replay final must never qualify")
	}
	if decide(2, 2, true, false, r404, nil).AllowCustom {
		t.Fatalf("pinned ordinary 4xx must never qualify")
	}
	if decide(0, 0, true, false, r403, nil).AllowCustom {
		t.Fatalf("zero eligible without flag must never qualify")
	}
	// Zero-send valid exhaustion is a separate flag, not Attempted counts.
	if !decideDomainRecovery(domainRecoveryInput{Pinned: &pinnedDomainEvidence{Eligible: 0, ZeroSendExhausted: true}}).AllowCustom {
		t.Fatalf("valid zero-send must qualify")
	}
}
