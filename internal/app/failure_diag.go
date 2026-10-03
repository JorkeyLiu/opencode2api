package app

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// Transport failure diagnosis: observable facts only, never blame.
// failureDiag is additive metadata on UpstreamAttempt/history attempts.
// FailureClass/Outcome stay the shared recovery-compatible classification;
// stage/reason only describe what was observed at the failed send or the
// pre-commit stream gate. Success attempts never carry a diagnosis.
// Raw error strings, bodies, headers, keys, and URLs never leave this file:
// every output is a fixed whitelist enum; unknown input falls back to
// unknown/empty and never to a node-blaming label.

const (
	FailureStageConnect         = "connect"
	FailureStageDNS             = "dns"
	FailureStageTLS             = "tls"
	FailureStageResponseHeaders = "response_headers"
	FailureStageStreamStartup   = "stream_startup"
	FailureStageRequest         = "request"
	FailureStageUnknown         = "unknown"
)

const (
	FailureReasonRequestBudgetExhausted = "request_budget_exhausted"
	FailureReasonCallerCancelled        = "caller_cancelled"
	FailureReasonResponseHeaderTimeout  = "response_header_timeout"
	FailureReasonConnectRefused         = "connect_refused"
	FailureReasonConnectTimeout         = "connect_timeout"
	FailureReasonDNSError               = "dns_error"
	FailureReasonTLSError               = "tls_error"
	FailureReasonConnectionReset        = "connection_reset"
	FailureReasonUnexpectedEOF          = "unexpected_eof"
	FailureReasonStreamStartupError     = "stream_startup_error"
	FailureReasonStreamStartupTimeout   = "stream_startup_timeout"
	FailureReasonTransportTimeout       = "transport_timeout"
	FailureReasonTransportError         = "transport_error"
	FailureReasonUnknown                = "unknown"
)

// failureDiag is the sanitized per-attempt transport diagnosis.
type failureDiag struct {
	Stage  string
	Reason string
}

func (d failureDiag) empty() bool { return d.Stage == "" && d.Reason == "" }

func validFailureStage(s string) bool {
	switch s {
	case FailureStageConnect, FailureStageDNS, FailureStageTLS,
		FailureStageResponseHeaders, FailureStageStreamStartup,
		FailureStageRequest, FailureStageUnknown:
		return true
	}
	return false
}

func validFailureReason(r string) bool {
	switch r {
	case FailureReasonRequestBudgetExhausted, FailureReasonCallerCancelled,
		FailureReasonResponseHeaderTimeout, FailureReasonConnectRefused,
		FailureReasonConnectTimeout, FailureReasonDNSError,
		FailureReasonTLSError, FailureReasonConnectionReset,
		FailureReasonUnexpectedEOF, FailureReasonStreamStartupError,
		FailureReasonStreamStartupTimeout,
		FailureReasonTransportTimeout, FailureReasonTransportError,
		FailureReasonUnknown:
		return true
	}
	return false
}

// sanitizeFailureDiag enforces the projection boundary: success carries
// nothing, real HTTP 400/429/503 responses carry nothing (the status plus
// FailureClass already describe them), empty stays empty for old records,
// and any non-whitelist injection collapses to unknown (never raw).
func sanitizeFailureDiag(success bool, status int, stage, reason string) (string, string) {
	if success {
		return "", ""
	}
	if status == http.StatusBadRequest || status == http.StatusTooManyRequests ||
		status == http.StatusServiceUnavailable {
		return "", ""
	}
	if stage == "" && reason == "" {
		return "", ""
	}
	if !validFailureStage(stage) {
		stage = FailureStageUnknown
	}
	if !validFailureReason(reason) {
		reason = FailureReasonUnknown
	}
	if stage == "" {
		stage = FailureStageUnknown
	}
	if reason == "" {
		reason = FailureReasonUnknown
	}
	return stage, reason
}

// nonstreamDeadlineKey carries the request-local whole-request deadline fact
// for non-stream inference (context.WithTimeout child of the client context).
// It is observation-only metadata: no lifecycle, no causality change. The
// stored parent lets the classifier tell the gateway's own deadline (parent
// still live) from caller cancellation (parent already done).
type nonstreamDeadlineKey struct{}

type nonstreamDeadlineInfo struct {
	parent   context.Context
	deadline time.Time
}

func withNonstreamDeadline(ctx, parent context.Context, d time.Duration) context.Context {
	if ctx == nil || d <= 0 {
		return ctx
	}
	return context.WithValue(ctx, nonstreamDeadlineKey{}, nonstreamDeadlineInfo{
		parent:   parent,
		deadline: time.Now().Add(d),
	})
}

// ownNonstreamDeadlineExceeded reports the gateway's own whole-request
// deadline fact: the derived context expired by deadline while the caller
// parent is still live. A concurrently cancelled parent is conservatively a
// caller cancel, never a budget claim. The verdict reads only the context
// lifecycle facts (derived DeadlineExceeded plus a live parent); elapsed
// wall-clock is never used to guess a cause.
func ownNonstreamDeadlineExceeded(ctx context.Context) bool {
	if ctx == nil || ctx.Err() != context.DeadlineExceeded {
		return false
	}
	info, ok := ctx.Value(nonstreamDeadlineKey{}).(nonstreamDeadlineInfo)
	if !ok {
		return false
	}
	if info.parent != nil && info.parent.Err() != nil {
		return false
	}
	return true
}

func failureDiagBudget(ctx context.Context, stageHint string) failureDiag {
	stage := FailureStageRequest
	if stageHint == FailureStageStreamStartup || isStreamContext(ctx) {
		stage = FailureStageStreamStartup
	}
	return failureDiag{Stage: stage, Reason: FailureReasonRequestBudgetExhausted}
}

func failureDiagCallerCancel() failureDiag {
	return failureDiag{Stage: FailureStageRequest, Reason: FailureReasonCallerCancelled}
}

// classifyFailureDiag is the single diagnostic authority for failed upstream
// attempts. Inputs are the request context lifecycle (stream budget state,
// own non-stream deadline vs parent cancel), the send outcome (resp/err),
// and the explicit call phase (stageHint "request" for Do sends,
// "stream_startup" for pre-commit gate outcomes, plus the gate cause before
// any sentinel replacement). Success and real 400/429/503 responses yield an
// empty diagnosis. Priority is lifecycle-first, then gate fact, then
// conservative error-shape evidence:
//
//  1. stream startup budget expiry (controller expired fact) wins over every
//     ctx error reading.
//  2. the gateway's own non-stream deadline (derived deadline exceeded while
//     the caller parent is still live) is a budget fact; any other done
//     context is a caller cancel (pan-parent context is never located).
//  3. a pre-commit gate failure (sentinel or gate cause) unifies to
//     stream_startup_error without splitting parse/EOF/error-event.
//  4. typed error evidence only promotes connect/dns/tls/response_headers
//     when the chain proves it (net.OpError dial, net.DNSError, TLS chain,
//     the fixed "awaiting response headers" timeout phrase). A bare
//     Timeout()==true never implies connect; broad SOCKS errors stay a
//     neutral transport_error, never a bad-node label. Anything else is
//     unknown/transport_timeout/transport_error, never invented locality.
func classifyFailureDiag(ctx context.Context, resp *http.Response, err error, stageHint string, gateCause error) failureDiag {
	// Candidate-local pre-commit startup timeout (per-attempt attempt_timeout
	// expiry while the parent request is still live) is an observable
	// node/target failure, never caller cancellation or request budget.
	// It wins over generic ctx readings so the stall cools the target and
	// walks/falls back instead of bypassing recovery as neutral.
	if isStreamStartupTimeoutErr(err) || isStreamStartupTimeoutErr(gateCause) {
		return failureDiag{Stage: FailureStageStreamStartup, Reason: FailureReasonStreamStartupTimeout}
	}
	if err == nil {
		if resp == nil {
			return failureDiag{}
		}
		if resp.StatusCode/100 == 2 {
			return failureDiag{}
		}
		// Real HTTP error responses: status plus FailureClass already
		// describe them; only the pre-commit startup pseudo-outcome may
		// carry a gate diagnosis alongside its preserved status.
		if stageHint == FailureStageStreamStartup {
			if gateCause != nil || isStreamStartupFailureErr(err) {
				return failureDiag{Stage: FailureStageStreamStartup, Reason: FailureReasonStreamStartupError}
			}
			return failureDiag{Stage: FailureStageStreamStartup, Reason: FailureReasonStreamStartupError}
		}
		return failureDiag{}
	}
	// Failed send below.
	if streamStartupExpired(ctx) {
		return failureDiagBudget(ctx, stageHint)
	}
	if ctx != nil && ctx.Err() != nil {
		if ownNonstreamDeadlineExceeded(ctx) {
			return failureDiagBudget(ctx, stageHint)
		}
		return failureDiagCallerCancel()
	}
	if isStreamStartupFailureErr(err) || gateCause != nil {
		return failureDiag{Stage: FailureStageStreamStartup, Reason: FailureReasonStreamStartupError}
	}
	if stageHint == FailureStageStreamStartup && err != nil && resp == nil {
		// Pre-commit gate read/parse outcome without a separable cause is
		// still a startup fact (unified, no parse/EOF split).
		if isGateReadOrParseErr(err, gateCause) {
			return failureDiag{Stage: FailureStageStreamStartup, Reason: FailureReasonStreamStartupError}
		}
	}
	return classifyTransportErrShape(err, stageHint)
}

func isGateReadOrParseErr(err, gateCause error) bool {
	if gateCause != nil {
		return true
	}
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errSSEUnexpectedEOF) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "upstream stream startup failure") ||
		strings.Contains(msg, "upstream stream error") ||
		strings.Contains(msg, "unexpected eof") ||
		strings.Contains(msg, "eof")
}

// timeoutError is the minimal Timeout() probe; it never implies connect.
type timeoutError interface{ Timeout() bool }

func classifyTransportErrShape(err error, stageHint string) failureDiag {
	msg := strings.ToLower(err.Error())
	// SOCKS facility errors are neutral transport facts, never connect proof
	// and never a bad-node verdict.
	if strings.Contains(msg, "socks") {
		return failureDiag{Stage: FailureStageRequest, Reason: FailureReasonTransportError}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return failureDiag{Stage: FailureStageDNS, Reason: FailureReasonDNSError}
	}
	if tlsErrDiag(err, msg) {
		return failureDiag{Stage: FailureStageTLS, Reason: FailureReasonTLSError}
	}
	// Fixed client phrasing for headers-stage timeouts (both HTTP/1 and the
	// HTTP/2 "awaiting response headers" statement): category evidence only,
	// the output stays the enum.
	if strings.Contains(msg, "awaiting response headers") {
		return failureDiag{Stage: FailureStageResponseHeaders, Reason: FailureReasonResponseHeaderTimeout}
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		if isRefusedMsg(msg) || errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return failureDiag{Stage: FailureStageConnect, Reason: FailureReasonConnectRefused}
		}
		var tErr timeoutError
		if errors.As(err, &tErr) && tErr.Timeout() {
			return failureDiag{Stage: FailureStageConnect, Reason: FailureReasonConnectTimeout}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return failureDiag{Stage: FailureStageConnect, Reason: FailureReasonConnectTimeout}
		}
		return failureDiag{Stage: FailureStageConnect, Reason: FailureReasonTransportError}
	}
	if isRefusedMsg(msg) {
		return failureDiag{Stage: FailureStageConnect, Reason: FailureReasonConnectRefused}
	}
	if isResetMsg(msg) {
		return failureDiag{Stage: FailureStageRequest, Reason: FailureReasonConnectionReset}
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(msg, "unexpected eof") {
		stage := FailureStageRequest
		if stageHint == FailureStageStreamStartup {
			stage = FailureStageStreamStartup
		}
		return failureDiag{Stage: stage, Reason: FailureReasonUnexpectedEOF}
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Timeout() {
		return failureDiag{Stage: FailureStageRequest, Reason: FailureReasonTransportTimeout}
	}
	var tErr timeoutError
	if errors.As(err, &tErr) && tErr.Timeout() {
		return failureDiag{Stage: FailureStageRequest, Reason: FailureReasonTransportTimeout}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return failureDiag{Stage: FailureStageRequest, Reason: FailureReasonTransportTimeout}
	}
	lower := msg
	if strings.Contains(lower, "timeout") || strings.Contains(lower, "timed out") ||
		strings.Contains(lower, "deadline exceeded") || strings.Contains(lower, "client.timeout exceeded") {
		return failureDiag{Stage: FailureStageRequest, Reason: FailureReasonTransportTimeout}
	}
	if strings.Contains(lower, "custom fallback transport failed") {
		return failureDiag{Stage: FailureStageRequest, Reason: FailureReasonTransportError}
	}
	return failureDiag{Stage: FailureStageUnknown, Reason: FailureReasonUnknown}
}

func tlsErrDiag(err error, lowerMsg string) bool {
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &certErr) {
		return true
	}
	if strings.Contains(lowerMsg, "tls handshake") || strings.Contains(lowerMsg, "tls:") {
		return true
	}
	if strings.Contains(lowerMsg, "certificate") && (strings.Contains(lowerMsg, "verify") ||
		strings.Contains(lowerMsg, "unknown authority") || strings.Contains(lowerMsg, "expired") ||
		strings.Contains(lowerMsg, "handshake")) {
		return true
	}
	return false
}

func isRefusedMsg(lower string) bool {
	return strings.Contains(lower, "connection refused")
}

// upstreamAttemptFailedInfoArgs builds the single safe INFO diagnostic for
// one failed upstream attempt. Whitelist fields only, taken from the already
// sanitized final UpstreamAttempt so monitor/history/log share the same
// stage/reason. Raw errors, bodies, headers, keys, and URLs never enter;
// Proxy is already redacted in the record. Success attempts never log.
func upstreamAttemptFailedInfoArgs(a UpstreamAttempt) []any {
	return []any{"component", "upstream", "event", "upstream_attempt_failed",
		"request_id", a.RequestID, "model", a.Model, "tier", a.Tier, "protocol", a.Protocol,
		"client_session_hash", a.ClientSessionHash, "key_id", a.KeyID, "channel", a.Channel, "anonymous", a.Anonymous,
		"attempt", a.Attempt, "proxy_pool", a.ProxyPool, "proxy_node", a.Proxy,
		"status", a.Status, "failure_class", a.FailureClass,
		"failure_stage", a.FailureStage, "failure_reason", a.FailureReason,
		"duration_ms", max(a.DurationMS, 0)}
}

func isResetMsg(lower string) bool {
	return strings.Contains(lower, "connection reset") || strings.Contains(lower, "broken pipe")
}
