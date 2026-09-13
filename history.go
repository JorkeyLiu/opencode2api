package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Persistent history: lightweight embedded NDJSON projection of redacted
// inference metadata. Standard library only. Failures never affect Gateway
// startup, Apply, healthz readiness, or inference; the store degrades to a
// memory-only disabled status.

const (
	historyQueueSize    = 4096
	historySegmentBytes = 32 << 20
	historyFlushBatch   = 512
	historyFlushEvery   = 2 * time.Second
	historyFileMode     = 0600
	historyDirMode      = 0700
	historySchemaV      = 1
)

var historySegmentMaxBytes = int64(historySegmentBytes)

// Fixed operator-visible history error taxonomy. Status/LastError must only
// ever contain one of these values (or empty); raw paths and error strings
// stay in internal logs via the redactor and never reach the admin API.
const (
	historyErrUnavailable = "history unavailable"
	historyErrWriteFailed = "history write failed"
	historyErrReadFailed  = "history read failed"
	historyErrCorrupt     = "history segment corrupt"
)

func sanitizeHistoryLastError(v string) string {
	switch v {
	case "", historyErrUnavailable, historyErrWriteFailed, historyErrReadFailed, historyErrCorrupt:
		return v
	default:
		return historyErrUnavailable
	}
}

// historyScanHook is a test-only injection point called once per scanned
// line (and once per segment). Tests may set it to sleep to force a query
// context deadline without waiting for real IO.
var historyScanHook func()

// historyRecordKind identifies the NDJSON line schema.
type historyRecordKind string

const (
	historyKindRequest historyRecordKind = "request"
	historyKindAttempt historyRecordKind = "attempt"
	historyKindMinute  historyRecordKind = "minute"
)

// historyEnvelope is the per-line NDJSON schema.
type historyEnvelope struct {
	V    int    `json:"v"`
	Kind string `json:"kind"`
	Time string `json:"time"`
}

// historyRequestLine carries the safe subset of UpstreamRequest.
type historyRequestLine struct {
	V          int    `json:"v"`
	Kind       string `json:"kind"`
	Time       string `json:"time"`
	RequestID  string `json:"request_id"`
	Model      string `json:"model"`
	Tier       string `json:"tier,omitempty"`
	KeyID      string `json:"key_id,omitempty"`
	Channel    string `json:"channel"`
	Anonymous  bool   `json:"anonymous"`
	ProxyPool  string `json:"proxy_pool,omitempty"`
	ProxyNode  string `json:"proxy_node,omitempty"`
	Attempts   int    `json:"attempts"`
	Status     int    `json:"status"`
	DurationMS int64  `json:"duration_ms"`
	Success    bool   `json:"success"`
	Outcome    string `json:"outcome,omitempty"`
}

// historyAttemptLine carries the safe subset of UpstreamAttempt.
type historyAttemptLine struct {
	V            int    `json:"v"`
	Kind         string `json:"kind"`
	Time         string `json:"time"`
	RequestID    string `json:"request_id"`
	Model        string `json:"model"`
	Tier         string `json:"tier,omitempty"`
	Attempt      int    `json:"attempt"`
	KeyID        string `json:"key_id,omitempty"`
	Channel      string `json:"channel"`
	Anonymous    bool   `json:"anonymous"`
	ProxyPool    string `json:"proxy_pool,omitempty"`
	ProxyNode    string `json:"proxy_node,omitempty"`
	Status       int    `json:"status,omitempty"`
	DurationMS   int64  `json:"duration_ms"`
	Success      bool   `json:"success"`
	Outcome      string `json:"outcome,omitempty"`
	FailureClass string `json:"failure_class,omitempty"`
	Retryable    bool   `json:"retryable"`
	CoolsDown    bool   `json:"cools_down"`
}

// historyMinuteLine carries MetricSeries numeric fields only.
type historyMinuteLine struct {
	V               int    `json:"v"`
	Kind            string `json:"kind"`
	Time            string `json:"time"`
	Minute          string `json:"minute"`
	Total           uint64 `json:"total"`
	Success         uint64 `json:"success"`
	Errors          uint64 `json:"errors"`
	InputTokens     uint64 `json:"input_tokens"`
	OutputTokens    uint64 `json:"output_tokens"`
	CachedTokens    uint64 `json:"cached_tokens"`
	ReasoningTokens uint64 `json:"reasoning_tokens"`
	TotalTokens     uint64 `json:"total_tokens"`
	UsageReported   uint64 `json:"usage_reported"`
}

// HistoryStatus is the operator-visible runtime state (no absolute path,
// no secrets; last_error is redacted).
type HistoryStatus struct {
	EnabledConfig bool   `json:"enabled_config"`
	Active        bool   `json:"active"`
	Directory     string `json:"directory"`
	RetentionDays int    `json:"retention_days"`
	MaxBytesMB    int    `json:"max_bytes_mb"`
	Dropped       uint64 `json:"dropped"`
	Gap           bool   `json:"gap"`
	LastError     string `json:"last_error,omitempty"`
	OldestAt      string `json:"oldest_at,omitempty"`
	NewestAt      string `json:"newest_at,omitempty"`
	SizeBytes     uint64 `json:"size_bytes"`
}

type historyQueued struct {
	kind historyRecordKind
	line []byte
}

type historyOpenSegment struct {
	kind historyRecordKind
	date string // YYYYMMDD UTC
	seq  int
	path string
	file *os.File
	buf  *bufio.Writer
	size int64
	// firstAt/lastAt track record time bounds for the open segment,
	// including buffered lines not yet flushed to disk. They let
	// refreshStatsLocked merge open-segment boundaries without
	// rescanning open files (which would miss buffered tails).
	firstAt string
	lastAt  string
}

// HistoryStore owns async durable history. Enqueue methods are non-blocking
// and never perform disk IO. A single writer goroutine owns all files.
//
// Hot-path contract: EnqueueRequest/EnqueueAttempt/EnqueueMinute must never
// acquire mu (writer open/rotation/retention/stats lock). They only do
// atomic loads (closed/active), a non-blocking channel select, and atomic
// counters. Active/closed readiness is atomic; queue is never closed;
// Close is idempotent; Apply sink swap never sends on a closed channel.
type HistoryStore struct {
	dir           string
	displayDir    string
	cfg           HistoryConfig
	configPath    string
	logger        *slog.Logger
	redactor      *SecretRedactor
	queue         chan historyQueued
	done          chan struct{}
	closed        atomic.Bool
	active        atomic.Bool
	dropped       atomic.Uint64
	gap           atomic.Bool
	lastWarn      atomic.Int64 // unix nano of last drop warn
	mu            sync.Mutex   // guards lastError, stats, open segments, provider; NEVER taken on enqueue hot path
	enabledConfig bool
	lastError     string
	sizeBytes     uint64
	oldestAt      string
	newestAt      string
	open          map[historyRecordKind]*historyOpenSegment
	provider      func(minute time.Time) (MetricSeries, bool)
	minuteMu      sync.Mutex // guards lastMinute only; separate from writer mu
	lastMinute    string     // newest minute already persisted (YYYY-MM-DDTHH:MMZ)
	retentionDays int
	maxBytes      int64
	wg            sync.WaitGroup
}

func historyEqual(a, b HistoryConfig) bool {
	return a.Enabled == b.Enabled &&
		strings.TrimSpace(a.Directory) == strings.TrimSpace(b.Directory) &&
		a.RetentionDays == b.RetentionDays &&
		a.MaxBytesMB == b.MaxBytesMB
}

// OpenHistoryStore builds a store for cfg. On any init/write failure it
// returns a memory-only disabled store (active=false) with lastError set;
// the caller must continue startup/Apply regardless.
func OpenHistoryStore(configPath string, cfg HistoryConfig, logger *slog.Logger, redactor *SecretRedactor) *HistoryStore {
	s := &HistoryStore{
		cfg:           cfg,
		configPath:    configPath,
		logger:        logger,
		redactor:      redactor,
		queue:         make(chan historyQueued, historyQueueSize),
		done:          make(chan struct{}),
		open:          make(map[historyRecordKind]*historyOpenSegment),
		enabledConfig: cfg.Enabled,
		retentionDays: cfg.RetentionDays,
		maxBytes:      int64(cfg.MaxBytesMB) << 20,
	}
	if cfg.RetentionDays < 1 {
		s.retentionDays = 7
	}
	if cfg.MaxBytesMB < 1 {
		s.maxBytes = 128 << 20
	}
	s.displayDir = "history"
	if trimmed := strings.TrimSpace(cfg.Directory); trimmed != "" {
		cleaned := filepath.Clean(trimmed)
		s.displayDir = filepath.Base(cleaned)
		if s.displayDir == "" || s.displayDir == "." || s.displayDir == "/" {
			s.displayDir = "history"
		}
	}
	if !cfg.Enabled {
		return s
	}
	s.dir = ResolveHistoryDir(configPath, cfg.Directory)
	if err := os.MkdirAll(s.dir, historyDirMode); err != nil {
		s.setError(historyErrUnavailable)
		if logger != nil {
			logger.Warn("history disabled", "component", "history", "event", "history_disabled", "error", s.redact(err.Error()))
		}
		return s
	}
	_ = os.Chmod(s.dir, historyDirMode)
	// Probe writability without keeping state.
	probe, err := os.OpenFile(filepath.Join(s.dir, ".writetest.tmp"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, historyFileMode)
	if err != nil {
		s.setError(historyErrUnavailable)
		if logger != nil {
			logger.Warn("history disabled", "component", "history", "event", "history_disabled", "error", s.redact(err.Error()))
		}
		return s
	}
	_ = probe.Close()
	_ = os.Remove(filepath.Join(s.dir, ".writetest.tmp"))
	s.active.Store(true)
	s.scanStatsLocked()
	s.minuteMu.Lock()
	s.lastMinute = s.findNewestMinute()
	s.minuteMu.Unlock()
	s.wg.Add(1)
	go s.run()
	s.enforceRetention()
	return s
}

func (s *HistoryStore) redact(msg string) string {
	if s == nil || s.redactor == nil {
		return msg
	}
	return s.redactor.String(msg)
}

func (s *HistoryStore) setError(msg string) {
	s.active.Store(false)
	s.mu.Lock()
	s.lastError = msg
	s.mu.Unlock()
}

func (s *HistoryStore) setErrorErr(err error) {
	if s == nil {
		return
	}
	if s.logger != nil && err != nil {
		s.logger.Warn("history error", "component", "history", "event", "history_error", "error", s.redact(err.Error()))
	}
	s.setError(historyErrUnavailable)
}

// SetMinuteProvider installs the per-minute snapshot source (Monitor).
func (s *HistoryStore) SetMinuteProvider(fn func(minute time.Time) (MetricSeries, bool)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.provider = fn
	s.mu.Unlock()
}

func (s *HistoryStore) getProvider() func(minute time.Time) (MetricSeries, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.provider
}

// Active reports whether the store is persisting to disk (atomic, no mu).
func (s *HistoryStore) Active() bool {
	if s == nil {
		return false
	}
	return s.active.Load()
}

// Status returns the operator-visible history state.
func (s *HistoryStore) Status() HistoryStatus {
	if s == nil {
		return HistoryStatus{}
	}
	active := s.active.Load()
	s.mu.Lock()
	defer s.mu.Unlock()
	return HistoryStatus{
		EnabledConfig: s.enabledConfig, Active: active, Directory: s.displayDir,
		RetentionDays: s.retentionDays, MaxBytesMB: int(s.maxBytes >> 20),
		Dropped: s.dropped.Load(), Gap: s.gap.Load(), LastError: sanitizeHistoryLastError(s.lastError),
		OldestAt: s.oldestAt, NewestAt: s.newestAt, SizeBytes: s.sizeBytes,
	}
}

// DroppedCount exposes the atomic drop counter for query responses.
func (s *HistoryStore) DroppedCount() uint64 {
	if s == nil {
		return 0
	}
	return s.dropped.Load()
}

// GapFlag reports whether drops or corrupt lines were observed.
func (s *HistoryStore) GapFlag() bool {
	if s == nil {
		return false
	}
	return s.gap.Load()
}

// Dir returns the recognized history directory (empty when disabled).
// Lock-free: dir is immutable after Open; active is atomic.
func (s *HistoryStore) Dir() string {
	if s == nil {
		return ""
	}
	if !s.active.Load() {
		return ""
	}
	return s.dir
}

func (s *HistoryStore) enqueue(kind historyRecordKind, line []byte) {
	if s == nil || s.closed.Load() {
		return
	}
	if !s.active.Load() {
		return
	}
	select {
	case s.queue <- historyQueued{kind: kind, line: line}:
	default:
		// Drop newest to keep enqueued order simple and bounded.
		s.dropped.Add(1)
		s.gap.Store(true)
		s.warnDroppedThrottled()
	}
}

func (s *HistoryStore) warnDroppedThrottled() {
	now := time.Now().UnixNano()
	last := s.lastWarn.Load()
	if now-last < int64(time.Minute) {
		return
	}
	if s.lastWarn.CompareAndSwap(last, now) && s.logger != nil {
		s.logger.Warn("history records dropped", "component", "history", "event", "history_records_dropped", "dropped", s.dropped.Load())
	}
}

// historyKeySuffix enforces the display-only key contract at the store
// boundary: only the last 5 characters (or the anonymous literal) persist.
func historyKeySuffix(keyID string, anonymous bool) string {
	if anonymous || keyID == anonymousCredentialID {
		return anonymousCredentialID
	}
	runes := []rune(keyID)
	if len(runes) <= 5 {
		return keyID
	}
	return string(runes[len(runes)-5:])
}

// EnqueueRequest clones the safe subset of a completed request.
func (s *HistoryStore) EnqueueRequest(r UpstreamRequest) {
	if s == nil {
		return
	}
	line := historyRequestLine{
		V: historySchemaV, Kind: string(historyKindRequest), Time: r.Time.UTC().Format(time.RFC3339Nano),
		RequestID: r.RequestID, Model: r.Model, Tier: r.Tier,
		KeyID: historyKeySuffix(r.KeyID, r.Anonymous), Channel: r.Channel, Anonymous: r.Anonymous,
		ProxyPool: r.ProxyPool, ProxyNode: redactURL(r.Proxy),
		Attempts: r.Attempts, Status: r.Status, DurationMS: max(r.DurationMS, 0),
		Success: r.Success, Outcome: r.Outcome,
	}
	if line.Channel == "" {
		line.Channel = "not_routed"
	}
	if line.Anonymous {
		line.KeyID = anonymousCredentialID
	}
	data, err := json.Marshal(line)
	if err != nil {
		return
	}
	data = append(data, '\n')
	s.enqueue(historyKindRequest, data)
}

// EnqueueAttempt clones the safe subset of one upstream attempt.
func (s *HistoryStore) EnqueueAttempt(a UpstreamAttempt) {
	if s == nil {
		return
	}
	line := historyAttemptLine{
		V: historySchemaV, Kind: string(historyKindAttempt), Time: a.Time.UTC().Format(time.RFC3339Nano),
		RequestID: a.RequestID, Model: a.Model, Tier: a.Tier, Attempt: a.Attempt,
		KeyID: historyKeySuffix(a.KeyID, a.Anonymous), Channel: a.Channel, Anonymous: a.Anonymous,
		ProxyPool: a.ProxyPool, ProxyNode: redactURL(a.Proxy),
		Status: a.Status, DurationMS: max(a.DurationMS, 0),
		Success: a.Success, Outcome: a.Outcome,
		FailureClass: a.FailureClass, Retryable: a.Retryable, CoolsDown: a.CoolsDown,
	}
	if line.Anonymous {
		line.KeyID = anonymousCredentialID
	}
	data, err := json.Marshal(line)
	if err != nil {
		return
	}
	data = append(data, '\n')
	s.enqueue(historyKindAttempt, data)
}

// EnqueueMinute persists one complete minute of MetricSeries totals.
// Lock-free w.r.t. writer mu: lastMinute is guarded by minuteMu only.
func (s *HistoryStore) EnqueueMinute(series MetricSeries) {
	if s == nil {
		return
	}
	minute := series.Minute.UTC().Truncate(time.Minute)
	line := historyMinuteLine{
		V: historySchemaV, Kind: string(historyKindMinute), Time: minute.Format(time.RFC3339Nano),
		Minute: minute.Format(time.RFC3339Nano),
		Total:  series.Total, Success: series.Success, Errors: series.Errors,
		InputTokens: series.InputTokens, OutputTokens: series.OutputTokens,
		CachedTokens: series.CachedTokens, ReasoningTokens: series.ReasoningTokens,
		TotalTokens: series.TotalTokens, UsageReported: series.UsageReported,
	}
	data, err := json.Marshal(line)
	if err != nil {
		return
	}
	data = append(data, '\n')
	s.enqueue(historyKindMinute, data)
	s.minuteMu.Lock()
	if s.lastMinute == "" || line.Minute > s.lastMinute {
		s.lastMinute = line.Minute
	}
	s.minuteMu.Unlock()
}

// getLastMinute returns the newest persisted minute key (minuteMu only).
func (s *HistoryStore) getLastMinute() string {
	if s == nil {
		return ""
	}
	s.minuteMu.Lock()
	defer s.minuteMu.Unlock()
	return s.lastMinute
}

func (s *HistoryStore) run() {
	defer s.wg.Done()
	flushTicker := time.NewTicker(historyFlushEvery)
	defer flushTicker.Stop()
	minuteTicker := time.NewTicker(15 * time.Second)
	defer minuteTicker.Stop()
	batch := 0
	flush := func() {
		s.mu.Lock()
		for _, seg := range s.open {
			if seg != nil && seg.buf != nil {
				_ = seg.buf.Flush()
			}
		}
		s.mu.Unlock()
		batch = 0
	}
	for {
		select {
		case <-s.done:
			s.drainQueue()
			flush()
			s.syncAndClose()
			return
		case item, ok := <-s.queue:
			if !ok {
				flush()
				s.syncAndClose()
				return
			}
			s.writeLine(item.kind, item.line)
			batch++
			if batch >= historyFlushBatch {
				flush()
			}
		case <-flushTicker.C:
			flush()
		case <-minuteTicker.C:
			s.maybeWriteMinute()
		}
	}
}

func (s *HistoryStore) maybeWriteMinute() {
	provider := s.getProvider()
	if provider == nil {
		return
	}
	prev := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	key := prev.Format(time.RFC3339Nano)
	if last := s.getLastMinute(); last != "" && key <= last {
		return
	}
	series, ok := provider(prev)
	if !ok {
		return
	}
	s.EnqueueMinute(series)
}

func (s *HistoryStore) drainQueue() {
	for {
		select {
		case item := <-s.queue:
			s.writeLine(item.kind, item.line)
		default:
			return
		}
	}
}

func (s *HistoryStore) writeLine(kind historyRecordKind, line []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active.Load() {
		return
	}
	seg, err := s.openSegmentLocked(kind)
	if err != nil {
		if s.lastError == "" {
			s.lastError = historyErrWriteFailed
		}
		s.active.Store(false)
		if s.logger != nil {
			s.logger.Warn("history disabled", "component", "history", "event", "history_disabled", "error", s.redact(err.Error()))
		}
		return
	}
	if seg.size+int64(len(line)) > historySegmentMaxBytes && seg.size > 0 {
		s.rotateSegmentLocked(kind)
		var err error
		seg, err = s.openSegmentLocked(kind)
		if err != nil {
			if s.lastError == "" {
				s.lastError = historyErrWriteFailed
			}
			s.active.Store(false)
			if s.logger != nil {
				s.logger.Warn("history disabled", "component", "history", "event", "history_disabled", "error", s.redact(err.Error()))
			}
			return
		}
	}
	n, err := seg.buf.Write(line)
	if err != nil {
		if s.lastError == "" {
			s.lastError = historyErrWriteFailed
		}
		s.active.Store(false)
		if s.logger != nil {
			s.logger.Warn("history disabled", "component", "history", "event", "history_disabled", "error", s.redact(err.Error()))
		}
		return
	}
	seg.size += int64(n)
	if t, err := parseHistoryTime(line); err == nil {
		formatted := t.UTC().Format(time.RFC3339Nano)
		if seg.firstAt == "" {
			seg.firstAt = formatted
		}
		seg.lastAt = formatted
		if s.oldestAt == "" {
			s.oldestAt = formatted
		}
		s.newestAt = formatted
	}
	s.sizeBytes += uint64(n)
}

func parseHistoryTime(line []byte) (time.Time, error) {
	var env historyEnvelope
	if err := json.Unmarshal(line, &env); err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, env.Time)
}

func (s *HistoryStore) openSegmentLocked(kind historyRecordKind) (*historyOpenSegment, error) {
	today := time.Now().UTC().Format("20060102")
	if seg := s.open[kind]; seg != nil && seg.date == today && seg.file != nil {
		return seg, nil
	}
	if seg := s.open[kind]; seg != nil && seg.file != nil {
		_ = seg.buf.Flush()
		_ = seg.file.Sync()
		_ = seg.file.Close()
		s.open[kind] = nil
		s.enforceRetentionLocked()
		today = time.Now().UTC().Format("20060102")
	}
	seq := s.nextSeqLocked(kind, today)
	name := fmt.Sprintf("%ss-%s-%03d.ndjson", kind, today, seq)
	path := filepath.Join(s.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, historyFileMode)
	if err != nil {
		return nil, err
	}
	_ = f.Chmod(historyFileMode)
	var size int64
	if info, err := f.Stat(); err == nil {
		size = info.Size()
	}
	seg := &historyOpenSegment{kind: kind, date: today, seq: seq, path: path, file: f, buf: bufio.NewWriterSize(f, 64*1024), size: size}
	if size > 0 {
		// New seq names are normally empty, but if the path already held
		// data, seed bounds from disk so refresh keeps oldest/newest.
		if first, last := scanSegmentBounds(path); first != "" || last != "" {
			seg.firstAt = first
			seg.lastAt = last
		}
	}
	s.open[kind] = seg
	return seg, nil
}

func (s *HistoryStore) rotateSegmentLocked(kind historyRecordKind) {
	if seg := s.open[kind]; seg != nil && seg.file != nil {
		_ = seg.buf.Flush()
		_ = seg.file.Sync()
		_ = seg.file.Close()
		s.open[kind] = nil
	}
	s.enforceRetentionLocked()
}

func (s *HistoryStore) nextSeqLocked(kind historyRecordKind, date string) int {
	prefix := fmt.Sprintf("%ss-%s-", kind, date)
	maxSeq := 0
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 1
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".ndjson") {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".ndjson")
		if n, err := strconv.Atoi(mid); err == nil && n > maxSeq {
			maxSeq = n
		}
	}
	return maxSeq + 1
}

// Hot Apply keeps a short close budget so config Apply never blocks on
// disk; process Shutdown uses the longer shutdown budget.
const (
	historyApplyCloseTimeout    = 2 * time.Second
	historyShutdownCloseTimeout = 10 * time.Second
)

// historyCloseHook is a test-only injection point run at the start of
// syncAndClose. Tests may set it to sleep to force a CloseWithTimeout
// deadline without waiting for real IO.
var historyCloseHook func()

func (s *HistoryStore) syncAndClose() {
	if historyCloseHook != nil {
		historyCloseHook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for kind, seg := range s.open {
		if seg != nil && seg.file != nil {
			_ = seg.buf.Flush()
			_ = seg.file.Sync()
			_ = seg.file.Close()
		}
		s.open[kind] = nil
	}
}

// Close drains with the hot-Apply budget (~2s), flushes, and syncs. It
// never panics on concurrent Enqueue; late records after Close are
// dropped. Queue is never closed; idempotent.
func (s *HistoryStore) Close() {
	_ = s.CloseWithTimeout(historyApplyCloseTimeout)
}

// CloseWithTimeout drains, flushes, and syncs with an explicit budget. It
// reports true when the writer finished within timeout and false on
// timeout. On timeout it logs one history_shutdown_timeout warn and
// returns; the writer goroutine continues drain+sync in the background
// (best-effort, never panics). Idempotent: concurrent or repeated calls
// wait on the same writer without closing channels twice.
func (s *HistoryStore) CloseWithTimeout(timeout time.Duration) bool {
	if s == nil {
		return true
	}
	if !s.closed.CompareAndSwap(false, true) {
		if !s.active.Load() {
			return true
		}
		return s.waitWriter(timeout)
	}
	if !s.active.Load() {
		return true
	}
	select {
	case <-s.done:
		return true
	default:
		close(s.done)
	}
	ok := s.waitWriter(timeout)
	if !ok && s.logger != nil {
		s.logger.Warn("history close timed out; writer continues in background", "component", "history", "event", "history_shutdown_timeout")
	}
	return ok
}

func (s *HistoryStore) waitWriter(timeout time.Duration) bool {
	finished := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return true
	case <-time.After(timeout):
		return false
	}
}

// enforceRetention removes oldest sealed segments beyond retention or bytes.
func (s *HistoryStore) enforceRetention() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enforceRetentionLocked()
}

func (s *HistoryStore) enforceRetentionLocked() {
	if s.dir == "" {
		return
	}
	openPaths := map[string]bool{}
	for _, seg := range s.open {
		if seg != nil && seg.path != "" {
			openPaths[seg.path] = true
		}
	}
	type entry struct {
		path string
		date string
		size int64
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	var segs []entry
	var total int64
	for _, e := range entries {
		name := e.Name()
		if !isHistorySegment(name) {
			continue
		}
		full := filepath.Join(s.dir, name)
		if openPaths[full] {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		date := segmentDate(name)
		segs = append(segs, entry{path: full, date: date, size: info.Size()})
		total += info.Size()
	}
	// Include open segment sizes in the byte budget.
	for _, seg := range s.open {
		if seg != nil {
			total += seg.size
		}
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -s.retentionDays).Format("20060102")
	sort.Slice(segs, func(i, j int) bool {
		if segs[i].date != segs[j].date {
			return segs[i].date < segs[j].date
		}
		return segs[i].path < segs[j].path
	})
	for _, g := range segs {
		tooOld := g.date != "" && g.date < cutoff
		tooBig := total > s.maxBytes
		if !tooOld && !tooBig {
			break
		}
		if err := os.Remove(g.path); err == nil {
			total -= g.size
		}
	}
	s.refreshStatsLocked()
}

func isHistorySegment(name string) bool {
	for _, kind := range []string{"requests", "attempts", "minutes"} {
		if strings.HasPrefix(name, kind+"-") && strings.HasSuffix(name, ".ndjson") {
			return true
		}
	}
	return false
}

func segmentDate(name string) string {
	parts := strings.Split(name, "-")
	if len(parts) < 2 {
		return ""
	}
	if len(parts[1]) >= 8 {
		return parts[1][:8]
	}
	return ""
}

func segmentKind(name string) historyRecordKind {
	switch {
	case strings.HasPrefix(name, "requests-"):
		return historyKindRequest
	case strings.HasPrefix(name, "attempts-"):
		return historyKindAttempt
	case strings.HasPrefix(name, "minutes-"):
		return historyKindMinute
	}
	return ""
}

func (s *HistoryStore) scanStatsLocked() {
	if s.dir == "" {
		return
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	var total uint64
	var oldest, newest string
	for _, e := range entries {
		if !isHistorySegment(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += uint64(info.Size())
		full := filepath.Join(s.dir, e.Name())
		first, last := scanSegmentBounds(full)
		if first != "" && (oldest == "" || first < oldest) {
			oldest = first
		}
		if last != "" && (newest == "" || last > newest) {
			newest = last
		}
	}
	s.sizeBytes = total
	if oldest != "" {
		s.oldestAt = oldest
	}
	if newest != "" {
		s.newestAt = newest
	}
}

func (s *HistoryStore) refreshStatsLocked() {
	s.sizeBytes = 0
	s.oldestAt = ""
	s.newestAt = ""
	openPaths := map[string]bool{}
	for _, seg := range s.open {
		if seg != nil && seg.path != "" {
			openPaths[seg.path] = true
		}
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !isHistorySegment(e.Name()) {
			continue
		}
		full := filepath.Join(s.dir, e.Name())
		if openPaths[full] {
			// Open segments contribute size/bounds via in-memory seg
			// state below; counting FileInfo here would double-count
			// after flush when seg.size == on-disk size.
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		s.sizeBytes += uint64(info.Size())
		first, last := scanSegmentBounds(full)
		if first != "" && (s.oldestAt == "" || first < s.oldestAt) {
			s.oldestAt = first
		}
		if last != "" && (s.newestAt == "" || last > s.newestAt) {
			s.newestAt = last
		}
	}
	for _, seg := range s.open {
		if seg == nil {
			continue
		}
		s.sizeBytes += uint64(seg.size)
		first, last := seg.firstAt, seg.lastAt
		if first == "" && last == "" && seg.size > 0 && seg.path != "" {
			// Best-effort fallback for an open segment whose in-memory
			// bounds are empty (e.g. reopened pre-existing file where
			// the open-time scan found nothing).
			first, last = scanSegmentBounds(seg.path)
		}
		if first != "" && (s.oldestAt == "" || first < s.oldestAt) {
			s.oldestAt = first
		}
		if last != "" && (s.newestAt == "" || last > s.newestAt) {
			s.newestAt = last
		}
	}
	if s.newestAt == "" {
		s.newestAt = s.oldestAt
	}
}

func scanSegmentBounds(path string) (string, string) {
	f, err := os.Open(path)
	if err != nil {
		return "", ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var first, last string
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var env historyEnvelope
		if err := json.Unmarshal(line, &env); err != nil || env.Time == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339Nano, env.Time); err != nil {
			continue
		}
		if first == "" {
			first = env.Time
		}
		last = env.Time
	}
	return first, last
}

func (s *HistoryStore) findNewestMinute() string {
	if s.dir == "" {
		return ""
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return ""
	}
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "minutes-") && strings.HasSuffix(e.Name(), ".ndjson") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	newest := ""
	for _, name := range names {
		full := filepath.Join(s.dir, name)
		_, last := scanSegmentBounds(full)
		if last != "" && last > newest {
			newest = last
		}
	}
	// Normalize to minute key format used by maybeWriteMinute.
	if newest != "" {
		if t, err := time.Parse(time.RFC3339Nano, newest); err == nil {
			return t.UTC().Truncate(time.Minute).Format(time.RFC3339Nano)
		}
	}
	return newest
}

// --- Query model ---

type historyQueryFilter struct {
	From       time.Time
	To         time.Time
	Limit      int
	Cursor     string // opaque base64(time|id)
	Model      string
	Tier       string
	Channel    string
	Success    *bool
	ProxyPool  string
	RequestID  string
	FailureCls string
}

type historyPage struct {
	Items      []any  `json:"items"`
	NextCursor string `json:"next_cursor"`
	Gap        bool   `json:"gap"`
	Dropped    uint64 `json:"dropped"`
	Active     bool   `json:"active"`
	LastError  string `json:"last_error,omitempty"`
}

func decodeHistoryCursor(cursor string) (time.Time, string) {
	if cursor == "" {
		return time.Time{}, ""
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, ""
	}
	parts := strings.SplitN(string(raw), "|", 2)
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, ""
	}
	id := ""
	if len(parts) == 2 {
		id = parts[1]
	}
	return t, id
}

func encodeHistoryCursor(t time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + id))
}

// decodeHistoryCursorStrict validates the opaque cursor shape per kind.
// Empty cursor means "first page". Any decode/shape/time failure returns
// errBadCursor so handlers answer 400 invalid_cursor (never 200 empty).
// Requests carry time+request_id, attempts carry time+request_id+attempt,
// series carry minute/time (minute string must parse as RFC3339Nano).
func decodeHistoryCursorStrict(kind historyRecordKind, cursor string) (time.Time, string, int, error) {
	if cursor == "" {
		return time.Time{}, "", 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", 0, errBadCursor
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || parts[1] == "" {
		return time.Time{}, "", 0, errBadCursor
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", 0, errBadCursor
	}
	id := parts[1]
	switch kind {
	case historyKindRequest:
		if strings.Contains(id, "#") || strings.Contains(id, "|") {
			return time.Time{}, "", 0, errBadCursor
		}
		return t, id, 0, nil
	case historyKindAttempt:
		idx := strings.LastIndex(id, "#")
		if idx <= 0 || idx+1 >= len(id) {
			return time.Time{}, "", 0, errBadCursor
		}
		rid := id[:idx]
		if rid == "" || strings.Contains(rid, "|") {
			return time.Time{}, "", 0, errBadCursor
		}
		n, err := strconv.Atoi(id[idx+1:])
		if err != nil || n < 0 {
			return time.Time{}, "", 0, errBadCursor
		}
		return t, rid, n, nil
	default:
		if _, err := time.Parse(time.RFC3339Nano, id); err != nil {
			if _, err2 := time.Parse(time.RFC3339, id); err2 != nil {
				return time.Time{}, "", 0, errBadCursor
			}
		}
		return t, id, 0, nil
	}
}

func historyCheckCtx(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			return err
		}
		return context.Canceled
	default:
		return nil
	}
}

func historyRunHook() {
	if historyScanHook != nil {
		historyScanHook()
	}
}

func joinHistoryPath(dir, name string) string { return filepath.Join(dir, name) }

// historyReadDir returns the directory to scan even when the store is
// inactive (retains recognized on-disk data for queries that already
// passed the active gate). Lock-free: dir is immutable after Open.
func (s *HistoryStore) historyReadDir() string {
	if s == nil {
		return ""
	}
	return s.dir
}

// historySegmentNamesFor lists recognized segments for kind overlapping
// [from,to], newest-first by filename.
func historySegmentNamesFor(dir string, kind historyRecordKind, from, to time.Time) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	prefix := string(kind) + "s-"
	fromDate := from.UTC().Format("20060102")
	toDate := to.UTC().AddDate(0, 0, 1).Format("20060102")
	var names []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".ndjson") {
			continue
		}
		if date := segmentDate(name); date != "" {
			if date < fromDate || date > toDate {
				continue
			}
		}
		names = append(names, name)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	return names
}

// readHistorySegmentNewestFirst reads one segment fully (32MB segment cap
// makes this bounded) and returns raw lines newest-first. Corrupt/scanner
// failures set gap=true and continue; ctx cancellation returns ctx.Err().
// Unreadable files report gap and no error so scanning continues.
func readHistorySegmentNewestFirst(ctx context.Context, path string) ([][]byte, bool, error) {
	if err := historyCheckCtx(ctx); err != nil {
		return nil, false, err
	}
	historyRunHook()
	f, err := os.Open(path)
	if err != nil {
		return nil, true, nil
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var fwd [][]byte
	for sc.Scan() {
		if err := historyCheckCtx(ctx); err != nil {
			return nil, false, err
		}
		historyRunHook()
		chunk := sc.Bytes()
		line := append([]byte(nil), chunk...)
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		fwd = append(fwd, line)
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, false, err
		}
		// Scanner errors (e.g. oversized line) mark gap but keep what we have.
		if ctxErr := historyCheckCtx(ctx); ctxErr != nil {
			return nil, false, ctxErr
		}
		// Reverse what was collected before reporting gap.
		for i, j := 0, len(fwd)-1; i < j; i, j = i+1, j-1 {
			fwd[i], fwd[j] = fwd[j], fwd[i]
		}
		return fwd, true, nil
	}
	for i, j := 0, len(fwd)-1; i < j; i, j = i+1, j-1 {
		fwd[i], fwd[j] = fwd[j], fwd[i]
	}
	return fwd, false, nil
}

func (s *HistoryStore) queryEnabled() (string, bool) {
	if s == nil {
		return "", false
	}
	if !s.enabledConfig {
		return "", false
	}
	return s.dir, true
}

// scanKindLines scans recognized segments for kind within [from,to],
// newest-first by segment and newest-first within each segment (full file
// read then reversed; 32MB segment cap keeps this bounded). It returns all
// time-matching raw lines newest-first with no limit/filter/cursor
// truncation; callers apply filter/cursor/limit. Corrupt lines and
// unreadable segments set gap=true and continue.
func (s *HistoryStore) scanKindLines(kind historyRecordKind, from, to time.Time, want int) ([][]byte, bool) {
	return s.scanKindLinesCtx(context.Background(), kind, from, to)
}

func (s *HistoryStore) scanKindLinesCtx(ctx context.Context, kind historyRecordKind, from, to time.Time) ([][]byte, bool) {
	_ = ctx
	dir := s.historyReadDir()
	if dir == "" {
		return nil, false
	}
	names := historySegmentNamesFor(dir, kind, from, to)
	var out [][]byte
	gap := false
	for _, name := range names {
		if err := historyCheckCtx(ctx); err != nil {
			break
		}
		lines, segGap, err := readHistorySegmentNewestFirst(ctx, filepath.Join(dir, name))
		if err != nil {
			break
		}
		if segGap {
			gap = true
		}
		for _, line := range lines {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var env historyEnvelope
			if err := json.Unmarshal(line, &env); err != nil || env.V != historySchemaV || env.Kind != string(kind) {
				gap = true
				continue
			}
			t, err := time.Parse(time.RFC3339Nano, env.Time)
			if err != nil {
				gap = true
				continue
			}
			if t.Before(from) || t.After(to) {
				continue
			}
			out = append(out, line)
		}
	}
	if s.GapFlag() {
		gap = true
	}
	return out, gap
}

func historyLineTimeID(kind historyRecordKind, line []byte) (time.Time, string) {
	switch kind {
	case historyKindRequest:
		var v historyRequestLine
		if err := json.Unmarshal(line, &v); err != nil {
			return time.Time{}, ""
		}
		t, _ := time.Parse(time.RFC3339Nano, v.Time)
		return t, v.RequestID
	case historyKindAttempt:
		var v historyAttemptLine
		if err := json.Unmarshal(line, &v); err != nil {
			return time.Time{}, ""
		}
		t, _ := time.Parse(time.RFC3339Nano, v.Time)
		return t, v.RequestID + "#" + strconv.Itoa(v.Attempt)
	default:
		var v historyMinuteLine
		if err := json.Unmarshal(line, &v); err != nil {
			return time.Time{}, ""
		}
		t, _ := time.Parse(time.RFC3339Nano, v.Minute)
		if t.IsZero() {
			t, _ = time.Parse(time.RFC3339Nano, v.Time)
		}
		return t, v.Minute
	}
}
