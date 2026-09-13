package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// History query API: authenticated GET, no-store, no CSRF, independent
// 30/min/IP rate limit, 5s handler timeout. Queries scan only the
// recognized history directory; no path input is accepted.

func (a *AdminServer) allowHistory(client string) bool {
	return a.allowWindow(&a.historyAttempts, client, 30, rateLimitWindowMinute)
}

func historyStoreForQuery(a *AdminServer) *HistoryStore {
	if a == nil || a.manager == nil {
		return nil
	}
	return a.manager.History()
}

func writeHistoryPage(w http.ResponseWriter, page historyPage) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, page)
}

func disabledHistoryPage(s *HistoryStore) historyPage {
	if s == nil {
		return historyPage{Items: []any{}, Gap: false, Dropped: 0, Active: false}
	}
	st := s.Status()
	return historyPage{
		Items: []any{}, Gap: st.Gap, Dropped: st.Dropped,
		Active: false, LastError: st.LastError,
	}
}

func parseHistoryRange(query map[string][]string, defFrom time.Duration, maxRange time.Duration) (time.Time, time.Time, error) {
	now := time.Now().UTC()
	to := now
	if vals, ok := query["to"]; ok && len(vals) > 0 && strings.TrimSpace(vals[0]) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(vals[0]))
		if err != nil {
			if parsed2, err2 := time.Parse(time.RFC3339Nano, strings.TrimSpace(vals[0])); err2 == nil {
				parsed = parsed2
				err = nil
			} else {
				return time.Time{}, time.Time{}, err
			}
		}
		to = parsed.UTC()
	}
	from := to.Add(-defFrom)
	if vals, ok := query["from"]; ok && len(vals) > 0 && strings.TrimSpace(vals[0]) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(vals[0]))
		if err != nil {
			if parsed2, err2 := time.Parse(time.RFC3339Nano, strings.TrimSpace(vals[0])); err2 == nil {
				parsed = parsed2
				err = nil
			} else {
				return time.Time{}, time.Time{}, err
			}
		}
		from = parsed.UTC()
	}
	if from.After(to) {
		return time.Time{}, time.Time{}, errInvalidRange
	}
	if to.Sub(from) > maxRange {
		return time.Time{}, time.Time{}, errRangeTooWide
	}
	return from, to, nil
}

var (
	errInvalidRange = errString("invalid time range")
	errRangeTooWide = errString("time range exceeds retention limit")
	errBadCursor    = errString("invalid cursor")
)

type errString string

func (e errString) Error() string { return string(e) }

func parseHistoryLimit(values map[string][]string, def, cap int) (int, error) {
	limit := def
	if vals, ok := values["limit"]; ok && len(vals) > 0 && strings.TrimSpace(vals[0]) != "" {
		n, err := strconv.Atoi(strings.TrimSpace(vals[0]))
		if err != nil || n < 1 {
			return 0, errString("invalid limit")
		}
		limit = n
	}
	if limit > cap {
		limit = cap
	}
	return limit, nil
}

func historyCursor(values map[string][]string) string {
	if vals, ok := values["cursor"]; ok && len(vals) > 0 {
		return strings.TrimSpace(vals[0])
	}
	// Simple before=RFC3339+id fallback: combine into opaque cursor form.
	before := ""
	if vals, ok := values["before"]; ok && len(vals) > 0 {
		before = strings.TrimSpace(vals[0])
	}
	if before == "" {
		return ""
	}
	id := ""
	if vals, ok := values["id"]; ok && len(vals) > 0 {
		id = strings.TrimSpace(vals[0])
	}
	if _, err := time.Parse(time.RFC3339, before); err != nil {
		if _, err2 := time.Parse(time.RFC3339Nano, before); err2 != nil {
			return "invalid"
		}
	}
	return encodeHistoryCursor(mustParseTime(before), id)
}

func mustParseTime(v string) time.Time {
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t
	}
	return time.Now().UTC()
}

func historyDefWindow(def time.Duration, retentionDays int) time.Duration {
	maxRange := time.Duration(retentionDays) * 24 * time.Hour
	if maxRange < 24*time.Hour {
		maxRange = 24 * time.Hour
	}
	if def > maxRange {
		return maxRange
	}
	return def
}

func historyMaxRange(retentionDays int) time.Duration {
	maxRange := time.Duration(retentionDays) * 24 * time.Hour
	if maxRange < 24*time.Hour {
		maxRange = 24 * time.Hour
	}
	return maxRange
}

func writeHistoryTimeout(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		writeAdminError(w, http.StatusGatewayTimeout, "history_timeout", "history query timed out")
		return true
	}
	return false
}

func (a *AdminServer) handleHistoryRequests(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !a.allowHistory(clientIP(r)) {
		writeAdminError(w, http.StatusTooManyRequests, "history_rate_limited", "too many history requests; retry in one minute")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	store := historyStoreForQuery(a)
	if store == nil {
		writeHistoryPage(w, historyPage{Items: []any{}})
		return
	}
	st := store.Status()
	if !st.EnabledConfig || !st.Active {
		writeHistoryPage(w, disabledHistoryPage(store))
		return
	}
	q := r.URL.Query()
	maxRange := historyMaxRange(st.RetentionDays)
	from, to, err := parseHistoryRange(map[string][]string(q), historyDefWindow(24*time.Hour, st.RetentionDays), maxRange)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_range", err.Error())
		return
	}
	limit, err := parseHistoryLimit(map[string][]string(q), 100, 200)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_limit", err.Error())
		return
	}
	cursor := historyCursor(map[string][]string(q))
	if cursor == "invalid" {
		writeAdminError(w, http.StatusBadRequest, "invalid_cursor", "invalid cursor")
		return
	}
	filter := historyQueryFilter{
		From: from, To: to, Limit: limit, Cursor: cursor,
		Model: q.Get("model"), Tier: q.Get("tier"), Channel: q.Get("channel"),
		ProxyPool: q.Get("proxy_pool"),
	}
	if v := strings.TrimSpace(q.Get("success")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, "invalid_filter", "success must be true or false")
			return
		}
		filter.Success = &b
	}
	page, err := queryHistoryRequests(ctx, store, filter)
	if err != nil {
		if errors.Is(err, errBadCursor) {
			writeAdminError(w, http.StatusBadRequest, "invalid_cursor", "invalid cursor")
			return
		}
		if writeHistoryTimeout(w, err) {
			return
		}
		writeAdminError(w, http.StatusGatewayTimeout, "history_timeout", "history query timed out")
		return
	}
	select {
	case <-ctx.Done():
		writeAdminError(w, http.StatusGatewayTimeout, "history_timeout", "history query timed out")
	default:
		writeHistoryPage(w, page)
	}
}

func (a *AdminServer) handleHistoryAttempts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !a.allowHistory(clientIP(r)) {
		writeAdminError(w, http.StatusTooManyRequests, "history_rate_limited", "too many history requests; retry in one minute")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	store := historyStoreForQuery(a)
	if store == nil {
		writeHistoryPage(w, historyPage{Items: []any{}})
		return
	}
	st := store.Status()
	if !st.EnabledConfig || !st.Active {
		writeHistoryPage(w, disabledHistoryPage(store))
		return
	}
	q := r.URL.Query()
	maxRange := historyMaxRange(st.RetentionDays)
	from, to, err := parseHistoryRange(map[string][]string(q), historyDefWindow(24*time.Hour, st.RetentionDays), maxRange)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_range", err.Error())
		return
	}
	limit, err := parseHistoryLimit(map[string][]string(q), 100, 200)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_limit", err.Error())
		return
	}
	cursor := historyCursor(map[string][]string(q))
	if cursor == "invalid" {
		writeAdminError(w, http.StatusBadRequest, "invalid_cursor", "invalid cursor")
		return
	}
	filter := historyQueryFilter{
		From: from, To: to, Limit: limit, Cursor: cursor,
		Model: q.Get("model"), Tier: q.Get("tier"), Channel: q.Get("channel"),
		ProxyPool: q.Get("proxy_pool"), RequestID: q.Get("request_id"),
		FailureCls: q.Get("failure_class"),
	}
	page, err := queryHistoryAttempts(ctx, store, filter)
	if err != nil {
		if errors.Is(err, errBadCursor) {
			writeAdminError(w, http.StatusBadRequest, "invalid_cursor", "invalid cursor")
			return
		}
		if writeHistoryTimeout(w, err) {
			return
		}
		writeAdminError(w, http.StatusGatewayTimeout, "history_timeout", "history query timed out")
		return
	}
	select {
	case <-ctx.Done():
		writeAdminError(w, http.StatusGatewayTimeout, "history_timeout", "history query timed out")
	default:
		writeHistoryPage(w, page)
	}
}

func (a *AdminServer) handleHistorySeries(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !a.allowHistory(clientIP(r)) {
		writeAdminError(w, http.StatusTooManyRequests, "history_rate_limited", "too many history requests; retry in one minute")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	store := historyStoreForQuery(a)
	if store == nil {
		writeHistoryPage(w, historyPage{Items: []any{}})
		return
	}
	st := store.Status()
	if !st.EnabledConfig || !st.Active {
		writeHistoryPage(w, disabledHistoryPage(store))
		return
	}
	q := r.URL.Query()
	maxRange := historyMaxRange(st.RetentionDays)
	from, to, err := parseHistoryRange(map[string][]string(q), historyDefWindow(7*24*time.Hour, st.RetentionDays), maxRange)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_range", err.Error())
		return
	}
	limit, err := parseHistoryLimit(map[string][]string(q), 10080, 10080)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_limit", err.Error())
		return
	}
	cursor := historyCursor(map[string][]string(q))
	if cursor == "invalid" {
		writeAdminError(w, http.StatusBadRequest, "invalid_cursor", "invalid cursor")
		return
	}
	page, err := queryHistorySeries(ctx, store, historyQueryFilter{From: from, To: to, Limit: limit, Cursor: cursor})
	if err != nil {
		if errors.Is(err, errBadCursor) {
			writeAdminError(w, http.StatusBadRequest, "invalid_cursor", "invalid cursor")
			return
		}
		if writeHistoryTimeout(w, err) {
			return
		}
		writeAdminError(w, http.StatusGatewayTimeout, "history_timeout", "history query timed out")
		return
	}
	select {
	case <-ctx.Done():
		writeAdminError(w, http.StatusGatewayTimeout, "history_timeout", "history query timed out")
	default:
		writeHistoryPage(w, page)
	}
}

func applyHistoryCursorDesc(times []time.Time, ids []string, cursor string) ([]int, string) {
	if cursor == "" {
		idx := make([]int, len(times))
		for i := range idx {
			idx[i] = i
		}
		return idx, ""
	}
	ct, cid := decodeHistoryCursor(cursor)
	if ct.IsZero() && cid == "" {
		return nil, "invalid"
	}
	if ct.IsZero() || cid == "" {
		return nil, "invalid"
	}
	var idx []int
	for i := range times {
		if times[i].After(ct) {
			continue
		}
		if times[i].Equal(ct) && ids[i] >= cid {
			continue
		}
		idx = append(idx, i)
	}
	return idx, ""
}

func normalizeHistoryLimit(limit, def, cap int) int {
	if limit <= 0 {
		limit = def
	}
	if limit > cap {
		limit = cap
	}
	return limit
}

// queryHistoryRequests scans recognized request segments newest-first,
// newest-first within each segment, applying time/filter/cursor while
// scanning until limit+1 matches are collected or all relevant segments
// are exhausted. next_cursor is set only when more matches exist.
func queryHistoryRequests(ctx context.Context, store *HistoryStore, f historyQueryFilter) (historyPage, error) {
	st := store.Status()
	limit := normalizeHistoryLimit(f.Limit, 100, 200)
	ct, cid, _, err := decodeHistoryCursorStrict(historyKindRequest, f.Cursor)
	if err != nil {
		return historyPage{}, errBadCursor
	}
	hasCursor := f.Cursor != ""
	if err := historyCheckCtx(ctx); err != nil {
		return historyPage{}, err
	}
	dir := store.historyReadDir()
	if dir == "" {
		return historyPage{Items: []any{}, Gap: false, Dropped: st.Dropped, Active: st.Active, LastError: st.LastError}, nil
	}
	names := historySegmentNamesFor(dir, historyKindRequest, f.From, f.To)
	type row struct {
		v historyRequestLine
		t time.Time
	}
	var rows []row
	gap := false
outer:
	for _, name := range names {
		if err := historyCheckCtx(ctx); err != nil {
			return historyPage{}, err
		}
		lines, segGap, err := readHistorySegmentNewestFirst(ctx, joinHistoryPath(dir, name))
		if err != nil {
			return historyPage{}, err
		}
		if segGap {
			gap = true
		}
		for _, line := range lines {
			if err := historyCheckCtx(ctx); err != nil {
				return historyPage{}, err
			}
			var env historyEnvelope
			if err := json.Unmarshal(line, &env); err != nil || env.V != historySchemaV || env.Kind != string(historyKindRequest) {
				gap = true
				continue
			}
			var v historyRequestLine
			if err := json.Unmarshal(line, &v); err != nil {
				gap = true
				continue
			}
			t, err := time.Parse(time.RFC3339Nano, v.Time)
			if err != nil {
				gap = true
				continue
			}
			if t.Before(f.From) || t.After(f.To) {
				continue
			}
			if f.Model != "" && v.Model != f.Model {
				continue
			}
			if f.Tier != "" && v.Tier != f.Tier {
				continue
			}
			if f.Channel != "" && v.Channel != f.Channel {
				continue
			}
			if f.ProxyPool != "" && v.ProxyPool != f.ProxyPool {
				continue
			}
			if f.Success != nil && v.Success != *f.Success {
				continue
			}
			if hasCursor {
				if t.After(ct) {
					continue
				}
				if t.Equal(ct) && v.RequestID >= cid {
					continue
				}
			}
			rows = append(rows, row{v: v, t: t})
			if len(rows) >= limit+1 {
				break outer
			}
		}
	}
	if store.GapFlag() {
		gap = true
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].t.Equal(rows[j].t) {
			return rows[i].v.RequestID > rows[j].v.RequestID
		}
		return rows[i].t.After(rows[j].t)
	})
	// rows are already newest-first; keep only limit+1 then trim.
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit+1]
	}
	var items []any
	var next string
	n := len(rows)
	if hasMore {
		n = limit
	}
	for i := 0; i < n; i++ {
		items = append(items, rows[i].v)
	}
	if hasMore && n > 0 {
		next = encodeHistoryCursor(rows[n-1].t, rows[n-1].v.RequestID)
	}
	if items == nil {
		items = []any{}
	}
	return historyPage{Items: items, NextCursor: next, Gap: gap, Dropped: st.Dropped, Active: st.Active, LastError: st.LastError}, nil
}

func queryHistoryAttempts(ctx context.Context, store *HistoryStore, f historyQueryFilter) (historyPage, error) {
	st := store.Status()
	limit := normalizeHistoryLimit(f.Limit, 100, 200)
	ct, crid, catt, err := decodeHistoryCursorStrict(historyKindAttempt, f.Cursor)
	if err != nil {
		return historyPage{}, errBadCursor
	}
	hasCursor := f.Cursor != ""
	if err := historyCheckCtx(ctx); err != nil {
		return historyPage{}, err
	}
	dir := store.historyReadDir()
	if dir == "" {
		return historyPage{Items: []any{}, Gap: false, Dropped: st.Dropped, Active: st.Active, LastError: st.LastError}, nil
	}
	names := historySegmentNamesFor(dir, historyKindAttempt, f.From, f.To)
	type row struct {
		v historyAttemptLine
		t time.Time
	}
	var rows []row
	gap := false
outer:
	for _, name := range names {
		if err := historyCheckCtx(ctx); err != nil {
			return historyPage{}, err
		}
		lines, segGap, err := readHistorySegmentNewestFirst(ctx, joinHistoryPath(dir, name))
		if err != nil {
			return historyPage{}, err
		}
		if segGap {
			gap = true
		}
		for _, line := range lines {
			if err := historyCheckCtx(ctx); err != nil {
				return historyPage{}, err
			}
			var env historyEnvelope
			if err := json.Unmarshal(line, &env); err != nil || env.V != historySchemaV || env.Kind != string(historyKindAttempt) {
				gap = true
				continue
			}
			var v historyAttemptLine
			if err := json.Unmarshal(line, &v); err != nil {
				gap = true
				continue
			}
			t, err := time.Parse(time.RFC3339Nano, v.Time)
			if err != nil {
				gap = true
				continue
			}
			if t.Before(f.From) || t.After(f.To) {
				continue
			}
			if f.Model != "" && v.Model != f.Model {
				continue
			}
			if f.Tier != "" && v.Tier != f.Tier {
				continue
			}
			if f.Channel != "" && v.Channel != f.Channel {
				continue
			}
			if f.ProxyPool != "" && v.ProxyPool != f.ProxyPool {
				continue
			}
			if f.RequestID != "" && v.RequestID != f.RequestID {
				continue
			}
			if f.FailureCls != "" && v.FailureClass != f.FailureCls {
				continue
			}
			if hasCursor {
				if t.After(ct) {
					continue
				}
				if t.Equal(ct) {
					if v.RequestID > crid {
						continue
					}
					if v.RequestID == crid && v.Attempt >= catt {
						continue
					}
				}
			}
			rows = append(rows, row{v: v, t: t})
			if len(rows) >= limit+1 {
				break outer
			}
		}
	}
	if store.GapFlag() {
		gap = true
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].t.Equal(rows[j].t) {
			if rows[i].v.RequestID != rows[j].v.RequestID {
				return rows[i].v.RequestID > rows[j].v.RequestID
			}
			return rows[i].v.Attempt > rows[j].v.Attempt
		}
		return rows[i].t.After(rows[j].t)
	})
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit+1]
	}
	var items []any
	var next string
	n := len(rows)
	if hasMore {
		n = limit
	}
	for i := 0; i < n; i++ {
		items = append(items, rows[i].v)
	}
	if hasMore && n > 0 {
		next = encodeHistoryCursor(rows[n-1].t, rows[n-1].v.RequestID+"#"+strconv.Itoa(rows[n-1].v.Attempt))
	}
	if items == nil {
		items = []any{}
	}
	return historyPage{Items: items, NextCursor: next, Gap: gap, Dropped: st.Dropped, Active: st.Active, LastError: st.LastError}, nil
}

func queryHistorySeries(ctx context.Context, store *HistoryStore, f historyQueryFilter) (historyPage, error) {
	st := store.Status()
	limit := normalizeHistoryLimit(f.Limit, 10080, 10080)
	ct, cid, _, err := decodeHistoryCursorStrict(historyKindMinute, f.Cursor)
	if err != nil {
		return historyPage{}, errBadCursor
	}
	hasCursor := f.Cursor != ""
	if err := historyCheckCtx(ctx); err != nil {
		return historyPage{}, err
	}
	dir := store.historyReadDir()
	if dir == "" {
		return historyPage{Items: []any{}, Gap: false, Dropped: st.Dropped, Active: st.Active, LastError: st.LastError}, nil
	}
	names := historySegmentNamesFor(dir, historyKindMinute, f.From, f.To)
	type row struct {
		v historyMinuteLine
		t time.Time
	}
	var rows []row
	gap := false
outer:
	for _, name := range names {
		if err := historyCheckCtx(ctx); err != nil {
			return historyPage{}, err
		}
		lines, segGap, err := readHistorySegmentNewestFirst(ctx, joinHistoryPath(dir, name))
		if err != nil {
			return historyPage{}, err
		}
		if segGap {
			gap = true
		}
		for _, line := range lines {
			if err := historyCheckCtx(ctx); err != nil {
				return historyPage{}, err
			}
			var env historyEnvelope
			if err := json.Unmarshal(line, &env); err != nil || env.V != historySchemaV || env.Kind != string(historyKindMinute) {
				gap = true
				continue
			}
			var v historyMinuteLine
			if err := json.Unmarshal(line, &v); err != nil {
				gap = true
				continue
			}
			t, err := time.Parse(time.RFC3339Nano, v.Minute)
			if err != nil {
				t, err = time.Parse(time.RFC3339Nano, v.Time)
				if err != nil {
					gap = true
					continue
				}
			}
			if t.Before(f.From) || t.After(f.To) {
				continue
			}
			if hasCursor {
				if t.After(ct) {
					continue
				}
				if t.Equal(ct) && v.Minute >= cid {
					continue
				}
			}
			rows = append(rows, row{v: v, t: t})
			if len(rows) >= limit+1 {
				break outer
			}
		}
	}
	if store.GapFlag() {
		gap = true
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].t.Equal(rows[j].t) {
			return rows[i].v.Minute > rows[j].v.Minute
		}
		return rows[i].t.After(rows[j].t)
	})
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit+1]
	}
	var items []any
	var next string
	n := len(rows)
	if hasMore {
		n = limit
	}
	for i := 0; i < n; i++ {
		items = append(items, rows[i].v)
	}
	if hasMore && n > 0 {
		next = encodeHistoryCursor(rows[n-1].t, rows[n-1].v.Minute)
	}
	if items == nil {
		items = []any{}
	}
	return historyPage{Items: items, NextCursor: next, Gap: gap, Dropped: st.Dropped, Active: st.Active, LastError: st.LastError}, nil
}
