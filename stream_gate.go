package main

import (
	"errors"
)

// sseStartupGate buffers only the opening portion of an upstream SSE stream
// until a safe commit condition appears or an uncommitted startup failure is
// observed. It is intentionally networking-free: callers feed raw SSE frames
// and/or parsed bridgeStreamEvents plus parse/read outcomes and the gate
// exposes the exact decision and buffering state that the gateway startup
// recovery wiring will need for both raw passthrough release and
// bridge-event transcode release, preserving exact frame/event order.
//
// Classification (spec-required):
//   - start / usage alone never commits.
//   - deliverable kinds text, reasoning, reasoning_signature, tool_start,
//     tool_delta commit.
//   - normal terminal done commits even with no prior deliverable.
//   - error before commit is a startup failure.
//   - malformed parse (parser.Parse error), unterminated frame/EOF before
//     commit is a startup failure.
//   - error or EOF after a commit condition is NOT a startup failure.
type sseStartupGate struct {
	rawFrames [][]byte
	events    []bridgeStreamEvent
	committed bool
	failed    bool
	failure   error
}

// newSSEStartupGate returns a fresh startup gate.
func newSSEStartupGate() *sseStartupGate {
	return &sseStartupGate{}
}

// AddRawFrame buffers one complete SSE frame (including the trailing \n\n
// boundary) preserving exact byte order. A copy is retained so the caller
// may reuse its buffer. Frames added after a terminal decision are ignored
// because the buffered opening portion ends at the decision boundary.
func (g *sseStartupGate) AddRawFrame(frame []byte) {
	if g == nil || g.committed || g.failed || len(frame) == 0 {
		return
	}
	copied := make([]byte, len(frame))
	copy(copied, frame)
	g.rawFrames = append(g.rawFrames, copied)
}

// AddEvents buffers parsed bridge events in exact emission order and advances
// the gate decision. Deliverable kinds and done commit once observed;
// an error event before commit marks a startup failure. The entire slice
// from the current SSE frame is buffered atomically: if a commit occurs
// mid-slice the remaining events from that same frame are still retained so
// the buffered opening portion preserves exact frame/event order for later
// raw passthrough or bridge transcode release. Events from subsequent frames
// after a decision are ignored.
func (g *sseStartupGate) AddEvents(events []bridgeStreamEvent) {
	if g == nil || g.failed || len(events) == 0 {
		return
	}
	if g.committed {
		return
	}
	for _, ev := range events {
		if g.failed {
			break
		}
		alreadyCommitted := g.committed
		g.events = append(g.events, ev)
		switch ev.Kind {
		case "error":
			if !alreadyCommitted {
				g.failed = true
				if ev.Error != "" {
					g.failure = errors.New(ev.Error)
				} else {
					g.failure = errors.New("upstream stream error")
				}
			}
		case "text", "reasoning", "reasoning_signature", "tool_start", "tool_delta", "done":
			g.committed = true
		default:
			// start, usage, finish and any unknown kinds never commit or fail.
		}
		// Do not break mid-slice: the whole frame's events must be preserved
		// in order, but an error after a commit in the same slice is not a
		// startup failure (handled via alreadyCommitted above).
	}
}

// NoteParseError records a malformed SSE JSON / parse failure. Before commit
// it is a startup failure; after commit it is not.
func (g *sseStartupGate) NoteParseError(err error) {
	if g == nil || g.committed || g.failed || err == nil {
		return
	}
	g.failed = true
	g.failure = err
}

// NoteReadError records an SSE read outcome (EOF, unterminated frame,
// transport error). A nil err means clean EOF. Before commit any EOF or
// unterminated condition is a startup failure; after commit it is not.
func (g *sseStartupGate) NoteReadError(err error) {
	if g == nil || g.committed || g.failed {
		return
	}
	if err == nil {
		g.failed = true
		g.failure = errSSEUnexpectedEOF
		return
	}
	// Any non-nil read error before commit is a startup failure. This
	// includes errSSEUnexpectedEOF (unterminated final line) and I/O errors.
	g.failed = true
	g.failure = err
}

// ShouldCommit reports whether a safe commit condition has been observed.
func (g *sseStartupGate) ShouldCommit() bool {
	if g == nil {
		return false
	}
	return g.committed
}

// HasStartupFailure reports whether an uncommitted startup failure was
// observed. True only when no commit condition was seen before the failure.
func (g *sseStartupGate) HasStartupFailure() bool {
	if g == nil {
		return false
	}
	return g.failed
}

// FailureCause returns the underlying failure cause when HasStartupFailure is
// true, otherwise nil.
func (g *sseStartupGate) FailureCause() error {
	if g == nil {
		return nil
	}
	return g.failure
}

// BufferedRaw returns the concatenated buffered raw frames in arrival order.
func (g *sseStartupGate) BufferedRaw() []byte {
	if g == nil || len(g.rawFrames) == 0 {
		return nil
	}
	total := 0
	for _, frame := range g.rawFrames {
		total += len(frame)
	}
	out := make([]byte, 0, total)
	for _, frame := range g.rawFrames {
		out = append(out, frame...)
	}
	return out
}

// BufferedRawFrames returns a copy of the buffered raw frames preserving
// exact frame boundaries and order.
func (g *sseStartupGate) BufferedRawFrames() [][]byte {
	if g == nil || len(g.rawFrames) == 0 {
		return nil
	}
	out := make([][]byte, len(g.rawFrames))
	for i, frame := range g.rawFrames {
		copied := make([]byte, len(frame))
		copy(copied, frame)
		out[i] = copied
	}
	return out
}

// BufferedEvents returns a copy of the buffered bridge events in emission
// order (the same order a later transcode release must replay).
func (g *sseStartupGate) BufferedEvents() []bridgeStreamEvent {
	if g == nil || len(g.events) == 0 {
		return nil
	}
	out := make([]bridgeStreamEvent, len(g.events))
	copy(out, g.events)
	return out
}

// isStartupDeliverableKind reports the bridge kinds that permit commit when
// observed before any startup failure. Exported only for testing convenience
// via the unexported helper; tests use the gate behavior rather than calling
// this directly.
func isStartupDeliverableKind(kind string) bool {
	switch kind {
	case "text", "reasoning", "reasoning_signature", "tool_start", "tool_delta":
		return true
	default:
		return false
	}
}
