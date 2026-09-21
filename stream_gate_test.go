package main

import (
	"errors"
	"reflect"
	"testing"
)

func TestSSEStartupGateImmediateEOF(t *testing.T) {
	gate := newSSEStartupGate()
	gate.NoteReadError(nil)
	if !gate.HasStartupFailure() {
		t.Fatalf("immediate EOF must be startup failure before commit")
	}
	if gate.ShouldCommit() {
		t.Fatalf("immediate EOF must not commit")
	}
}

func TestSSEStartupGateCreatedStartThenEOF(t *testing.T) {
	gate := newSSEStartupGate()
	frame := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"m\"}}\n\n"
	gate.AddRawFrame([]byte(frame))
	gate.AddEvents([]bridgeStreamEvent{{Kind: "start", ResponseID: "resp_1", Model: "m"}})
	if gate.ShouldCommit() {
		t.Fatalf("start alone must not commit")
	}
	gate.NoteReadError(nil)
	if !gate.HasStartupFailure() {
		t.Fatalf("start then EOF must be startup failure")
	}
	if gate.ShouldCommit() {
		t.Fatalf("must remain uncommitted after failure")
	}
}

func TestSSEStartupGatePrecommitErrorEvent(t *testing.T) {
	gate := newSSEStartupGate()
	gate.AddRawFrame([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"boom\"}}\n\n"))
	gate.AddEvents([]bridgeStreamEvent{{Kind: "error", Error: "boom", ErrorType: "upstream_error"}})
	if !gate.HasStartupFailure() {
		t.Fatalf("pre-commit error event must be startup failure")
	}
	if gate.ShouldCommit() {
		t.Fatalf("error must not commit")
	}
}

func TestSSEStartupGatePrecommitFailedTerminalIsAlsoFailure(t *testing.T) {
	// Responses response.failed decodes to an error kind before commit.
	gate := newSSEStartupGate()
	gate.AddEvents([]bridgeStreamEvent{{Kind: "start", ResponseID: "resp_2"}})
	gate.AddEvents([]bridgeStreamEvent{{Kind: "error", Error: "upstream Responses request failed", ErrorType: "upstream_error"}})
	if !gate.HasStartupFailure() {
		t.Fatalf("response.failed before commit must be startup failure")
	}
	if gate.ShouldCommit() {
		t.Fatalf("must not commit on failure")
	}
}

func TestSSEStartupGateMalformedParseIsStartupFailure(t *testing.T) {
	gate := newSSEStartupGate()
	gate.AddRawFrame([]byte("data: {invalid json\n\n"))
	gate.NoteParseError(errors.New("invalid upstream SSE JSON: unexpected"))
	if !gate.HasStartupFailure() {
		t.Fatalf("malformed parse before commit must be startup failure")
	}
	if gate.ShouldCommit() {
		t.Fatalf("malformed must not commit")
	}
}

func TestSSEStartupGateMalformedAfterCommitIsNotStartupFailure(t *testing.T) {
	gate := newSSEStartupGate()
	gate.AddEvents([]bridgeStreamEvent{{Kind: "text", Text: "hello"}})
	if !gate.ShouldCommit() {
		t.Fatalf("deliverable must commit")
	}
	gate.NoteParseError(errors.New("invalid upstream SSE JSON: late"))
	if gate.HasStartupFailure() {
		t.Fatalf("malformed after commit must NOT be startup failure")
	}
	if !gate.ShouldCommit() {
		t.Fatalf("commit must remain after post-commit parse error")
	}
}

func TestSSEStartupGateEmptyNormalCompletionCommits(t *testing.T) {
	gate := newSSEStartupGate()
	gate.AddEvents([]bridgeStreamEvent{{Kind: "start", ResponseID: "resp_3"}})
	if gate.ShouldCommit() {
		t.Fatalf("start alone must not commit")
	}
	// Normal terminal done even with no deliverable is valid completion.
	gate.AddEvents([]bridgeStreamEvent{{Kind: "done"}})
	if !gate.ShouldCommit() {
		t.Fatalf("empty normal completion via done must commit")
	}
	if gate.HasStartupFailure() {
		t.Fatalf("empty completion must not be failure")
	}
	// EOF after commit is not a startup failure.
	gate.NoteReadError(nil)
	if gate.HasStartupFailure() {
		t.Fatalf("EOF after commit must not be failure")
	}
}

func TestSSEStartupGateDeliverableThenEOFIsNotFailure(t *testing.T) {
	gate := newSSEStartupGate()
	gate.AddRawFrame([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
	gate.AddEvents([]bridgeStreamEvent{{Kind: "text", Text: "hi"}})
	if !gate.ShouldCommit() {
		t.Fatalf("deliverable text must commit")
	}
	gate.NoteReadError(nil)
	if gate.HasStartupFailure() {
		t.Fatalf("EOF after deliverable must not be startup failure")
	}
	// Error after commit is also not startup failure.
	gate2 := newSSEStartupGate()
	gate2.AddEvents([]bridgeStreamEvent{{Kind: "reasoning", Text: "think"}})
	gate2.AddEvents([]bridgeStreamEvent{{Kind: "error", Error: "late boom"}})
	if !gate2.ShouldCommit() {
		t.Fatalf("reasoning must commit")
	}
	if gate2.HasStartupFailure() {
		t.Fatalf("error after commit must not be startup failure")
	}
}

func TestSSEStartupGateDeliverableKindsCommit(t *testing.T) {
	for _, kind := range []string{"text", "reasoning", "reasoning_signature", "tool_start", "tool_delta"} {
		gate := newSSEStartupGate()
		gate.AddEvents([]bridgeStreamEvent{{Kind: kind, Text: "x", ToolKey: "k"}})
		if !gate.ShouldCommit() {
			t.Fatalf("kind %q must permit commit", kind)
		}
		if gate.HasStartupFailure() {
			t.Fatalf("kind %q must not be failure", kind)
		}
	}
}

func TestSSEStartupGateMetadataOnlyDoesNotCommit(t *testing.T) {
	gate := newSSEStartupGate()
	gate.AddEvents([]bridgeStreamEvent{{Kind: "start", ResponseID: "resp_9"}})
	gate.AddEvents([]bridgeStreamEvent{{Kind: "usage", Usage: &bridgeUsage{Input: 10}}})
	if gate.ShouldCommit() {
		t.Fatalf("start/usage alone must not commit")
	}
	gate.AddEvents([]bridgeStreamEvent{{Kind: "finish", Stop: "stop"}})
	if gate.ShouldCommit() {
		t.Fatalf("finish alone must not commit (done is required)")
	}
}

func TestSSEStartupGateUnterminatedFrameBeforeCommitIsFailure(t *testing.T) {
	gate := newSSEStartupGate()
	gate.AddEvents([]bridgeStreamEvent{{Kind: "start"}})
	gate.NoteReadError(errSSEUnexpectedEOF)
	if !gate.HasStartupFailure() {
		t.Fatalf("unterminated frame before commit must be startup failure")
	}
}

func TestSSEStartupGatePreserveBufferedOrder(t *testing.T) {
	gate := newSSEStartupGate()
	frames := [][]byte{
		[]byte("data: {\"type\":\"response.created\"}\n\n"),
		[]byte("data: {\"delta\":\"hello\"}\n\n"),
	}
	eventsSeq := [][]bridgeStreamEvent{
		{{Kind: "start", ResponseID: "resp_o"}},
		{{Kind: "text", Text: "hello"}},
	}
	for i, frame := range frames {
		gate.AddRawFrame(frame)
		gate.AddEvents(eventsSeq[i])
		if i == 0 && gate.ShouldCommit() {
			t.Fatalf("after first frame (start) must not commit yet")
		}
	}
	if !gate.ShouldCommit() {
		t.Fatalf("after deliverable must commit")
	}
	bufferedFrames := gate.BufferedRawFrames()
	if len(bufferedFrames) != len(frames) {
		t.Fatalf("frames=%d want %d", len(bufferedFrames), len(frames))
	}
	for i := range frames {
		if string(bufferedFrames[i]) != string(frames[i]) {
			t.Fatalf("frame[%d] order mismatch: got %q want %q", i, bufferedFrames[i], frames[i])
		}
	}
	concatenated := gate.BufferedRaw()
	expectedConcat := []byte{}
	for _, f := range frames {
		expectedConcat = append(expectedConcat, f...)
	}
	if string(concatenated) != string(expectedConcat) {
		t.Fatalf("concatenated raw mismatch")
	}
	bufferedEvents := gate.BufferedEvents()
	var kinds []string
	for _, ev := range bufferedEvents {
		kinds = append(kinds, ev.Kind)
	}
	wantKinds := []string{"start", "text"}
	if !reflect.DeepEqual(kinds, wantKinds) {
		t.Fatalf("event order %v want %v", kinds, wantKinds)
	}
	// Buffered opening portion ends at commit: further frames/events after
	// commit are not buffered and do not change the decision.
	gate.AddRawFrame([]byte("data: {\"delta\":\" world\"}\n\n"))
	gate.AddEvents([]bridgeStreamEvent{{Kind: "text", Text: " world"}})
	if len(gate.BufferedRawFrames()) != len(frames) {
		t.Fatalf("post-commit frames must not extend opening buffer")
	}
	if len(gate.BufferedEvents()) != len(wantKinds) {
		t.Fatalf("post-commit events must not extend opening buffer")
	}
	gate.NoteReadError(nil)
	if gate.HasStartupFailure() {
		t.Fatalf("post-commit EOF must not become failure")
	}
	if gate.HasStartupFailure() {
		t.Fatalf("post-commit EOF must not become failure")
	}
	if len(gate.BufferedRawFrames()) != len(frames) {
		t.Fatalf("buffer must stay stable after commit")
	}
	// Verify mixed frame/event interleaving preserves exact order when multiple
	// bridge events arise from a single SSE frame.
	gate2 := newSSEStartupGate()
	mixedFrame := []byte("data: {\"type\":\"response.output_item.added\"}\n\n")
	gate2.AddRawFrame(mixedFrame)
	gate2.AddEvents([]bridgeStreamEvent{{Kind: "start"}, {Kind: "tool_start", ToolKey: "k1"}, {Kind: "tool_delta", ToolKey: "k1", Text: "{\"a\":1}"}})
	gotKinds := func() []string {
		var out []string
		for _, ev := range gate2.BufferedEvents() {
			out = append(out, ev.Kind)
		}
		return out
	}()
	wantMixed := []string{"start", "tool_start", "tool_delta"}
	if !reflect.DeepEqual(gotKinds, wantMixed) {
		t.Fatalf("mixed frame event order %v want %v", gotKinds, wantMixed)
	}
	if string(gate2.BufferedRaw()) != string(mixedFrame) {
		t.Fatalf("mixed raw frame order mismatch")
	}
}

func TestSSEStartupGateUsageAloneThenEOFIsFailure(t *testing.T) {
	gate := newSSEStartupGate()
	gate.AddEvents([]bridgeStreamEvent{{Kind: "usage", Usage: &bridgeUsage{Input: 5, Output: 7}}})
	if gate.ShouldCommit() {
		t.Fatalf("usage alone must not commit")
	}
	gate.NoteReadError(nil)
	if !gate.HasStartupFailure() {
		t.Fatalf("usage alone then EOF must be startup failure")
	}
}
