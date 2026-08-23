package agentframe

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

func TestDurableExecutionFramePreservesByteDistinctPayloads(t *testing.T) {
	seed, event, surface := testExecutionFrameInputs(t)
	for _, payload := range []string{`{"a":1,"b":2}`, `{ "a": 1, "b": 2 }`} {
		candidate := eventtest.RunCreatingRootIngress(
			event.ID(), event.Type(), "operator", event.TaskID(), json.RawMessage(payload),
			0, event.RunID(), "", events.EnvelopeForEntityID(events.EventEnvelope{}, event.EntityID()), time.Unix(1, 0).UTC(),
		)
		frame := completeTestFrame(t, seed, TurnDraft{Kind: TurnInitial, Event: candidate}, surface)
		raw, err := EncodeDurable(frame)
		if err != nil {
			t.Fatalf("EncodeDurable: %v", err)
		}
		hydrated, err := DecodeDurable(raw)
		if err != nil {
			t.Fatalf("DecodeDurable: %v", err)
		}
		if !bytes.Equal(hydrated.Turn.Event.Payload, []byte(payload)) || !reflect.DeepEqual(hydrated, frame) {
			t.Fatalf("hydrated frame did not preserve exact payload bytes: got %q want %q", hydrated.Turn.Event.Payload, payload)
		}
	}
}

func TestDurableExecutionFramePreservesToolResultAndRejectsMutation(t *testing.T) {
	seed, event, surface := testExecutionFrameInputs(t)
	frame := completeTestFrame(t, seed, TurnDraft{
		Kind: TurnToolContinuation, Event: event,
		ParentFrameID: "agent-frame:v1:00000000-0000-4000-8000-000000000099",
		InputRole:     "tool", InputContent: `[{"ok":true,"result":{"status":"ready"}}]`,
	}, surface)
	raw, err := EncodeDurable(frame)
	if err != nil {
		t.Fatal(err)
	}
	hydrated, err := DecodeDurable(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hydrated.Turn.ToolResult, frame.Turn.ToolResult) {
		t.Fatalf("tool result changed: got %s want %s", hydrated.Turn.ToolResult, frame.Turn.ToolResult)
	}
	mutated := append([]byte(nil), raw...)
	mutated = append(mutated, ' ')
	if _, err := DecodeDurable(mutated); err == nil {
		t.Fatal("DecodeDurable accepted non-canonical trailing bytes")
	}
}
