package decisioncard

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

func TestHumanTaskOperationFixedVectors(t *testing.T) {
	for _, tc := range []struct{ run, call, want string }{
		{"run-a", "event\x00tool_call:1:0:mock-1:ask_human", "human-task-operation:v1:d441c51c-f34b-533e-a3c4-5d8151d370ce"},
		{"run-b", "event\x00tool_call:1:0:mock-1:ask_human", "human-task-operation:v1:9c9b8621-0ebf-5aa0-9d21-f769db741ccf"},
		{"run-a", "event\x00tool_call:1:1:mock-1:ask_human", "human-task-operation:v1:7b68683d-7da0-50bb-a694-be05682e7d47"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			id, err := NewHumanTaskOperationID(tc.run, tc.call)
			if err != nil || id.String() != tc.want {
				t.Fatalf("operation = %q, %v; want %q", id.String(), err, tc.want)
			}
			retry, err := NewHumanTaskOperationID(tc.run, tc.call)
			if err != nil || retry != id {
				t.Fatalf("retry = %v, %v", retry, err)
			}
			decoded, err := parseHumanTaskOperationID(id.String())
			if err != nil || decoded != id {
				t.Fatalf("decode = %v, %v", decoded, err)
			}
		})
	}
}

func TestHumanTaskOperationPreservesCompleteCoordinates(t *testing.T) {
	seen := map[HumanTaskOperationID][2]string{}
	for _, pair := range [][2]string{
		{"a\x00b", "c"}, {"a", "b\x00c"}, {"ab", "c"}, {"a", "bc"},
		{"a", "b"}, {" a", "b"}, {"a", "b "}, {"a", "b\x00"},
	} {
		id, err := NewHumanTaskOperationID(pair[0], pair[1])
		if err != nil {
			t.Fatal(err)
		}
		if other, ok := seen[id]; ok {
			t.Fatalf("coordinates %q and %q alias", pair, other)
		}
		seen[id] = pair
		if strings.ContainsRune(id.String(), '\x00') {
			t.Fatal("raw delimiter escaped into durable identity")
		}
	}
	for _, pair := range [][2]string{{"", "call"}, {"run", ""}, {" ", "call"}, {"run", "\t"}} {
		if _, err := NewHumanTaskOperationID(pair[0], pair[1]); err == nil {
			t.Fatalf("accepted missing coordinate %q", pair)
		}
	}
}

func TestHumanTaskOperationAnchorRejectsNonCanonicalWire(t *testing.T) {
	id, err := NewHumanTaskOperationID("run", "event\x00tool_call:1")
	if err != nil {
		t.Fatal(err)
	}
	source, err := events.NewRootRoutingSource("entity")
	if err != nil {
		t.Fatal(err)
	}
	input := HumanTaskAnchor{RequesterAgentID: "actor", OperationID: id, Category: "review", Scope: Scope{Kind: ScopeGlobal}, Source: source}
	anchor, err := NewHumanTaskAnchor(input)
	if err != nil {
		t.Fatal(err)
	}
	for _, wire := range []string{"", "event\x00tool_call:1", "old-operation", " " + id.String(), id.String() + " ", strings.ToUpper(id.String()), "human-task-operation:v1:00000000-0000-0000-0000-000000000000", "human-task-operation:v1:6ba7b812-9dad-11d1-80b4-00c04fd430c8"} {
		t.Run(wire, func(t *testing.T) {
			data := anchor.data.Interface().(map[string]any)
			data["operation_id"] = wire
			value, err := canonicaljson.FromGo(data)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := (Anchor{kind: AnchorKindHumanTask, data: value}).HumanTask(); err == nil {
				t.Fatalf("accepted noncanonical operation %q", wire)
			}
		})
	}
	input.OperationID = HumanTaskOperationID{}
	if _, err := NewHumanTaskAnchor(input); err == nil {
		t.Fatal("accepted zero operation")
	}
}
