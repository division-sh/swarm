package loopruntime

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
)

func TestCapturedContextRetainsOwnedHistoricalGeneration(t *testing.T) {
	now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	activation, err := New("run", "entity", "orders", "review", "revision_id", "start", "working", 3, now)
	if err != nil {
		t.Fatal(err)
	}
	captured := activation.Generation()
	want := activation.Context()
	if _, err := activation.Repeat("working", "repeat", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, closed := range []bool{false, true} {
		if closed {
			if err := activation.Close("done", "close", now.Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}
		}
		before := activation
		got, err := activation.CapturedContext(captured)
		if err != nil || !reflect.DeepEqual(got, want) || activation != before {
			t.Fatalf("closed=%v: context=%v err=%v owner=%+v", closed, got, err, activation)
		}
		got["revision_id"] = "hostile"
		again, _ := activation.CapturedContext(captured)
		if !reflect.DeepEqual(again, want) {
			t.Fatal("context aliases mutable owner state")
		}
	}
	for name, mutate := range map[string]func(*attemptgeneration.Generation){
		"flow":           func(g *attemptgeneration.Generation) { g.FlowID = "sibling" },
		"loop":           func(g *attemptgeneration.Generation) { g.LoopID = "sibling" },
		"activation":     func(g *attemptgeneration.Generation) { g.ActivationID = "sibling" },
		"revision field": func(g *attemptgeneration.Generation) { g.RevisionField = "other" },
		"revision":       func(g *attemptgeneration.Generation) { g.RevisionID = activation.RevisionID },
		"future":         func(g *attemptgeneration.Generation) { g.Attempt = 3; g.RevisionID = revisionID(g.ActivationID, 3) },
		"noncanonical":   func(g *attemptgeneration.Generation) { g.FlowID += " " },
	} {
		t.Run(name, func(t *testing.T) {
			g := captured
			mutate(&g)
			if context, err := activation.CapturedContext(g); err == nil || context != nil {
				t.Fatalf("accepted unowned reference %+v: %v", g, context)
			}
		})
	}
	activation.RevisionID = "corrupt-current"
	if _, err := activation.CapturedContext(captured); err == nil {
		t.Fatal("accepted malformed owning activation")
	}
}
