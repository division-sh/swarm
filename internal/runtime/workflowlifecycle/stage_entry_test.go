package workflowlifecycle

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestA2StageEntryUsesAdmittedOccurrenceNotClock(t *testing.T) {
	owner, err := flowidentity.NewRunScopedFlowInstance("run-1", flowidentity.RouteForInstancePath("flow/one"))
	if err != nil {
		t.Fatal(err)
	}
	cause, err := NewCompiledTransition(effectTestCompiledTransition(t), handlerselection.NotApplicable(), nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	makeEntry := func(eventID, deliveryID string, clock time.Time) string {
		t.Helper()
		effect, err := NewAcceptedEvent(owner.Route, identity.NormalizeEntityID("entity-1"), eventID, "review.approved", executionmode.Live, clock, &cause)
		if err != nil {
			t.Fatal(err)
		}
		effect, err = effect.WithExecutionOccurrence("delivery", deliveryID)
		if err != nil {
			t.Fatal(err)
		}
		entry, enters, err := effect.StageEntry(owner)
		if err != nil || !enters {
			t.Fatalf("entry = %#v enters=%v error=%v", entry, enters, err)
		}
		if entry.OriginRunID != "" {
			t.Fatal("fresh local lifecycle entry inherited a fork origin")
		}
		bookkeeping := map[string]any{}
		if err := StoreStageEntry(bookkeeping, entry); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(bookkeeping)
		var restored map[string]any
		if err := json.Unmarshal(raw, &restored); err != nil {
			t.Fatal(err)
		}
		decoded, found, err := LoadStageEntry(restored)
		if err != nil || !found || decoded != entry {
			t.Fatalf("entry roundtrip = %#v found=%v error=%v", decoded, found, err)
		}
		return entry.Key()
	}
	first := makeEntry("event-1", "delivery-1", now)
	if first != makeEntry("event-1", "delivery-1", now.Add(time.Hour)) {
		t.Fatal("clock changed the admitted entry identity")
	}
	if first == makeEntry("event-2", "delivery-2", now) || first == makeEntry("event-1", "delivery-3", now) {
		t.Fatal("distinct accepted occurrences reused one stage entry")
	}
}

func TestA2StageEntryRejectsMissingOrForeignOccurrence(t *testing.T) {
	owner, _ := flowidentity.NewRunScopedFlowInstance("run-1", flowidentity.RouteForInstancePath("flow/one"))
	cause, err := NewCompiledTransition(effectTestCompiledTransition(t), handlerselection.NotApplicable(), nil)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := NewAcceptedEvent(owner.Route, identity.NormalizeEntityID("entity-1"), "event-1", "review.approved", executionmode.Live, time.Now().UTC(), &cause)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := effect.StageEntry(owner); err == nil {
		t.Fatal("semantic transition identity was accepted without execution occurrence")
	}
	effect, _ = effect.WithExecutionOccurrence("delivery", "delivery-1")
	foreign, _ := flowidentity.NewRunScopedFlowInstance("run-1", flowidentity.RouteForInstancePath("flow/two"))
	if _, _, err := effect.StageEntry(foreign); err == nil {
		t.Fatal("foreign instance supplied lifecycle entry authority")
	}
	if _, _, err := LoadStageEntry(map[string]any{"stage_entry": nil}); err == nil {
		t.Fatal("present null entry was treated as lawful absence")
	}
}
