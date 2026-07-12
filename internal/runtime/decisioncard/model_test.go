package decisioncard

import (
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/google/uuid"
)

func TestNewStampsDefaultCadenceAndStableHashes(t *testing.T) {
	now := time.Date(2026, time.July, 12, 10, 0, 0, 0, time.UTC)
	input := Card{
		CardID: uuid.NewString(), RunID: "run-1", FlowInstance: "root", EntityID: "entity-1",
		Stage: "awaiting_review", StageActivationID: uuid.NewString(), DecisionID: "launch_review",
		BundleHash: "bundle-hash", WorkflowVersion: "1", CreatedAt: now,
		Snapshot: Snapshot{Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "operating"},
		}},
	}
	first, err := New(input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(input)
	if err != nil {
		t.Fatal(err)
	}
	if first.EffectiveCadence.InputDraftTTL != DefaultInputDraftTTL.String() || first.EffectiveCadence.ReminderInterval != DefaultReminderInterval.String() {
		t.Fatalf("effective cadence = %#v, want platform defaults", first.EffectiveCadence)
	}
	if first.CardContentHash == "" || first.DecisionSchemaHash == "" || first.CardContentHash != second.CardContentHash || first.DecisionSchemaHash != second.DecisionSchemaHash {
		t.Fatalf("hashes are not stable: first=%q/%q second=%q/%q", first.CardContentHash, first.DecisionSchemaHash, second.CardContentHash, second.DecisionSchemaHash)
	}
}

func TestNewRejectsInputDraftTTLBeyondReminderInterval(t *testing.T) {
	_, err := New(Card{
		CardID: uuid.NewString(), RunID: "run-1", EntityID: "entity-1", Stage: "awaiting_review",
		StageActivationID: uuid.NewString(), DecisionID: "launch_review", BundleHash: "bundle-hash",
		EffectiveCadence: Cadence{InputDraftTTL: "25h", ReminderInterval: "24h"},
		Snapshot: Snapshot{Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "operating"},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "TTL exceeds reminder interval") {
		t.Fatalf("New error = %v, want draft TTL constraint", err)
	}
}
