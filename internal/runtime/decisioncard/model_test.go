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
	if !first.EffectiveCadence.FirstReminderAt.Equal(now.Add(DefaultFirstReminder)) || !first.EffectiveCadence.UrgencyAt.Equal(now.Add(DefaultUrgency)) {
		t.Fatalf("effective cadence deadlines = %#v, want frozen platform defaults", first.EffectiveCadence)
	}
	if first.CardContentHash == "" || first.DecisionSchemaHash == "" || first.CardContentHash != second.CardContentHash || first.DecisionSchemaHash != second.DecisionSchemaHash {
		t.Fatalf("hashes are not stable: first=%q/%q second=%q/%q", first.CardContentHash, first.DecisionSchemaHash, second.CardContentHash, second.DecisionSchemaHash)
	}
}

func TestCadencePolicyStampsOperatorOverrides(t *testing.T) {
	now := time.Date(2026, time.July, 12, 10, 0, 0, 0, time.UTC)
	got := (CadencePolicy{FirstReminderDelay: time.Hour, UrgencyDelay: 6 * time.Hour, InputDraftTTL: 5 * time.Minute, ReminderInterval: 2 * time.Hour}).Stamp(now)
	if !got.FirstReminderAt.Equal(now.Add(time.Hour)) || !got.UrgencyAt.Equal(now.Add(6*time.Hour)) || got.InputDraftTTL != "5m0s" || got.ReminderInterval != "2h0m0s" {
		t.Fatalf("operator cadence stamp = %#v", got)
	}
}

func TestValidateNoticeShapeReservesDecisionCardAuthority(t *testing.T) {
	for _, tc := range []struct {
		itemType string
		payload  map[string]any
	}{
		{itemType: "decision-card"},
		{itemType: "notice", payload: map[string]any{"card_id": "forged"}},
		{itemType: "notice", payload: map[string]any{"verdict": "approve"}},
	} {
		if err := ValidateNoticeShape(tc.itemType, tc.payload); err == nil {
			t.Fatalf("ValidateNoticeShape(%q, %#v) accepted decision-shaped notice", tc.itemType, tc.payload)
		}
	}
	if err := ValidateNoticeShape("review_request", map[string]any{"summary": "review this"}); err != nil {
		t.Fatalf("ordinary notice rejected: %v", err)
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
