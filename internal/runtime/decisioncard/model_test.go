package decisioncard

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/google/uuid"
)

func TestPublicJSONOmitsUnsetOptionalTimestampsAndIncludesTransitions(t *testing.T) {
	now := time.Date(2026, time.July, 13, 1, 2, 3, 4, time.UTC)
	for name, value := range map[string]any{
		"detail": Card{CardID: "card-1", CreatedAt: now, UpdatedAt: now},
		"list":   ListItem{CardID: "card-1", CreatedAt: now, UpdatedAt: now},
	} {
		t.Run(name+"/unset", func(t *testing.T) {
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if _, ok := got["deferred_until"]; ok {
				t.Fatalf("unset deferred_until was serialized: %s", raw)
			}
			if name == "detail" {
				if _, ok := got["decided_at"]; ok {
					t.Fatalf("unset decided_at was serialized: %s", raw)
				}
			}
		})
	}

	detailRaw, err := json.Marshal(Card{CardID: "card-1", DecidedAt: now, DeferredUntil: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	listRaw, err := json.Marshal(ListItem{CardID: "card-1", DeferredUntil: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]string{"decided_at": now.Format(time.RFC3339Nano), "deferred_until": now.Add(time.Hour).Format(time.RFC3339Nano)} {
		if !strings.Contains(string(detailRaw), `"`+field+`":"`+want+`"`) {
			t.Fatalf("detail %s = %s, want %s", field, detailRaw, want)
		}
	}
	if want := now.Add(time.Hour).Format(time.RFC3339Nano); !strings.Contains(string(listRaw), `"deferred_until":"`+want+`"`) {
		t.Fatalf("list deferred_until = %s, want %s", listRaw, want)
	}
}

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

func TestNewRejectsNonCanonicalGateInputTypeInSnapshot(t *testing.T) {
	_, err := New(Card{
		CardID: uuid.NewString(), RunID: "run-1", EntityID: "entity-1", Stage: "awaiting_review",
		StageActivationID: uuid.NewString(), DecisionID: "launch_review", BundleHash: "bundle-hash",
		Snapshot: Snapshot{Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"reject": {
				AdvancesTo: "building",
				Input: map[string]runtimecontracts.WorkflowGateInputField{
					"feedback": {Type: "string", Required: true},
				},
			},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported stage gate input type") {
		t.Fatalf("New error = %v, want noncanonical gate input rejection", err)
	}
}

func TestNewRejectsNonExactCanonicalGateInputTypeBeforeHashing(t *testing.T) {
	card, err := New(Card{
		CardID: uuid.NewString(), RunID: "run-1", EntityID: "entity-1", Stage: "awaiting_review",
		StageActivationID: uuid.NewString(), DecisionID: "launch_review", BundleHash: "bundle-hash",
		Snapshot: Snapshot{Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"reject": {
				AdvancesTo: "building",
				Input: map[string]runtimecontracts.WorkflowGateInputField{
					"feedback": {Type: " TEXT ", Required: true},
				},
			},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "is not canonical") {
		t.Fatalf("New error = %v, want exact canonical-spelling rejection", err)
	}
	if card.CardContentHash != "" || card.DecisionSchemaHash != "" {
		t.Fatalf("noncanonical card was hashed before rejection: %#v", card)
	}
}

func TestNewAndValidateRejectNonCanonicalTopLevelDecisionID(t *testing.T) {
	input := baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", AdvancesTo: "operating"},
	})
	input.DecisionID = " launch_review "
	card, err := New(input)
	if err == nil || !strings.Contains(err.Error(), "decision_id") || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("New error = %v, want noncanonical top-level decision identity rejection", err)
	}
	if card.CardContentHash != "" || card.DecisionSchemaHash != "" {
		t.Fatalf("noncanonical decision identity was hashed: %#v", card)
	}

	valid, err := New(baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", AdvancesTo: "operating"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	valid.DecisionID = " launch_review "
	if err := valid.Validate(); err == nil || !strings.Contains(err.Error(), "decision_id") || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("persisted-card validation error = %v, want exact top-level identity rejection", err)
	}
}

func TestDecisionSchemaHashTracksEffectiveAcceptanceAndIgnoresPresentation(t *testing.T) {
	newCard := func(title, outcomeLabel, inputLabel, pattern string) Card {
		t.Helper()
		input := baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {
				Verdict: "approve", Label: outcomeLabel, AdvancesTo: "operating",
				Input: map[string]runtimecontracts.WorkflowGateInputField{
					"code": {Type: "text", Required: true, Label: inputLabel},
				},
				Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
					"code": runtimecontracts.CELExpression("decision.code"),
				}},
				EmitSchema: textEventSchema(map[string]map[string]any{
					"code": {"type": "string", "pattern": pattern, "description": inputLabel + " help"},
				}, []string{"code"}),
			},
		})
		input.Snapshot.Outcomes["approve"].EmitSchema["title"] = outcomeLabel + " result"
		input.Snapshot.Title = title
		card, err := New(input)
		if err != nil {
			t.Fatal(err)
		}
		return card
	}

	lowercase := newCard("Launch review", "Approve", "Code", "^[a-z]+$")
	uppercase := newCard("Launch review", "Approve", "Code", "^[A-Z]+$")
	if lowercase.DecisionSchemaHash == uppercase.DecisionSchemaHash {
		t.Fatalf("acceptance-changing patterns share schema hash %q", lowercase.DecisionSchemaHash)
	}

	polished := newCard("Production launch review", "Ship it", "Approval code", "^[a-z]+$")
	if lowercase.DecisionSchemaHash != polished.DecisionSchemaHash {
		t.Fatalf("presentation-only edits changed schema hash: %q != %q", lowercase.DecisionSchemaHash, polished.DecisionSchemaHash)
	}
	if lowercase.CardContentHash == polished.CardContentHash {
		t.Fatal("presentation-only edits did not change card content hash")
	}
}

func TestValidateRecomputesImmutableSnapshotHashes(t *testing.T) {
	valid, err := New(baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"revise": {
			Verdict: "revise", AdvancesTo: "building",
			Input: map[string]runtimecontracts.WorkflowGateInputField{
				"feedback": {Type: "text", Required: true},
			},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	staleContent := valid
	staleContent.CardContentHash = "sha256:stale-content"
	if err := staleContent.Validate(); err == nil || !strings.Contains(err.Error(), "content hash does not match") {
		t.Fatalf("stale content hash validation error = %v", err)
	}

	staleSchema := valid
	staleSchema.DecisionSchemaHash = "sha256:stale-schema"
	if err := staleSchema.Validate(); err == nil || !strings.Contains(err.Error(), "schema hash does not match") {
		t.Fatalf("stale schema hash validation error = %v", err)
	}

	changedSnapshot := valid
	outcome := changedSnapshot.Snapshot.Outcomes["revise"]
	outcome.Input["feedback"] = runtimecontracts.WorkflowGateInputField{Type: "text", Required: false}
	changedSnapshot.Snapshot.Outcomes["revise"] = outcome
	if err := changedSnapshot.Validate(); err == nil || !strings.Contains(err.Error(), "content hash does not match") {
		t.Fatalf("changed snapshot under stale hashes validation error = %v", err)
	}
}

func TestNewRejectsNonCanonicalGateMapIdentityBeforeHashing(t *testing.T) {
	tests := []struct {
		name     string
		outcomes map[string]runtimecontracts.WorkflowGateOutcomePlan
	}{
		{name: "verdict", outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{" approve ": {AdvancesTo: "operating"}}},
		{name: "input", outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {
			AdvancesTo: "operating", Input: map[string]runtimecontracts.WorkflowGateInputField{" note ": {Type: "text"}},
		}}},
		{name: "emit", outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {
			AdvancesTo: "operating",
			Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
				" note ": runtimecontracts.LiteralExpression("ready"),
			}},
			EmitSchema: textEventSchema(map[string]map[string]any{"note": {"type": "string"}}, []string{"note"}),
		}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			card, err := New(baseTestDecisionCard(tc.outcomes))
			if err == nil || !strings.Contains(err.Error(), "is not canonical") {
				t.Fatalf("New error = %v, want canonical map-identity rejection", err)
			}
			if card.CardContentHash != "" || card.DecisionSchemaHash != "" {
				t.Fatalf("noncanonical card was hashed before rejection: %#v", card)
			}
		})
	}
}

func TestValidateDecisionConsumesFrozenEmitSchemaBeforeSettlement(t *testing.T) {
	patternSchema := textEventSchema(map[string]map[string]any{
		"code": {"type": "string", "pattern": "^[a-z]+$"},
	}, []string{"code"})
	card, err := New(baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {
			Verdict: "approve", AdvancesTo: "operating",
			Input: map[string]runtimecontracts.WorkflowGateInputField{"code": {Type: "text", Required: true}},
			Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
				"code": runtimecontracts.CELExpression("decision.code"),
			}},
			EmitSchema: patternSchema,
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateDecision(card, "approve", map[string]any{"code": "NOT-LOWER"}); err == nil || !strings.Contains(err.Error(), "pattern") {
		t.Fatalf("ValidateDecision error = %v, want frozen pattern rejection", err)
	}
	if err := ValidateDecision(card, "approve", map[string]any{"code": "ready"}); err != nil {
		t.Fatalf("valid refined decision rejected: %v", err)
	}
}

func TestNewRejectsOptionalEmittedDecisionInputBeforeHashing(t *testing.T) {
	card, err := New(baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {
			Verdict: "approve", AdvancesTo: "operating",
			Input: map[string]runtimecontracts.WorkflowGateInputField{"note": {Type: "text", Required: false}},
			Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
				"note": runtimecontracts.CELExpression("decision.note"),
			}},
			EmitSchema: textEventSchema(map[string]map[string]any{"note": {"type": "string"}}, []string{"note"}),
		},
	}))
	if err == nil || !strings.Contains(err.Error(), "reads optional decision.note") {
		t.Fatalf("New error = %v, want optional emitted input rejection", err)
	}
	if card.CardContentHash != "" || card.DecisionSchemaHash != "" {
		t.Fatalf("invalid optional-input card was hashed: %#v", card)
	}
}

func TestNewAndValidateDecisionConsumeRelationalEmitSchema(t *testing.T) {
	relational := textEventSchema(map[string]map[string]any{
		"component": {"type": "string"},
		"owner":     {"type": "string", "x-swarm-equalTo": "component"},
	}, []string{"component", "owner"})
	staticCard, staticErr := New(baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {
			Verdict: "approve", AdvancesTo: "operating",
			Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
				"component": runtimecontracts.LiteralExpression("api"),
				"owner":     runtimecontracts.LiteralExpression("worker"),
			}},
			EmitSchema: relational,
		},
	}))
	if staticErr == nil || !strings.Contains(staticErr.Error(), "must equal") || staticCard.CardContentHash != "" {
		t.Fatalf("static relational card = %#v, error = %v, want pre-hash rejection", staticCard, staticErr)
	}

	dynamic, err := New(baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {
			Verdict: "approve", AdvancesTo: "operating",
			Input: map[string]runtimecontracts.WorkflowGateInputField{
				"component": {Type: "text", Required: true},
				"owner":     {Type: "text", Required: true},
			},
			Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
				"component": runtimecontracts.CELExpression("decision.component"),
				"owner":     runtimecontracts.CELExpression("decision.owner"),
			}},
			EmitSchema: relational,
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateDecision(dynamic, "approve", map[string]any{"component": "api", "owner": "worker"}); err == nil || !strings.Contains(err.Error(), "must equal") {
		t.Fatalf("dynamic relational decision error = %v, want pre-settlement rejection", err)
	}
}

func baseTestDecisionCard(outcomes map[string]runtimecontracts.WorkflowGateOutcomePlan) Card {
	return Card{
		CardID: uuid.NewString(), RunID: "run-1", FlowInstance: "root", EntityID: "entity-1",
		Stage: "awaiting_review", StageActivationID: uuid.NewString(), DecisionID: "launch_review",
		BundleHash: "bundle-hash", WorkflowVersion: "1", CreatedAt: time.Date(2026, time.July, 13, 5, 0, 0, 0, time.UTC),
		Snapshot: Snapshot{Outcomes: outcomes},
	}
}

func textEventSchema(properties map[string]map[string]any, required []string) map[string]any {
	rawProperties := make(map[string]any, len(properties))
	for name, schema := range properties {
		rawProperties[name] = schema
	}
	return map[string]any{"type": "object", "properties": rawProperties, "required": required, "additionalProperties": false}
}
