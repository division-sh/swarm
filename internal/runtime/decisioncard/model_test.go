package decisioncard

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func TestPublicJSONOmitsUnsetOptionalTimestampsAndIncludesTransitions(t *testing.T) {
	now := time.Date(2026, time.July, 13, 1, 2, 3, 4, time.UTC)
	card, err := New(baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", AdvancesTo: "operating"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	card.CreatedAt = now
	card.UpdatedAt = now
	scope, err := card.Anchor.Scope()
	if err != nil {
		t.Fatal(err)
	}
	listItem := ListItem{
		Kind: KindDecisionCard, CardID: card.CardID, RunID: card.RunID,
		Anchor: card.Anchor, Scope: scope, Status: card.Status,
		CreatedAt: now, UpdatedAt: now,
	}
	for name, value := range map[string]any{
		"detail": card,
		"list":   listItem,
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

	card.DecidedAt = now
	card.DeferredUntil = now.Add(time.Hour)
	detailRaw, err := json.Marshal(card)
	if err != nil {
		t.Fatal(err)
	}
	listItem.DeferredUntil = now.Add(time.Hour)
	listRaw, err := json.Marshal(listItem)
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
		CardID: uuid.NewString(), RunID: "run-1", Anchor: testStageAnchor(),
		BundleHash: "bundle-hash", WorkflowVersion: "1", CreatedAt: now,
		Snapshot: testSnapshot(t, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "operating"},
		}, nil),
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
		{itemType: "human_task"},
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
		CardID: uuid.NewString(), RunID: "run-1", Anchor: testStageAnchor(), BundleHash: "bundle-hash",
		EffectiveCadence: Cadence{InputDraftTTL: "25h", ReminderInterval: "24h"},
		Snapshot: testSnapshot(t, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "operating"},
		}, nil),
	})
	if err == nil || !strings.Contains(err.Error(), "TTL exceeds reminder interval") {
		t.Fatalf("New error = %v, want draft TTL constraint", err)
	}
}

func TestNewRejectsNonCanonicalGateInputTypeInSnapshot(t *testing.T) {
	_, err := New(Card{
		CardID: uuid.NewString(), RunID: "run-1", Anchor: testStageAnchor(), BundleHash: "bundle-hash",
		Snapshot: testSnapshot(t, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"reject": {
				AdvancesTo: "building",
				Input: map[string]runtimecontracts.WorkflowGateInputField{
					"feedback": {Type: "string", Required: true},
				},
			},
		}, nil),
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported stage gate input type") {
		t.Fatalf("New error = %v, want noncanonical gate input rejection", err)
	}
}

func TestNewRejectsNonExactCanonicalGateInputTypeBeforeHashing(t *testing.T) {
	card, err := New(Card{
		CardID: uuid.NewString(), RunID: "run-1", Anchor: testStageAnchor(), BundleHash: "bundle-hash",
		Snapshot: testSnapshot(t, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"reject": {
				AdvancesTo: "building",
				Input: map[string]runtimecontracts.WorkflowGateInputField{
					"feedback": {Type: " TEXT ", Required: true},
				},
			},
		}, nil),
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
	input.Snapshot.Decision = " launch_review "
	card, err := New(input)
	if err == nil || !strings.Contains(err.Error(), "decision identity") || !strings.Contains(err.Error(), "not canonical") {
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
	valid.Snapshot.Decision = " launch_review "
	if err := valid.Validate(); err == nil || !strings.Contains(err.Error(), "decision identity") || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("persisted-card validation error = %v, want exact top-level identity rejection", err)
	}
}

func TestDecisionSchemaHashTracksEffectiveAcceptanceAndIgnoresPresentation(t *testing.T) {
	newCard := func(title, outcomeLabel, inputLabel, pattern string) Card {
		t.Helper()
		schema := textEventSchema(map[string]map[string]any{
			"code": {"type": "string", "pattern": pattern, "description": inputLabel + " help"},
		}, []string{"code"})
		schema["title"] = outcomeLabel + " result"
		input := baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {
				Verdict: "approve", Label: outcomeLabel, AdvancesTo: "operating",
				Input: map[string]runtimecontracts.WorkflowGateInputField{
					"code": {Type: "text", Required: true, Label: inputLabel},
				},
				Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
					"code": runtimecontracts.CELExpression("decision.code"),
				}},
				EmitSchema: schema,
			},
		})
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

func TestDecisionSchemaHashSplitsSafeNumericBounds(t *testing.T) {
	newCard := func(minimum int64) Card {
		t.Helper()
		card, err := New(baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {
				Verdict: "approve", AdvancesTo: "operating",
				Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
					"score": runtimecontracts.LiteralExpression(minimum),
				}},
				EmitSchema: textEventSchema(map[string]map[string]any{
					"score": {"type": "integer", "minimum": minimum},
				}, []string{"score"}),
			},
		}))
		if err != nil {
			t.Fatal(err)
		}
		return card
	}
	lower := newCard(9007199254740990)
	upper := newCard(9007199254740991)
	if lower.DecisionSchemaHash == upper.DecisionSchemaHash {
		t.Fatalf("distinct safe numeric bounds share schema hash %q", lower.DecisionSchemaHash)
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

func TestValidateRequiresObjectShapedSemanticCarriers(t *testing.T) {
	valid, err := New(baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", AdvancesTo: "operating"},
	}))
	if err != nil {
		t.Fatal(err)
	}

	badProvenance := valid
	badProvenance.Provenance = semanticvalue.MustString("not-an-object")
	if err := badProvenance.Validate(); err == nil || !strings.Contains(err.Error(), "provenance must be an object") {
		t.Fatalf("scalar provenance validation error = %v", err)
	}

	badFields := valid
	badFields.Fields = semanticvalue.MustString("not-an-object")
	if err := badFields.Validate(); err == nil || !strings.Contains(err.Error(), "fields must be an object") {
		t.Fatalf("scalar fields validation error = %v", err)
	}
}

func TestSnapshotDecodePreservesSafeNumericHashIdentity(t *testing.T) {
	const safeInteger = int64(9007199254740991)
	card, err := New(baseTestDecisionCard(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {
			Verdict: "approve", AdvancesTo: "operating",
			Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
				"large_integer": runtimecontracts.LiteralExpression(safeInteger),
			}},
			EmitSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"large_integer": map[string]any{"type": "integer", "minimum": safeInteger},
				},
				"required": []string{"large_integer"}, "additionalProperties": false,
			},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	card.Snapshot.Context, err = canonicaljson.FromGo(map[string]any{"large_integer": safeInteger})
	if err != nil {
		t.Fatal(err)
	}
	card, err = New(card)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := SnapshotJSON(card)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	assertSafeSnapshotNumbers(t, decoded, float64(safeInteger))

	roundTripped := card
	roundTripped.Snapshot = decoded
	if err := roundTripped.Validate(); err != nil {
		t.Fatalf("round-tripped card validation: %v", err)
	}
}

func TestDecodeSnapshotRejectsStructuralSemanticDriftAtEveryTypedLevel(t *testing.T) {
	snapshot, err := FreezeSnapshot("launch_review", "", map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"revise": {
			Verdict: "revise", AdvancesTo: "building",
			Input: map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "text", Required: true}},
			Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
				"feedback": runtimecontracts.CELExpression("decision.feedback"),
			}},
			EmitSchema: textEventSchema(map[string]map[string]any{"feedback": {"type": "string"}}, []string{"feedback"}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := SnapshotJSON(Card{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range decisionSnapshotStructuralDriftCases() {
		t.Run(test.name, func(t *testing.T) {
			corrupted := mutateDecisionSnapshotJSON(t, raw, test.mutate)
			if _, err := DecodeSnapshot(corrupted); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("DecodeSnapshot error = %v, want %q", err, test.want)
			}
		})
	}
}

func assertSafeSnapshotNumbers(t *testing.T, snapshot Snapshot, want float64) {
	t.Helper()
	assertNumber := func(name string, value any) {
		t.Helper()
		number, ok := value.(float64)
		if !ok || number != want {
			t.Fatalf("%s = %#v, want float64(%v)", name, value, want)
		}
	}
	contextValue, ok := snapshot.Context.Lookup("large_integer")
	if !ok {
		t.Fatal("context.large_integer is absent")
	}
	assertNumber("context.large_integer", contextValue.Interface())
	outcome := snapshot.Outcomes["approve"]
	assertNumber("outcome literal", outcome.Emit.Fields["large_integer"].Literal.Interface())
	schema, ok := outcome.EmitSchema.Interface().(map[string]any)
	if !ok {
		t.Fatalf("emit schema = %#v", outcome.EmitSchema.Interface())
	}
	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("emit schema properties = %#v", schema["properties"])
	}
	property, ok := properties["large_integer"].(map[string]any)
	if !ok {
		t.Fatalf("large_integer schema = %#v", properties["large_integer"])
	}
	assertNumber("schema minimum", property["minimum"])
}

func TestNewRejectsUnsupportedSnapshotNumbersBeforeHashing(t *testing.T) {
	const unsafeInteger = int64(9007199254740992)
	for _, tc := range []struct {
		name     string
		context  map[string]any
		outcomes map[string]runtimecontracts.WorkflowGateOutcomePlan
	}{
		{name: "context", context: map[string]any{"unsafe": unsafeInteger}, outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "operating"}}},
		{name: "outcome literal", outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {
			Verdict: "approve", AdvancesTo: "operating",
			Emit:       runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{"value": runtimecontracts.LiteralExpression(unsafeInteger)}},
			EmitSchema: textEventSchema(map[string]map[string]any{"value": {"type": "integer"}}, []string{"value"}),
		}}},
		{name: "schema bound", outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {
			Verdict: "approve", AdvancesTo: "operating",
			Emit:       runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{"value": runtimecontracts.LiteralExpression(1)}},
			EmitSchema: textEventSchema(map[string]map[string]any{"value": {"type": "integer", "minimum": unsafeInteger}}, []string{"value"}),
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := FreezeSnapshot("launch_review", "", tc.context, tc.outcomes); err == nil {
				t.Fatal("FreezeSnapshot accepted an unsupported number")
			}
		})
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
	if err := ValidateDecision(card, "approve", semanticObject(map[string]any{"code": "NOT-LOWER"})); err == nil || !strings.Contains(err.Error(), "pattern") {
		t.Fatalf("ValidateDecision error = %v, want frozen pattern rejection", err)
	}
	if err := ValidateDecision(card, "approve", semanticObject(map[string]any{"code": "ready"})); err != nil {
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
	if err := ValidateDecision(dynamic, "approve", semanticObject(map[string]any{"component": "api", "owner": "worker"})); err == nil || !strings.Contains(err.Error(), "must equal") {
		t.Fatalf("dynamic relational decision error = %v, want pre-settlement rejection", err)
	}
}

func baseTestDecisionCard(outcomes map[string]runtimecontracts.WorkflowGateOutcomePlan) Card {
	snapshot, err := FreezeSnapshot("launch_review", "", nil, outcomes)
	if err != nil {
		panic(err)
	}
	return Card{
		CardID: uuid.NewString(), RunID: "run-1", Anchor: testStageAnchor(),
		BundleHash: "bundle-hash", WorkflowVersion: "1", CreatedAt: time.Date(2026, time.July, 13, 5, 0, 0, 0, time.UTC),
		Snapshot: snapshot,
	}
}

func testStageAnchor() Anchor {
	anchor, err := NewStageGateAnchor(StageGateAnchor{
		FlowInstance: "root", EntityID: "entity-1", Stage: "awaiting_review",
		StageActivationID: uuid.NewString(),
	})
	if err != nil {
		panic(err)
	}
	return anchor
}

func testSnapshot(t *testing.T, outcomes map[string]runtimecontracts.WorkflowGateOutcomePlan, context map[string]any) Snapshot {
	t.Helper()
	snapshot, err := FreezeSnapshot("launch_review", "", context, outcomes)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func semanticObject(values map[string]any) semanticvalue.Value {
	value, err := canonicaljson.FromGo(values)
	if err != nil {
		panic(err)
	}
	return value
}

func textEventSchema(properties map[string]map[string]any, required []string) map[string]any {
	rawProperties := make(map[string]any, len(properties))
	for name, schema := range properties {
		rawProperties[name] = schema
	}
	return map[string]any{"type": "object", "properties": rawProperties, "required": required, "additionalProperties": false}
}

type decisionSnapshotStructuralDriftCase struct {
	name   string
	want   string
	mutate func(*testing.T, map[string]any)
}

func decisionSnapshotStructuralDriftCases() []decisionSnapshotStructuralDriftCase {
	unexpected := "non-canonical semantic structure"
	return []decisionSnapshotStructuralDriftCase{
		{name: "root_shadow", want: unexpected, mutate: func(_ *testing.T, root map[string]any) { root["shadow_semantics"] = true }},
		{name: "root_missing", want: unexpected, mutate: func(_ *testing.T, root map[string]any) { delete(root, "title") }},
		{name: "outcome_shadow", want: unexpected, mutate: func(t *testing.T, root map[string]any) {
			decisionSnapshotNestedObject(t, root, "outcomes", "revise")["shadow_semantics"] = true
		}},
		{name: "outcome_missing", want: unexpected, mutate: func(t *testing.T, root map[string]any) {
			delete(decisionSnapshotNestedObject(t, root, "outcomes", "revise"), "Label")
		}},
		{name: "input_shadow", want: unexpected, mutate: func(t *testing.T, root map[string]any) {
			decisionSnapshotNestedObject(t, root, "outcomes", "revise", "Input", "feedback")["shadow_semantics"] = true
		}},
		{name: "input_missing", want: unexpected, mutate: func(t *testing.T, root map[string]any) {
			delete(decisionSnapshotNestedObject(t, root, "outcomes", "revise", "Input", "feedback"), "label")
		}},
		{name: "emit_shadow", want: unexpected, mutate: func(t *testing.T, root map[string]any) {
			decisionSnapshotNestedObject(t, root, "outcomes", "revise", "Emit")["shadow_semantics"] = true
		}},
		{name: "emit_missing", want: unexpected, mutate: func(t *testing.T, root map[string]any) {
			delete(decisionSnapshotNestedObject(t, root, "outcomes", "revise", "Emit"), "Event")
		}},
		{name: "expression_shadow", want: unexpected, mutate: func(t *testing.T, root map[string]any) {
			decisionSnapshotNestedObject(t, root, "outcomes", "revise", "Emit", "Fields", "feedback")["shadow_semantics"] = true
		}},
		{name: "expression_missing", want: unexpected, mutate: func(t *testing.T, root map[string]any) {
			delete(decisionSnapshotNestedObject(t, root, "outcomes", "revise", "Emit", "Fields", "feedback"), "Ref")
		}},
		{name: "missing_emit_schema", want: unexpected, mutate: func(t *testing.T, root map[string]any) {
			delete(decisionSnapshotNestedObject(t, root, "outcomes", "revise"), "EmitSchema")
		}},
		{name: "missing_literal", want: unexpected, mutate: func(t *testing.T, root map[string]any) {
			delete(decisionSnapshotNestedObject(t, root, "outcomes", "revise", "Emit", "Fields", "feedback"), "Literal")
		}},
		{name: "ignored_nonliteral_value", want: "does not preserve its exact semantic value", mutate: func(t *testing.T, root map[string]any) {
			decisionSnapshotNestedObject(t, root, "outcomes", "revise", "Emit", "Fields", "feedback")["Literal"] = "shadow"
		}},
	}
}

func mutateDecisionSnapshotJSON(t *testing.T, raw []byte, mutate func(*testing.T, map[string]any)) []byte {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatal(err)
	}
	mutate(t, root)
	corrupted, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	return corrupted
}

func decisionSnapshotNestedObject(t *testing.T, root map[string]any, path ...string) map[string]any {
	t.Helper()
	current := root
	for _, name := range path {
		next, ok := current[name].(map[string]any)
		if !ok {
			t.Fatalf("snapshot path %s is %#v, want object", strings.Join(path, "."), current[name])
		}
		current = next
	}
	return current
}
