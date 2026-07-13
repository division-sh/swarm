package contracts

import (
	"strings"
	"testing"

	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func TestLowerEmitSpecFieldsLowersFromAndBareNamespaceValues(t *testing.T) {
	bundle := emitFieldLoweringTestBundle()
	spec := EmitSpec{
		Event: "account.bucketed",
		From:  "entity",
		Fields: map[string]ExpressionValue{
			"interest_score": CELExpression("payload"),
			"tier":           CELExpression("payload.computed_tier"),
		},
	}

	lowered, err := bundle.LowerEmitSpecFields(EmitFieldLoweringContext{
		NodeID:           "bucket-node",
		TriggerEventType: "account.scored",
		Site:             "handler.emit",
	}, spec)
	if err != nil {
		t.Fatalf("LowerEmitSpecFields error: %v", err)
	}

	if lowered.From != "" {
		t.Fatalf("lowered From = %q, want empty canonical spec", lowered.From)
	}
	assertEmitCEL(t, lowered.Fields, "account_id", "entity.account_id")
	assertEmitCEL(t, lowered.Fields, "bucket", "entity.bucket")
	assertEmitCEL(t, lowered.Fields, "interest_score", "payload.interest_score")
	assertEmitCEL(t, lowered.Fields, "tier", "payload.computed_tier")
}

func TestLowerEmitSpecFieldsExplicitFieldsWinAndOptionalsRemainExplicit(t *testing.T) {
	bundle := emitFieldLoweringTestBundle()
	spec := EmitSpec{
		Event: "account.bucketed",
		From:  "entity",
		Fields: map[string]ExpressionValue{
			"bucket":         CELExpression(`"manual"`),
			"interest_score": CELExpression("payload"),
		},
	}

	lowered, err := bundle.LowerEmitSpecFields(EmitFieldLoweringContext{
		NodeID:           "bucket-node",
		TriggerEventType: "account.scored",
		Site:             "handler.emit",
	}, spec)
	if err != nil {
		t.Fatalf("LowerEmitSpecFields error: %v", err)
	}

	assertEmitCEL(t, lowered.Fields, "account_id", "entity.account_id")
	assertEmitCEL(t, lowered.Fields, "bucket", `"manual"`)
	if _, ok := lowered.Fields["tier"]; ok {
		t.Fatalf("optional tier was auto-filled by emit.from: %#v", lowered.Fields)
	}
}

func TestLowerEmitSpecFieldsFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		spec EmitSpec
		want string
	}{
		{
			name: "missing source field",
			spec: EmitSpec{Event: "account.bucketed", From: "payload"},
			want: "source payload does not declare it",
		},
		{
			name: "undeclared output field",
			spec: EmitSpec{
				Event: "account.bucketed",
				From:  "entity",
				Fields: map[string]ExpressionValue{
					"extra": CELExpression("payload.interest_score"),
				},
			},
			want: "emit.fields.extra is not declared",
		},
		{
			name: "bare namespace missing same named field",
			spec: EmitSpec{
				Event: "account.bucketed",
				Fields: map[string]ExpressionValue{
					"tier": CELExpression("payload"),
				},
			},
			want: "source payload does not declare same-named field tier",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := emitFieldLoweringTestBundle().LowerEmitSpecFields(EmitFieldLoweringContext{
				NodeID:           "bucket-node",
				TriggerEventType: "account.scored",
				Site:             "handler.emit",
			}, tc.spec)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LowerEmitSpecFields error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestEmitFromSugarDoesNotBleedIntoTargetMatch(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/contracts/workflow_contract_emit_lowering_test.go:TestEmitFromSugarDoesNotBleedIntoTargetMatch"))
	var spec EmitSpec
	err := yaml.Unmarshal([]byte(`
event: account.bucketed
from: entity
target:
  flow: account
  match:
    account_id: payload
fields:
  interest_score: payload
`), &spec)
	if err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}

	lowered, err := emitFieldLoweringTestBundle().LowerEmitSpecFields(EmitFieldLoweringContext{
		NodeID:           "bucket-node",
		TriggerEventType: "account.scored",
		Site:             "handler.emit",
	}, spec)
	if err != nil {
		t.Fatalf("LowerEmitSpecFields error: %v", err)
	}

	assertEmitCEL(t, lowered.Fields, "interest_score", "payload.interest_score")
	assertEmitCEL(t, lowered.Target.Match, "account_id", "payload")
}

func TestMailboxPayloadDoesNotAcceptEmitFromSugar(t *testing.T) {
	var spec MailboxWriteSpec
	err := yaml.Unmarshal([]byte(`
from: entity
item_type:
  literal: review
severity:
  literal: info
summary:
  literal: ready
payload:
  interest_score: payload
`), &spec)
	if err == nil || !strings.Contains(err.Error(), `mailbox field "from" is not supported.`) {
		t.Fatalf("yaml.Unmarshal error = %v, want mailbox from-field rejection", err)
	}
}

func emitFieldLoweringTestBundle() *WorkflowContractBundle {
	return &WorkflowContractBundle{
		RootEntities: EntityContractsDocument{
			"account": {
				Fields: map[string]EntityFieldDecl{
					"account_id": {Type: "string"},
					"bucket":     {Type: "string"},
				},
			},
		},
		Events: map[string]EventCatalogEntry{
			"account.scored": {
				Payload: EventPayloadSpec{
					Properties: map[string]EventFieldSpec{
						"account_id":      {Type: "string"},
						"interest_score":  {Type: "number"},
						"computed_tier":   {Type: "string"},
						"unrelated_input": {Type: "string"},
					},
					Required: []string{"account_id", "interest_score"},
				},
			},
			"account.bucketed": {
				Payload: EventPayloadSpec{
					Properties: map[string]EventFieldSpec{
						"account_id":     {Type: "string"},
						"bucket":         {Type: "string"},
						"interest_score": {Type: "number"},
						"tier":           {Type: "string"},
					},
				},
				Required: []string{"account_id", "bucket", "interest_score"},
			},
		},
	}
}

func assertEmitCEL(t *testing.T, fields map[string]ExpressionValue, field, want string) {
	t.Helper()
	got, ok := fields[field]
	if !ok {
		t.Fatalf("field %s missing from %#v", field, fields)
	}
	got.hydrate()
	if got.Kind != ExpressionKindCEL || got.CEL != want {
		t.Fatalf("field %s = %#v, want CEL %q", field, got, want)
	}
}
