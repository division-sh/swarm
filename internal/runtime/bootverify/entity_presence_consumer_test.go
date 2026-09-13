package bootverify

import (
	"context"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"strings"
	"testing"
)

func TestEntityNestedPresenceAcrossBootConsumers(t *testing.T) {
	for _, consumer := range []string{"guard", "rule", "write", "emit", "filter", "activity", "mailbox", "artifact"} {
		for _, safe := range []bool{false, true} {
			name := consumer + "/unsafe"
			value := `entity.profile.note`
			if safe {
				name = consumer + "/decision"
				value = `entity.profile.?note.orValue("fallback")`
			}
			t.Run(name, func(t *testing.T) {
				handler := rc.SystemNodeEventHandler{}
				switch consumer {
				case "guard":
					handler.Guard = &rc.GuardSpec{Check: value + ` == "fallback"`}
				case "rule":
					handler.Rules = []rc.HandlerRuleEntry{{ID: "accept", Condition: value + ` == "fallback"`}}
				case "write":
					handler.DataAccumulation = rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{{TargetField: "captured", Value: rc.CELExpression(value)}}}
				case "emit":
					handler.Emit = rc.EmitSpec{Event: "work.result", Fields: map[string]rc.ExpressionValue{"note": rc.CELExpression(value)}}
				case "filter":
					handler.Filter = &rc.FilterSpec{ItemsFrom: "payload.items", Condition: value + ` == "fallback"`, StoreAs: "computed.filtered"}
				case "activity":
					handler.Activity = rc.ActivitySpec{Tool: "notify", Input: map[string]rc.ExpressionValue{"value": rc.CELExpression(value)}}
				case "mailbox":
					handler.Action = rc.ActionSpec{ID: "mailbox_write", Mailbox: &rc.MailboxWriteSpec{Summary: rc.CELExpression(value)}}
				case "artifact":
					handler.Action = rc.ActionSpec{ID: "artifact_repo_commit", ArtifactRepo: &rc.ArtifactRepoSpec{Namespace: rc.CELExpression(value)}}
				}
				source := collectionItemSemanticsSource(handler)
				bundle, _ := semanticview.Bundle(source)
				bundle.Platform.Platform.Name = "presence-proof"
				bundle.Platform.Platform.Version = "1"
				entity := bundle.RootEntities["items"]
				entity.Fields["profile"] = rc.EntityFieldDecl{Type: "WorkItem", Initial: map[string]any{"id": "proof", "status": "ready", "tags": []any{}}}
				entity.Fields["captured"] = rc.EntityFieldDecl{Type: "text"}
				for name, field := range entity.Fields {
					field.UnusedReason = "externally populated proof fixture"
					entity.Fields[name] = field
				}
				bundle.RootEntities["items"] = entity
				bundle.Events["work.result"] = rc.EventCatalogEntry{Payload: rc.EventPayloadSpec{Properties: map[string]rc.EventFieldSpec{"note": {Type: "text"}}, Required: []string{"note"}}}
				tools, _ := semanticview.Bundle(schemaBoundActivityInputSource(value, "text", rc.ToolSchemaString, true))
				bundle.Tools = tools.Tools
				report := Run(context.Background(), source, Options{})
				presenceError := false
				for _, f := range report.Errors() {
					if strings.Contains(f.Message, "without a presence decision") {
						presenceError = true
					}
				}
				if !safe && !presenceError {
					t.Fatalf("missing presence error: %#v", report.Errors())
				}
				if safe {
					for _, f := range report.Errors() {
						if strings.Contains(f.CheckID, "expression") || strings.Contains(f.CheckID, "collection") {
							t.Fatalf("safe expression rejected: %#v", f)
						}
					}
				}
			})
		}
	}
}

func TestStageGateContextPreservesNestedEntityPresence(t *testing.T) {
	for _, safe := range []bool{false, true} {
		expression := `entity.profile.note`
		if safe {
			expression = `entity.profile.?note.orValue("fallback")`
		}
		plan := rc.WorkflowGatePlan{FlowID: ".", Stage: "awaiting_review", Decision: "review", Context: map[string]rc.ExpressionValue{"note": rc.CELExpression(expression)}, Outcomes: map[string]rc.WorkflowGateOutcomePlan{"accept": {AdvancesTo: "complete"}}}
		bundle := stageGateValidationBundle(plan, nil, nil)
		bundle.RootTypes = rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"Profile": {Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}}}}}
		bundle.RootEntities = rc.EntityContractsDocument{"work": {Fields: map[string]rc.EntityFieldDecl{"profile": {Type: "Profile", Initial: map[string]any{}}}}}
		findings := checkStageGateValidation(&checkerContext{source: semanticview.Wrap(bundle)})
		if safe && len(findings) != 0 {
			t.Fatalf("safe gate context: %#v", findings)
		}
		if !safe && (len(findings) != 1 || !strings.Contains(findings[0].Message, "presence decision")) {
			t.Fatalf("unsafe gate context: %#v", findings)
		}
	}
}
