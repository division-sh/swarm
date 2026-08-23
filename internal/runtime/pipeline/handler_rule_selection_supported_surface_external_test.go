package pipeline_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestHandlerRuleSelectionRunsThroughDurableEventBusAndReconstructedTraceOnBothStores(t *testing.T) {
	tests := []struct {
		event       string
		context     handlerselection.Context
		disposition handlerselection.Disposition
		elementID   string
		label       string
	}{
		{event: "rules.selected", context: handlerselection.ContextRules, disposition: handlerselection.DispositionSelected, elementID: "00000000-0000-4000-8000-000000000421", label: "rules-label"},
		{event: "rules.no_match", context: handlerselection.ContextRules, disposition: handlerselection.DispositionNoMatch},
		{event: "complete.selected", context: handlerselection.ContextOnComplete, disposition: handlerselection.DispositionSelected, elementID: "00000000-0000-4000-8000-000000000423", label: "complete-label"},
		{event: "complete.no_match", context: handlerselection.ContextOnComplete, disposition: handlerselection.DispositionNoMatch},
		{event: "direct", context: handlerselection.ContextNone, disposition: handlerselection.DispositionNotApplicable},
	}
	for _, storeCase := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(storeCase.name, func(t *testing.T) {
			selected := storeCase.open(t)
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			source := handlerRuleSelectionSupportedSource(t)
			node := externalPipelineSourceNode(t, source, "", "selection-node")
			workflowName := source.WorkflowName()
			policies := map[string]runtimepipeline.WorkflowEventPolicy{}
			subscriptions := make([]events.EventType, 0, len(tests))
			for _, tc := range tests {
				eventType := workflowName + "/" + tc.event
				subscriptions = append(subscriptions, events.EventType(eventType))
				policies[eventType] = runtimepipeline.WorkflowEventPolicy{Consume: true}
			}
			module := proposedEffectProofModule{
				source:   source,
				workflow: runtimepipeline.NewWorkflowDefinition(workflowName, []runtimepipeline.WorkflowStage{{Name: "active"}}, nil),
				nodes: []runtimepipeline.WorkflowNode{{
					Node: node, Subscriptions: subscriptions, ExecutionType: runtimecontracts.SystemNodeExecutionType, Policies: policies,
				}},
			}
			eventBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("new handler-rule EventBus: %v", err)
			}
			coordinator := newGateRecoveryCoordinator(eventBus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: module})
			eventBus.SetInterceptors(coordinator)

			for index, tc := range tests {
				t.Run(tc.event, func(t *testing.T) {
					sourceEnvelope := events.EnvelopeForFlowInstance(
						events.EnvelopeForEntityID(events.EventEnvelope{}, runtimeflowidentity.EntityID(workflowName)),
						workflowName,
					)
					event := eventtest.ExistingRunRootIngress(
						uuid.NewString(), events.EventType(tc.event), "operator", "", []byte(`{"proof":true}`),
						0, runID, sourceEnvelope,
						time.Now().UTC().Add(time.Duration(index)*time.Millisecond),
					)
					plan, err := eventBus.CheckPublishRecipientPlan(ctx, event)
					if err != nil || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Recipient.ID() != node.Key() {
						t.Fatalf("handler-rule plan = %#v err=%v", plan, err)
					}
					if err := eventBus.PublishAcknowledged(ctx, event); err != nil {
						t.Fatalf("publish handler-rule event: %v", err)
					}
					waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
						cancel()
						t.Fatalf("wait handler-rule delivery: %v", err)
					}
					cancel()
					assertPersistedHandlerRuleSelection(t, selected, ctx, event.ID(), tc.context, tc.disposition, tc.elementID, tc.label)
					assertTraceHandlerRuleSelection(t, selected, ctx, runID, event.ID(), tc.context, tc.disposition, tc.elementID, tc.label)

					if err := eventBus.PublishAcknowledged(ctx, event); err != nil {
						t.Fatalf("replay handler-rule event: %v", err)
					}
					assertExactJoinDeliveryCount(t, selected, ctx, event.ID(), node.Key(), 1)
					assertPersistedHandlerRuleSelection(t, selected, ctx, event.ID(), tc.context, tc.disposition, tc.elementID, tc.label)
				})
			}
		})
	}
}

func handlerRuleSelectionSupportedSource(t *testing.T) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"package.yaml": "name: handler-rule-selection-proof\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\n",
		"schema.yaml": `name: handler-rule-selection-proof
pins:
  inputs:
    events:
      - {name: rules_selected, event: rules.selected, source: external}
      - {name: rules_no_match, event: rules.no_match, source: external}
      - {name: complete_selected, event: complete.selected, source: external}
      - {name: complete_no_match, event: complete.no_match, source: external}
      - {name: direct, event: direct, source: external}
  outputs:
    events: []
`,
		"events.yaml": `rules.selected: {swarm: {source: external}}
rules.no_match: {swarm: {source: external}}
complete.selected: {swarm: {source: external}}
complete.no_match: {swarm: {source: external}}
direct: {swarm: {source: external}}
`,
		"nodes.yaml": `selection-node:
  id: selection-node
  execution_type: system_node
  subscribes_to: [rules.selected, rules.no_match, complete.selected, complete.no_match, direct]
  event_handlers:
    rules.selected:
      rules:
        selected:
          element_id: 00000000-0000-4000-8000-000000000421
          id: rules-label
          condition: else
    rules.no_match:
      rules:
        - element_id: 00000000-0000-4000-8000-000000000422
          id: never-rules
          condition: "false"
    complete.selected:
      on_complete:
        - element_id: 00000000-0000-4000-8000-000000000423
          id: complete-label
          condition: else
    complete.no_match:
      on_complete:
        - element_id: 00000000-0000-4000-8000-000000000424
          id: never-complete
          condition: "false"
    direct: {}
`,
	}
	for _, name := range []string{"entities.yaml", "policy.yaml", "tools.yaml", "agents.yaml", "types.yaml"} {
		files[name] = "{}\n"
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load supported handler-rule bundle: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func assertPersistedHandlerRuleSelection(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, eventID string, wantContext handlerselection.Context, wantDisposition handlerselection.Disposition, wantElementID, wantLabel string) {
	assertPersistedHandlerRuleSelectionInPackage(t, selected, ctx, eventID, wantContext, wantDisposition, ".", wantElementID, wantLabel)
}

func assertPersistedHandlerRuleSelectionInPackage(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, eventID string, wantContext handlerselection.Context, wantDisposition handlerselection.Disposition, wantPackage, wantElementID, wantLabel string) {
	t.Helper()
	query := `SELECT s.selection_context, s.disposition, COALESCE(s.package_coordinate, ''), COALESCE(s.element_id, ''), s.display_label
FROM event_delivery_handler_rule_selections s JOIN event_deliveries d ON d.delivery_id = s.delivery_id
WHERE d.event_id = ? AND d.subscriber_type = 'node'`
	if selected.postgres {
		query = `SELECT s.selection_context, s.disposition, COALESCE(s.package_coordinate, ''), COALESCE(s.element_id::text, ''), s.display_label
FROM event_delivery_handler_rule_selections s JOIN event_deliveries d ON d.delivery_id = s.delivery_id
WHERE d.event_id = $1::uuid AND d.subscriber_type = 'node'`
	}
	var gotContext, gotDisposition, gotPackage, gotElementID, gotLabel string
	if err := selected.db.QueryRowContext(ctx, query, eventID).Scan(&gotContext, &gotDisposition, &gotPackage, &gotElementID, &gotLabel); err != nil {
		t.Fatalf("load handler-rule selection for %s: %v", eventID, err)
	}
	if gotContext != string(wantContext) || gotDisposition != string(wantDisposition) || gotElementID != wantElementID || gotLabel != wantLabel {
		t.Fatalf("persisted selection = %s/%s/%s/%s/%s, want %s/%s/%s/%s/%s", gotContext, gotDisposition, gotPackage, gotElementID, gotLabel, wantContext, wantDisposition, wantPackage, wantElementID, wantLabel)
	}
	if wantDisposition == handlerselection.DispositionSelected && gotPackage != wantPackage {
		t.Fatalf("selected package coordinate = %q, want %q", gotPackage, wantPackage)
	}
	if wantDisposition != handlerselection.DispositionSelected && gotPackage != "" {
		t.Fatalf("non-selected package coordinate = %q, want empty", gotPackage)
	}
}

func assertTraceHandlerRuleSelection(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, runID, eventID string, wantContext handlerselection.Context, wantDisposition handlerselection.Disposition, wantElementID, wantLabel string) {
	assertTraceHandlerRuleSelectionInPackage(t, selected, ctx, runID, eventID, wantContext, wantDisposition, ".", wantElementID, wantLabel)
}

func assertTraceHandlerRuleSelectionInPackage(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, runID, eventID string, wantContext handlerselection.Context, wantDisposition handlerselection.Disposition, wantPackage, wantElementID, wantLabel string) {
	t.Helper()
	if wantDisposition != handlerselection.DispositionSelected {
		wantPackage = ""
	}
	rows, _, err := selected.trace.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 100})
	if err != nil {
		t.Fatalf("load reconstructed handler-rule trace: %v", err)
	}
	for _, row := range rows {
		if row.EventID != eventID {
			continue
		}
		if row.HandlerRuleSelection == nil {
			t.Fatalf("trace row %s has no handler-rule selection", eventID)
		}
		got := row.HandlerRuleSelection
		if got.Context != wantContext || got.Disposition != wantDisposition || got.PackageCoordinate != wantPackage || got.ElementID != wantElementID || got.DisplayLabel != wantLabel {
			t.Fatalf("trace selection = %#v, want %s/%s/%s/%s/%s", got, wantContext, wantDisposition, wantPackage, wantElementID, wantLabel)
		}
		return
	}
	t.Fatalf("trace omitted event %s; rows=%s", eventID, fmt.Sprint(rows))
}
