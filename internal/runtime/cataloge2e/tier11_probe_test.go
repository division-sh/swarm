package cataloge2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestTier11Probe(t *testing.T) {
	if os.Getenv("SWARM_CATALOG_E2E_DEBUG") != "1" {
		t.Skip("diagnostic duplicate of Tier 11 runtime/classification proof; run with SWARM_CATALOG_E2E_DEBUG=1 for manual probe output")
	}

	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range []string{
		"test-child-flow-absolute-path",
		"test-child-flow-local-events",
		"test-child-flow-policy-inherit",
		"test-data-pin-write-conflict",
		"test-child-flow-pin-wiring",
		"test-multi-level-policy-inherit",
		"test-nested-three-levels",
		"test-required-agents-child",
		"test-child-flow-sibling-isolation",
	} {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			var expected catalogExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)
			if expected.Trigger.Boot {
				bundle, err := loadFixtureBundleMaybe(fixtureRoot)
				if err != nil {
					t.Logf("load error: %v", err)
					return
				}
				source := semanticview.Wrap(bundle)
				report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
				t.Logf("boot warnings=%#v", report.Warnings())
				t.Logf("boot errors=%#v", report.Errors())
				return
			}
			h := newRuntimeHarness(t, fixtureRoot, true)
			h.seedEntityFields(expected)
			for _, step := range expected.triggerSequence() {
				h.publishAndWait(step, catalogRuntimePublishTimeout)
			}
			rows, err := workflowStateDebugRows(h.db)
			if err != nil {
				t.Fatalf("debug rows: %v", err)
			}
			t.Logf("rows=%s", rows)
			instance, ok, err := h.workflow.Load(catalogRuntimeContext(), "11111111-1111-4111-8111-111111111111")
			if err == nil && ok {
				t.Logf("root metadata=%#v", instance.Metadata)
			}
			if h.rt != nil && h.rt.Pipeline != nil {
				source := h.rt.Pipeline.SemanticSource()
				for _, node := range h.rt.Pipeline.WorkflowNodes() {
					t.Logf("node=%s subs=%v produces=%v", node.ID, node.Subscriptions, node.Produces)
				}
				if source != nil {
					trigger := strings.TrimSpace(expected.Trigger.Event)
					t.Logf("owners(%s)=%v", trigger, source.RuntimeEventOwners(trigger))
					for _, node := range h.rt.Pipeline.WorkflowNodes() {
						if src, ok := source.NodeContractSource(node.ID); ok {
							t.Logf("nodeSource(%s)=%#v flowPath=%q", node.ID, src, source.FlowPath(src.FlowID))
						}
					}
					for _, owner := range source.RuntimeEventOwners(trigger) {
						if handler, ok := source.NodeEventHandler(owner, trigger); ok {
							t.Logf("handler owner=%s emits=%v advances_to=%s", owner, runtimecontracts.HandlerEmitEvents(handler), handler.AdvancesTo)
						}
					}
					for _, observed := range []string{
						"child/task.done",
						"child/work.completed",
						"analyzer-flow/analysis.done",
						"child/grandchild/micro.done",
						"child/step.result",
						"flow-a/alpha.complete",
						"child/child.done",
					} {
						if owners := source.RuntimeEventOwners(observed); len(owners) > 0 {
							t.Logf("owners(%s)=%v", observed, owners)
						}
					}
				}
			}
			var eventsDump []string
			eventRows, err := h.db.QueryContext(context.Background(), `
				SELECT event_name, COALESCE(NULLIF(payload->>'entity_id',''), COALESCE(entity_id::text,'')), COALESCE(flow_instance,'')
				FROM events
				ORDER BY created_at ASC, event_id ASC
			`)
			if err == nil {
				defer eventRows.Close()
				for eventRows.Next() {
					var name, entityID, flowInstance string
					if scanErr := eventRows.Scan(&name, &entityID, &flowInstance); scanErr == nil {
						eventsDump = append(eventsDump, name+" entity="+entityID+" flow="+flowInstance)
					}
				}
			}
			t.Logf("events=%v", eventsDump)
			var runtimeLogs []string
			logRows, err := h.db.QueryContext(context.Background(), `
				SELECT COALESCE(payload->'details'->>'component',''),
				       COALESCE(payload->'details'->>'action',''),
				       COALESCE(payload->'details'->>'error',''),
				       COALESCE(payload->>'message','')
				FROM events
				WHERE event_name = 'platform.runtime_log'
				ORDER BY created_at ASC, event_id ASC
			`)
			if err == nil {
				defer logRows.Close()
				for logRows.Next() {
					var component, action, errText, message string
					if scanErr := logRows.Scan(&component, &action, &errText, &message); scanErr == nil {
						runtimeLogs = append(runtimeLogs, component+":"+action+" error="+errText+" message="+message)
					}
				}
			}
			t.Logf("runtime_logs=%v", runtimeLogs)
			var receipts []string
			receiptRows, err := h.db.QueryContext(context.Background(), `
				SELECT subscriber_type, subscriber_id, outcome, COALESCE(flow_instance,''), COALESCE(entity_id::text,'')
				FROM event_receipts
				ORDER BY processed_at ASC NULLS LAST, receipt_id ASC
			`)
			if err == nil {
				defer receiptRows.Close()
				for receiptRows.Next() {
					var subscriberType, subscriberID, outcome, flowInstance, entityID string
					if scanErr := receiptRows.Scan(&subscriberType, &subscriberID, &outcome, &flowInstance, &entityID); scanErr == nil {
						receipts = append(receipts, subscriberType+":"+subscriberID+" outcome="+outcome+" entity="+entityID+" flow="+flowInstance)
					}
				}
			}
			t.Logf("receipts=%v", receipts)
			var deliveries []string
			deliveryRows, err := h.db.QueryContext(context.Background(), `
				SELECT event_id::text, recipient_agent_id
				FROM event_deliveries
				ORDER BY created_at ASC, delivery_id ASC
			`)
			if err == nil {
				defer deliveryRows.Close()
				for deliveryRows.Next() {
					var eventID, agentID string
					if scanErr := deliveryRows.Scan(&eventID, &agentID); scanErr == nil {
						deliveries = append(deliveries, eventID+"->"+agentID)
					}
				}
			}
			t.Logf("deliveries=%v", deliveries)
		})
	}
}
