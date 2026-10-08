package serveapp

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestA9NestedKeyedIngressConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyNestedKeyedRawIngress(t)
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			const secret = "a9-nested-construction"
			if err := file.Set(t.Context(), "webhook_signing.partner", secret); err != nil {
				t.Fatal(err)
			}
			_, start := clockDeploymentHarness(t, backend, root)
			process, served := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("joined nested shutdown=%d", code)
				}
			})
			rt := servedTestProcessRuntime(t, process)
			statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 {
				t.Fatalf("binding status=%+v err=%v", statuses, err)
			}
			runID := statuses[0].RunID
			instances, err := rt.Pipeline.ListWorkflowInstances(t.Context(), runID)
			if err != nil || len(instances) != 1 {
				t.Fatalf("keyless root must not manufacture keyed descendants: %+v err=%v", instances, err)
			}
			endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/nested-shop/partner"
			var leaves []operatorread.OperatorEventDelivery
			for _, deliveryID := range []string{"left", "right"} {
				body := `{"delivery_id":"` + deliveryID + `"}`
				code, raw := a9PostSignedRawIngress(t, endpoint, secret, body)
				if code != http.StatusAccepted {
					t.Fatalf("nested ingress status=%d body=%s", code, raw)
				}
				var receipt struct {
					EventIDs []string `json:"event_ids"`
				}
				if err := json.Unmarshal(raw, &receipt); err != nil || len(receipt.EventIDs) != 1 {
					t.Fatalf("nested receipt=%s err=%v", raw, err)
				}
				event := a9RequireNestedIngressSettlement(t, served.Endpoint, receipt.EventIDs[0])
				for _, delivery := range event.Deliveries {
					var entity operatorread.OperatorEntityFull
					requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": delivery.Target.EntityID}, &entity)
					if entity.Fields["seen"] != float64(1) {
						t.Fatalf("constructor delivery not exactly once: %+v", entity)
					}
					if delivery.Target.FlowID == "parent/middle/leaf" {
						if entity.Fields["id"] != "partner" {
							t.Fatalf("leaf key borrowed parent slot: %+v", entity)
						}
						leaves = append(leaves, delivery)
					} else if delivery.Target.FlowID != "parent" || entity.Fields["id"] != deliveryID {
						t.Fatalf("parent key borrowed another edge: %+v", entity)
					}
				}
				code, duplicate := a9PostSignedRawIngress(t, endpoint, secret, body)
				if code != http.StatusOK || !reflect.DeepEqual(a9AliasReceipt(t, raw), a9AliasReceipt(t, duplicate)) {
					t.Fatalf("nested exact retry changed receipt: %s -> %s status=%d", raw, duplicate, code)
				}
			}
			if len(leaves) != 2 || leaves[0].Target.EntityID == leaves[1].Target.EntityID || leaves[0].Target.FlowInstance == leaves[1].Target.FlowInstance {
				t.Fatalf("same leaf key under two parents collapsed: %+v", leaves)
			}
			for _, leaf := range leaves {
				var entity operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": leaf.Target.EntityID}, &entity)
				if entity.Fields["seen"] != float64(1) {
					t.Fatalf("retry or sibling delivery mutated a retained leaf: %+v", entity)
				}
			}
			instances, err = rt.Pipeline.ListWorkflowInstances(t.Context(), runID)
			if err != nil || len(instances) != 7 {
				t.Fatalf("expected root + two exact parent/middle/leaf trees: %+v err=%v", instances, err)
			}
			byEntity := make(map[string]pipeline.WorkflowInstance, len(instances))
			for _, instance := range instances {
				byEntity[instance.EntityID] = instance
			}
			for _, instance := range instances {
				if instance.WorkflowName == "." {
					continue
				}
				parent, found := byEntity[instance.ParentEntityID]
				if !found || instance.ParentFlowID != parent.WorkflowName || instance.ParentFlowInstance != parent.StorageRef {
					t.Fatalf("constructed header lost its actual parent: child=%+v parent=%+v", instance, parent)
				}
				coordinate, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(instance.WorkflowName, instance.InstanceID, instance.StorageRef))
				if err != nil {
					t.Fatal(err)
				}
				initial, err := rt.Pipeline.LoadFlowConstructionPublication(t.Context(), coordinate, instance.EntityID)
				if err != nil || initial.CreatingInput.EventID == "" {
					t.Fatalf("nested immutable creating evidence absent: %+v err=%v", initial, err)
				}
				if instance.WorkflowName == "parent/middle" {
					if initial.CreatingInput.Input != "" {
						t.Fatalf("eager keyless intermediate gained an argument input: %+v", initial)
					}
				} else if initial.CreatingInput.Input != "account.opened" || initial.Fields["id"] != instance.Fields["id"] || initial.Fields["seen"] != int64(0) {
					t.Fatalf("nested immutable constructor fields were replaced by current state: %+v current=%+v", initial, instance.Fields)
				}
			}
		})
	}
}

func a9RequireNestedIngressSettlement(t *testing.T, endpoint, eventID string) operatorread.OperatorEventFull {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var event operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
		if len(event.Deliveries) == 2 && event.Deliveries[0].Terminal && event.Deliveries[1].Terminal && event.Deliveries[0].Status == "delivered" && event.Deliveries[1].Status == "delivered" {
			return event
		}
		if time.Now().After(deadline) {
			t.Fatalf("nested constructor deliveries did not settle: %+v", event)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
