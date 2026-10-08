package serveapp

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestA9ConcurrentKeyedIngressElectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, distinct := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/distinct_leaves=%t", backend, distinct), func(t *testing.T) {
				secret := a9SetRawIngressCredential(t)
				_, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyNestedConcurrentRawIngress(t, distinct))
				process, served := start()
				t.Cleanup(func() {
					if code := process.stop(); code != 0 {
						t.Errorf("concurrent shutdown=%d", code)
					}
				})
				rt := servedTestProcessRuntime(t, process)
				statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
				if err != nil || len(statuses) != 1 {
					t.Fatalf("binding=%+v err=%v", statuses, err)
				}
				runID := statuses[0].RunID
				endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/nested-shop/partner"
				gate := make(chan struct{})
				type response struct {
					key    string
					status int
					body   []byte
					err    error
				}
				responses := make(chan response, 4)
				ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				defer cancel()
				for _, key := range []string{"left", "right", "left", "right"} {
					go func() {
						<-gate
						status, body, err := a9SignedRawIngress(ctx, endpoint, secret, `{"delivery_id":"`+key+`"}`)
						responses <- response{key: key, status: status, body: body, err: err}
					}()
				}
				close(gate)
				receipts := make(map[string]map[string]any)
				var failures []response
				for range 4 {
					var result response
					select {
					case result = <-responses:
					case <-ctx.Done():
						t.Fatal("concurrent native election failed to join")
					}
					if result.err != nil || result.status != http.StatusAccepted && result.status != http.StatusOK {
						failures = append(failures, result)
						continue
					}
					receipt := a9AliasReceipt(t, result.body)
					if previous, found := receipts[result.key]; found && !reflect.DeepEqual(previous, receipt) {
						t.Fatalf("one identity elected two receipts: %+v / %+v", previous, receipt)
					}
					receipts[result.key] = receipt
				}
				if len(failures) > 0 {
					for _, key := range []string{"left", "right"} {
						status, retry := a9PostSignedRawIngress(t, endpoint, secret, `{"delivery_id":"`+key+`"}`)
						t.Logf("election retry %s=%d %s", key, status, retry)
					}
					var logs operatorread.OperatorRuntimeLogListResult
					requireServedJSONRPCResult(t, served.Endpoint, "runtime.logs", map[string]any{"limit": 100}, &logs)
					for _, log := range logs.Logs {
						if log.Level == "error" {
							t.Logf("native election error: %+v", log)
						}
					}
					t.Fatalf("concurrent arrivals refused: %+v", failures)
				}
				if receipts["left"]["publication_id"] == receipts["right"]["publication_id"] {
					t.Fatal("distinct deliveries sharing a business key aliased receipts")
				}
				eventIDs := make(map[string]bool)
				for _, receipt := range receipts {
					ids, ok := receipt["event_ids"].([]any)
					if !ok || len(ids) != 1 {
						t.Fatalf("raw receipt cardinality=%+v", receipt)
					}
					id := ids[0].(string)
					eventIDs[id] = true
					a9RequireNestedIngressSettlement(t, served.Endpoint, id)
				}
				instances, err := rt.Pipeline.ListWorkflowInstances(t.Context(), runID)
				wantInstances := 4
				if distinct {
					wantInstances++
				}
				if err != nil || len(instances) != wantInstances {
					t.Fatalf("concurrent construction cardinality=%+v want=%d err=%v", instances, wantInstances, err)
				}
				initial := make(map[string]pipeline.FlowConstructionPublicationEvidence)
				for _, instance := range instances {
					if instance.WorkflowName == "." || instance.WorkflowName == "parent/middle" {
						continue
					}
					var entity operatorread.OperatorEntityFull
					requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": instance.EntityID}, &entity)
					want := float64(2)
					if distinct && instance.WorkflowName == "parent/middle/leaf" {
						want = 1
					}
					if entity.Fields["seen"] != want {
						t.Fatalf("duplicate or lost same-key work: %+v want=%v", entity, want)
					}
					coordinate, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(instance.WorkflowName, instance.InstanceID, instance.StorageRef))
					if err != nil {
						t.Fatal(err)
					}
					original, err := rt.Pipeline.LoadFlowConstructionPublication(t.Context(), coordinate, instance.EntityID)
					if err != nil || !eventIDs[original.CreatingInput.EventID] || original.CreatingInput.Input != "account.opened" || original.Fields["seen"] != int64(0) {
						t.Fatalf("elected constructor lost original fields/input: %+v err=%v", original, err)
					}
					initial[instance.EntityID] = original
				}
				for key, receipt := range receipts {
					status, duplicate := a9PostSignedRawIngress(t, endpoint, secret, `{"delivery_id":"`+key+`"}`)
					if status != http.StatusOK || !reflect.DeepEqual(receipt, a9AliasReceipt(t, duplicate)) {
						t.Fatalf("concurrent election replay changed=%d %s", status, duplicate)
					}
				}
				for _, instance := range instances {
					if expected, checked := initial[instance.EntityID]; checked {
						coordinate, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(instance.WorkflowName, instance.InstanceID, instance.StorageRef))
						if err != nil {
							t.Fatal(err)
						}
						current, err := rt.Pipeline.LoadFlowConstructionPublication(t.Context(), coordinate, instance.EntityID)
						if err != nil || !reflect.DeepEqual(expected, current) {
							t.Fatal("concurrent replay replaced elected constructor")
						}
					}
				}
			})
		}
	}
}
