package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type a9ConstructorElectionBarrier struct {
	inboundpublication.Runner
	mode      string
	calls     atomic.Int32
	prepared  chan inboundpublication.CommitCommand
	committed chan inboundpublication.CommitResult
	release   chan struct{}
	joined    sync.Once
}

func (b *a9ConstructorElectionBarrier) unblock() { b.joined.Do(func() { close(b.release) }) }

func (b *a9ConstructorElectionBarrier) CommitInboundPublication(ctx context.Context, command inboundpublication.CommitCommand) (inboundpublication.CommitResult, error) {
	call := b.calls.Add(1)
	if b.mode == "both_planned" && call <= 2 || b.mode == "winner_not_installed" && call == 2 {
		b.prepared <- command
		if b.mode == "both_planned" {
			select {
			case <-b.release:
			case <-ctx.Done():
				return inboundpublication.CommitResult{}, ctx.Err()
			}
		}
	}
	result, err := b.Runner.CommitInboundPublication(ctx, command)
	if b.mode == "winner_not_installed" && call == 1 && result.Acknowledged && err == nil {
		b.committed <- result
		select {
		case <-b.release:
		case <-ctx.Done():
			return result, ctx.Err()
		}
	}
	return result, err
}

func TestA9ConcurrentKeyedIngressElectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, distinct := range []bool{false, true} {
			for _, mode := range []string{"concurrent_start", "both_planned", "winner_not_installed"} {
				t.Run(fmt.Sprintf("%s/distinct_leaves=%t/%s", backend, distinct, mode), func(t *testing.T) {
					secret := a9SetRawIngressCredential(t)
					_, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyNestedConcurrentRawIngress(t, distinct))
					barrier := &a9ConstructorElectionBarrier{mode: mode, prepared: make(chan inboundpublication.CommitCommand, 2), committed: make(chan inboundpublication.CommitResult, 1), release: make(chan struct{})}
					defer barrier.unblock()
					previous := projectRuntimePersistenceForServe
					projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
						projection := previous(owner)
						barrier.Runner = projection.deps.InboundStore
						projection.deps.InboundStore = barrier
						return projection
					}
					t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
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
					var clients sync.WaitGroup
					defer func() { cancel(); barrier.unblock(); clients.Wait() }()
					launch := func(key string) {
						clients.Add(1)
						go func() {
							defer clients.Done()
							<-gate
							status, body, err := a9SignedRawIngress(ctx, endpoint, secret, `{"delivery_id":"`+key+`"}`)
							responses <- response{key: key, status: status, body: body, err: err}
						}()
					}
					launch("left")
					if mode != "winner_not_installed" {
						for _, key := range []string{"right", "left", "right"} {
							launch(key)
						}
					}
					close(gate)
					if mode == "both_planned" {
						for range 2 {
							select {
							case command := <-barrier.prepared:
								if len(command.Publications) != 1 || len(command.Publications[0].Activations) != 2 {
									t.Fatal("both plans must carry the same fresh parent and a fresh leaf")
								}
							case <-ctx.Done():
								t.Fatal("two fresh plans did not reach native commit")
							}
						}
						barrier.unblock()
					}
					if mode == "winner_not_installed" {
						select {
						case <-barrier.committed:
						case <-ctx.Done():
							t.Fatal("winner did not commit before installation")
						}
						for _, key := range []string{"right", "left", "right"} {
							launch(key)
						}
						select {
						case command := <-barrier.prepared:
							for _, publication := range command.Publications {
								for _, activation := range publication.Activations {
									if activation.Identity.TemplateID == "parent" {
										t.Fatal("durable winner was re-created while local installation was held")
									}
								}
							}
						case <-ctx.Done():
							t.Fatal("fresh reuse depended on local construction installation")
						}
						barrier.unblock()
					}
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
						if instance.WorkflowName == "parent" {
							created := servedClockEvents(t, served.Endpoint, runID, instance.StorageRef+"/parent.created")
							if len(created) != 1 || !eventIDs[created[0].SourceEventID] {
								t.Fatalf("elected parent creation publication repeated or missing: %+v entity=%+v", created, entity)
							}
							a9RequireIngressDeliveries(t, served.Endpoint, created[0].EventID, 1)
							requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": instance.EntityID}, &entity)
							if entity.Fields["creation_count"] != float64(1) {
								t.Fatalf("creation handler repeated or missing: %+v", entity)
							}
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
}

func TestA9IndependentReceiverProgressWhileSiblingHandlerHeldBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			secret := a9SetRawIngressCredential(t)
			opts, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyNestedConcurrentRawIngress(t, true))
			held, release := make(chan string, 1), make(chan struct{})
			var unblock sync.Once
			defer unblock.Do(func() { close(release) })
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, key string, event events.Event) error {
				node, err := identity.ParseExecutableNodeKey(key)
				if err != nil {
					return err
				}
				if node.FlowPath() != "parent/middle/leaf" || event.Type() != "account.opened" {
					return nil
				}
				var payload map[string]any
				if err := json.Unmarshal(event.Payload(), &payload); err != nil {
					return err
				}
				if payload["provider_event_id"] != "left" {
					return nil
				}
				held <- event.ID()
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			process, served := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("held sibling shutdown=%d", code)
				}
			})
			endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/nested-shop/partner"
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			type response struct {
				status int
				body   []byte
				err    error
			}
			returned := make(chan response, 1)
			var clients sync.WaitGroup
			defer func() { cancel(); unblock.Do(func() { close(release) }); clients.Wait() }()
			clients.Add(1)
			go func() {
				defer clients.Done()
				status, body, err := a9SignedRawIngress(ctx, endpoint, secret, `{"delivery_id":"left"}`)
				returned <- response{status, body, err}
			}()
			var heldID string
			select {
			case heldID = <-held:
			case <-ctx.Done():
				t.Fatal("left receiver never reached its execution barrier")
			}
			status, raw := a9PostSignedRawIngress(t, endpoint, secret, `{"delivery_id":"right"}`)
			if status != http.StatusAccepted {
				t.Fatalf("independent key blocked or failed=%d %s", status, raw)
			}
			right := a9AliasReceipt(t, raw)
			ids := right["event_ids"].([]any)
			a9RequireNestedIngressSettlement(t, served.Endpoint, ids[0].(string))
			var pending operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, served.Endpoint, "event.get", map[string]any{"event_id": heldID}, &pending)
			if len(pending.Deliveries) != 2 || len(pending.DeadLetters) != 0 {
				t.Fatalf("held sibling lost its exact obligations: %+v", pending)
			}
			allTerminal := true
			for _, delivery := range pending.Deliveries {
				allTerminal = allTerminal && delivery.Terminal
			}
			if allTerminal {
				t.Fatal("held handler settled before its barrier released")
			}
			unblock.Do(func() { close(release) })
			select {
			case left := <-returned:
				if left.err != nil || left.status != http.StatusAccepted {
					t.Fatalf("left return=%d %s err=%v", left.status, left.body, left.err)
				}
			case <-ctx.Done():
				t.Fatal("left carrier failed to join")
			}
			a9RequireNestedIngressSettlement(t, served.Endpoint, heldID)
		})
	}
}

func TestA9ConcurrentKeyedRootPreservesCreationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"both_planned", "winner_not_installed"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				secret := a9SetRawIngressCredential(t)
				_, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyConcurrentKeyedRootRawIngress(t))
				barrier := &a9ConstructorElectionBarrier{mode: mode, prepared: make(chan inboundpublication.CommitCommand, 2), committed: make(chan inboundpublication.CommitResult, 1), release: make(chan struct{})}
				defer barrier.unblock()
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
					projection := previous(owner)
					barrier.Runner = projection.deps.InboundStore
					projection.deps.InboundStore = barrier
					return projection
				}
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
				process, served := start()
				t.Cleanup(func() {
					if code := process.stop(); code != 0 {
						t.Errorf("root election shutdown=%d", code)
					}
				})
				rt := servedTestProcessRuntime(t, process)
				statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
				if err != nil || len(statuses) != 1 {
					t.Fatalf("root binding=%+v err=%v", statuses, err)
				}
				runID := statuses[0].RunID
				endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/keyed-shop/partner"
				ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				defer cancel()
				type response struct {
					key    string
					status int
					body   []byte
					err    error
				}
				returned := make(chan response, 2)
				var clients sync.WaitGroup
				defer func() { cancel(); barrier.unblock(); clients.Wait() }()
				launch := func(key string) {
					clients.Add(1)
					go func() {
						defer clients.Done()
						status, body, err := a9SignedRawIngress(ctx, endpoint, secret, `{"delivery_id":"`+key+`"}`)
						returned <- response{key, status, body, err}
					}()
				}
				launch("left")
				if mode == "winner_not_installed" {
					select {
					case <-barrier.committed:
					case <-ctx.Done():
						t.Fatal("root winner did not commit")
					}
				}
				launch("right")
				plans := 2
				if mode == "winner_not_installed" {
					plans = 1
				}
				for range plans {
					select {
					case command := <-barrier.prepared:
						if len(command.Publications) != 1 {
							t.Fatal("root batch lost its event")
						}
						if mode == "both_planned" && len(command.Publications[0].Activations) != 1 {
							t.Fatal("expected two fresh root proposals")
						}
						if mode == "winner_not_installed" && len(command.Publications[0].Activations) != 0 {
							t.Fatal("committed root was reconstructed while installation held")
						}
					case <-ctx.Done():
						t.Fatal("root election did not reach the deterministic barrier")
					}
				}
				barrier.unblock()
				receipts := make(map[string]map[string]any)
				eventIDs := make(map[string]bool)
				for range 2 {
					select {
					case result := <-returned:
						if result.err != nil || result.status != http.StatusAccepted {
							t.Fatalf("root first arrival refused=%d %s err=%v", result.status, result.body, result.err)
						}
						receipts[result.key] = a9AliasReceipt(t, result.body)
						id := receipts[result.key]["event_ids"].([]any)[0].(string)
						eventIDs[id] = true
						a9RequireRootIngressSettlement(t, served.Endpoint, runID, id)
					case <-ctx.Done():
						t.Fatal("root carriers did not join")
					}
				}
				if receipts["left"]["publication_id"] == receipts["right"]["publication_id"] {
					t.Fatal("root receiver key merged distinct provider receipts")
				}
				created := servedClockEvents(t, served.Endpoint, runID, "root.created")
				if len(created) != 1 || !eventIDs[created[0].SourceEventID] {
					t.Fatalf("root creation repeated=%+v", created)
				}
				a9RequireRootIngressSettlement(t, served.Endpoint, runID, created[0].EventID)
				var entity operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": runID}, &entity)
				if entity.Fields["processed_count"] != float64(2) || entity.Fields["creation_count"] != float64(1) {
					t.Fatalf("root work repeated or lost=%+v", entity)
				}
				initial := a9RootConstructionReceipt(t, rt.Pipeline, runID)
				for key, receipt := range receipts {
					code, duplicate := a9PostSignedRawIngress(t, endpoint, secret, `{"delivery_id":"`+key+`"}`)
					if code != http.StatusOK || !reflect.DeepEqual(receipt, a9AliasReceipt(t, duplicate)) {
						t.Fatalf("root duplicate=%d %s", code, duplicate)
					}
				}
				if !reflect.DeepEqual(initial, a9RootConstructionReceipt(t, rt.Pipeline, runID)) {
					t.Fatal("root election rewrote immutable construction")
				}
			})
		}
	}
}
