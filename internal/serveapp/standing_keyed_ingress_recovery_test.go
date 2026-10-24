package serveapp

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type a9InboundCommitResponseFault struct {
	inboundpublication.Runner
	mode      string
	fired     atomic.Bool
	committed chan inboundpublication.CommitResult
}

func (f *a9InboundCommitResponseFault) CommitInboundPublication(ctx context.Context, command inboundpublication.CommitCommand) (inboundpublication.CommitResult, error) {
	if !f.fired.CompareAndSwap(false, true) {
		return f.Runner.CommitInboundPublication(ctx, command)
	}
	fault := errors.New("a9 exact native commit response fault")
	if f.mode == "before_commit" {
		return inboundpublication.CommitResult{}, fault
	}
	result, err := f.Runner.CommitInboundPublication(ctx, command)
	if err != nil || !result.Acknowledged {
		return result, err
	}
	f.committed <- result
	if f.mode == "lost_ack" {
		return inboundpublication.CommitResult{}, fault
	}
	return result, fault
}

func a9SetRawIngressCredential(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", path)
	file, err := credentials.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	const secret = "a9-keyed-recovery-proof"
	if err := file.Set(t.Context(), "webhook_signing.partner", secret); err != nil {
		t.Fatal(err)
	}
	if err := file.Set(t.Context(), "webhook_signing.telegram", secret); err != nil {
		t.Fatal(err)
	}
	return secret
}

func a9RootConstructionReceipt(t *testing.T, rt *pipeline.PipelineCoordinator, runID string) pipeline.FlowConstructionPublicationEvidence {
	t.Helper()
	coordinate, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(".", runID, runID))
	if err != nil {
		t.Fatal(err)
	}
	initial, err := rt.LoadFlowConstructionPublication(t.Context(), coordinate, runID)
	if err != nil {
		t.Fatal(err)
	}
	return initial
}

func TestA9KeyedIngressCommitResponseRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"before_commit", "acknowledged_cleanup_error", "lost_ack"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				secret := a9SetRawIngressCredential(t)
				opts, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyKeyedRootRawIngressCreationEvent(t))
				fault := &a9InboundCommitResponseFault{mode: mode, committed: make(chan inboundpublication.CommitResult, 1)}
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
					projection := previous(owner)
					fault.Runner = projection.deps.InboundStore
					projection.deps.InboundStore = fault
					return projection
				}
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
				first, served := start()
				rt := servedTestProcessRuntime(t, first)
				statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
				if err != nil || len(statuses) != 1 {
					t.Fatalf("binding=%+v err=%v", statuses, err)
				}
				status := statuses[0]
				endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/keyed-shop/partner"
				const body = `{"delivery_id":"commit-boundary"}`
				code, response := a9PostSignedRawIngress(t, endpoint, secret, body)
				want := http.StatusServiceUnavailable
				if mode == "acknowledged_cleanup_error" {
					want = http.StatusAccepted
				}
				if code != want {
					t.Fatalf("commit boundary status=%d want=%d response=%s", code, want, response)
				}
				identity := inboundpublication.Identity{ServiceID: status.ServiceID, RunID: status.RunID, Generation: status.Generation, Provider: "partner", ProviderEventID: "commit-boundary"}
				record, found, err := fault.Runner.LoadInboundPublicationByIdentity(t.Context(), identity)
				instances, loadErr := rt.Pipeline.ListWorkflowInstances(t.Context(), status.RunID)
				if err != nil || loadErr != nil {
					t.Fatalf("native observations=%v / %v", err, loadErr)
				}
				if mode == "before_commit" {
					if found || len(instances) != 0 || len(servedClockEvents(t, served.Endpoint, status.RunID, "account.opened")) != 0 || len(servedClockEvents(t, served.Endpoint, status.RunID, "root.created")) != 0 {
						t.Fatal("refused commit leaked construction, receipt or publication")
					}
					code, response = a9PostSignedRawIngress(t, endpoint, secret, body)
					if code != http.StatusAccepted {
						t.Fatalf("clean retry=%d %s", code, response)
					}
					record, found, err = fault.Runner.LoadInboundPublicationByIdentity(t.Context(), identity)
				}
				if err != nil || !found || len(record.Events) != 1 {
					t.Fatalf("exact committed receipt=%+v found=%t err=%v", record, found, err)
				}
				initial := a9RootConstructionReceipt(t, rt.Pipeline, status.RunID)
				if initial.CreatingInput.EventID != record.Events[0].EventID || initial.CreatingInput.Input != "account.opened" {
					t.Fatalf("committed constructor lost elected publication: %+v", initial)
				}
				if code := first.stop(); code != 0 {
					t.Fatalf("response fault shutdown=%d\n%s", code, first.outputString())
				}
				setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
				second, restarted := start()
				t.Cleanup(func() {
					if code := second.stop(); code != 0 {
						t.Errorf("recovered shutdown=%d", code)
					}
				})
				endpoint = strings.TrimSuffix(restarted.Endpoint, "/v1/rpc") + "/webhooks/keyed-shop/partner"
				code, response = a9PostSignedRawIngress(t, endpoint, secret, body)
				if code != http.StatusOK || a9AliasReceipt(t, response)["publication_id"] != record.PublicationID {
					t.Fatalf("restart retry reminted acceptance: %d %s receipt=%+v", code, response, record)
				}
				a9RequireRootIngressSettlement(t, restarted.Endpoint, status.RunID, record.Events[0].EventID)
				created := servedClockEvents(t, restarted.Endpoint, status.RunID, "root.created")
				if len(created) != 1 || created[0].SourceEventID != record.Events[0].EventID {
					t.Fatalf("response loss duplicated or changed creation: %+v", created)
				}
				a9RequireRootIngressSettlement(t, restarted.Endpoint, status.RunID, created[0].EventID)
				rt = servedTestProcessRuntime(t, second)
				if current := a9RootConstructionReceipt(t, rt.Pipeline, status.RunID); !reflect.DeepEqual(current, initial) {
					t.Fatalf("recovery replaced original construction: %+v -> %+v", initial, current)
				}
				var entity operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, restarted.Endpoint, "entity.get", map[string]any{"run_id": status.RunID, "entity_id": status.RunID}, &entity)
				if entity.Fields["processed_count"] != float64(1) || entity.Fields["creation_count"] != float64(1) {
					t.Fatalf("recovery did not settle exactly once: %+v", entity)
				}
			})
		}
	}
}

func TestA9KeyedIngressHeldConsumerRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			secret := a9SetRawIngressCredential(t)
			opts, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyKeyedRootTelegramIngress(t))
			reached := make(chan string, 1)
			returned := make(chan struct{}, 1)
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, event events.Event) error {
				if event.Type() != "inbound.telegram.text_message" {
					return nil
				}
				select {
				case reached <- event.ID():
				default:
				}
				<-ctx.Done()
				returned <- struct{}{}
				return ctx.Err()
			}
			first, served := start()
			rt := servedTestProcessRuntime(t, first)
			statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 {
				t.Fatalf("binding=%+v err=%v", statuses, err)
			}
			status := statuses[0]
			endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc")
			const body = `{"update_id":9178,"message":{"message_id":1,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"held carrier"}}`
			code, receipt := postProviderAliasUpdate(t, endpoint, "keyed-chat", secret, body)
			if code != http.StatusAccepted {
				t.Fatalf("held delivery was not acknowledged independently: %d %s", code, receipt)
			}
			var held string
			select {
			case held = <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("accepted receiver did not reach its barrier")
			}
			var before operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, served.Endpoint, "event.get", map[string]any{"event_id": held}, &before)
			if len(before.Deliveries) != 1 || before.Deliveries[0].Terminal || len(before.DeadLetters) != 0 {
				t.Fatalf("held native carrier=%+v", before)
			}
			initial := a9RootConstructionReceipt(t, rt.Pipeline, status.RunID)
			if code := first.stop(); code != 0 {
				t.Fatalf("held shutdown failed to join=%d\n%s", code, first.outputString())
			}
			select {
			case <-returned:
			default:
				t.Fatal("shutdown returned before accepted carrier cleanup")
			}
			var executions atomic.Int32
			opts.TestWorkflowNodeHandlerStartHook = func(_ context.Context, _ string, event events.Event) error {
				if event.ID() == held {
					executions.Add(1)
				}
				return nil
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			second, restarted := start()
			t.Cleanup(func() {
				if code := second.stop(); code != 0 {
					t.Errorf("held recovery shutdown=%d", code)
				}
			})
			a9RequireRootIngressSettlement(t, restarted.Endpoint, status.RunID, held)
			var after operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, restarted.Endpoint, "event.get", map[string]any{"event_id": held}, &after)
			if after.EventID != before.EventID || after.RunID != before.RunID || !after.CreatedAt.Equal(before.CreatedAt) || after.Deliveries[0].DeliveryID != before.Deliveries[0].DeliveryID || executions.Load() != 1 {
				t.Fatalf("restart substituted or duplicated accepted work: before=%+v after=%+v executions=%d", before, after, executions.Load())
			}
			if current := a9RootConstructionReceipt(t, servedTestProcessRuntime(t, second).Pipeline, status.RunID); !reflect.DeepEqual(initial, current) {
				t.Fatal("held restart replaced original creating evidence")
			}
			endpoint = strings.TrimSuffix(restarted.Endpoint, "/v1/rpc")
			code, duplicate := postProviderAliasUpdate(t, endpoint, "keyed-chat", secret, body)
			if code != http.StatusOK || !reflect.DeepEqual(a9AliasReceipt(t, receipt), a9AliasReceipt(t, duplicate)) || executions.Load() != 1 {
				t.Fatalf("held restart retry redelivered: %d %s -> %s executions=%d", code, receipt, duplicate, executions.Load())
			}
		})
	}
}

func TestA9KeyedIngressDesiredStateKeepsBindingAndReceiverDistinctBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			secret := a9SetRawIngressCredential(t)
			_, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyKeyedRootRawIngress(t))
			process, served := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("desired-state shutdown=%d", code)
				}
			})
			rt := servedTestProcessRuntime(t, process)
			statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 {
				t.Fatalf("binding=%+v err=%v", statuses, err)
			}
			status := statuses[0]
			endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/keyed-shop/partner"
			const body = `{"delivery_id":"same-provider-coordinate"}`
			code, receipt := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusAccepted {
				t.Fatalf("first binding acceptance=%d %s", code, receipt)
			}
			original := a9AliasReceipt(t, receipt)
			ids := original["event_ids"].([]any)
			a9RequireRootIngressSettlement(t, served.Endpoint, status.RunID, ids[0].(string))
			initial := a9RootConstructionReceipt(t, rt.Pipeline, status.RunID)
			suspended := invokeServedStandingOperation(t, served.Endpoint, "standing.suspend", status.ServiceID, "a9-keyed-suspend")
			if suspended.RunID != status.RunID || suspended.Generation != status.Generation || suspended.EffectiveState != "suspended" {
				t.Fatalf("suspension replaced receiver identity: %+v", suspended)
			}
			code, blocked := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusServiceUnavailable {
				t.Fatalf("suspended binding admitted a retry=%d %s", code, blocked)
			}
			resumed := invokeServedStandingOperation(t, served.Endpoint, "standing.resume", status.ServiceID, "a9-keyed-resume")
			if resumed.RunID != status.RunID || resumed.Generation != status.Generation || resumed.EffectiveState != "active" {
				t.Fatalf("resume replaced binding generation: %+v", resumed)
			}
			code, duplicate := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusOK || !reflect.DeepEqual(original, a9AliasReceipt(t, duplicate)) {
				t.Fatalf("resume redelivered old acceptance=%d %s", code, duplicate)
			}
			if current := a9RootConstructionReceipt(t, rt.Pipeline, status.RunID); !reflect.DeepEqual(current, initial) {
				t.Fatal("suspend/resume reinitialized the concrete receiver")
			}
			reset := invokeServedStandingOperation(t, served.Endpoint, "standing.reset", status.ServiceID, "a9-keyed-reset")
			if reset.RunID == status.RunID || reset.Generation != status.Generation+1 || reset.EffectiveState != "active" {
				t.Fatalf("reset did not advance exact binding generation: %+v", reset)
			}
			instances, err := rt.Pipeline.ListWorkflowInstances(t.Context(), reset.RunID)
			if err != nil || len(instances) != 0 {
				t.Fatalf("reset fabricated a keyed receiver before input: %+v err=%v", instances, err)
			}
			code, fresh := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusAccepted {
				t.Fatalf("successor could not construct from same provider coordinate=%d %s", code, fresh)
			}
			current := a9AliasReceipt(t, fresh)
			if current["publication_id"] == original["publication_id"] || current["run_id"] != reset.RunID || current["generation"] != float64(reset.Generation) {
				t.Fatalf("receiver/alias collapsed generation-scoped receipt: old=%+v new=%+v", original, current)
			}
			ids = current["event_ids"].([]any)
			a9RequireRootIngressSettlement(t, served.Endpoint, reset.RunID, ids[0].(string))
			if successor := a9RootConstructionReceipt(t, rt.Pipeline, reset.RunID); successor.CreatingInput.EventID == initial.CreatingInput.EventID || successor.Fields["processed_count"] != int64(0) {
				t.Fatalf("successor borrowed predecessor constructor: %+v", successor)
			}
		})
	}
}
