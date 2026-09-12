package serveapp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/servedparity"
)

type selectedResetPreparationProbe struct {
	entered, canceled, release chan struct{}
}

func (p *selectedResetPreparationProbe) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, _ runforkexecution.SelectedContractSourceLoadRequest) (runforkexecution.LoadedSelectedContractSource, error) {
	close(p.entered)
	<-ctx.Done()
	close(p.canceled)
	<-p.release
	return runforkexecution.LoadedSelectedContractSource{}, ctx.Err()
}

func TestSelectedResetPreparationAdmissionOrderBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			for _, first := range []string{"admission", "reset"} {
				t.Run(first, func(t *testing.T) {
					probe := &selectedResetPreparationProbe{make(chan struct{}), make(chan struct{}), make(chan struct{})}
					var release sync.Once
					defer release.Do(func() { close(probe.release) })
					supervisors := make(chan *processLifecycleSupervisor, 1)
					prior := buildSelectedAPICapabilities
					var captured sync.Once
					buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
						captured.Do(func() { supervisors <- req.RuntimeSupervisor })
						caps, err := prior(owner, req)
						if err == nil {
							executor := caps.RunFork.(apiv1.SelectedContractRunForkExecutor)
							executor.SourceLoader = probe
							caps.RunFork = executor
						}
						return caps, err
					}
					t.Cleanup(func() { buildSelectedAPICapabilities = prior })
					rt := startServedControlProofRuntime(t, backend)
					supervisor := <-supervisors
					predecessor := supervisor.selected
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
						"event_name": "item.received", "bundle_hash": rt.BundleHash,
						"payload": map[string]any{"item_id": "reset-preparation"}, "idempotency_key": "seed",
					})
					waitForServedEventPublishNodeDeliveryLifecycle(t, rt.DB, rt.Backend, seed.RunID, seed.EventID, rt.Probe)
					forkDone := make(chan error, 1)
					if first == "admission" {
						go func() {
							// Exercise the actual selected owner's admission without
							// holding SQLite's separately serialized API operation.
							ctx := worklifetime.WithProcess(context.Background(), supervisor.selectedProcess)
							prepared, err := predecessor.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{SourceLoader: probe})
							if prepared != nil {
								err = errors.Join(err, prepared.Close())
							}
							forkDone <- err
						}()
						select {
						case <-probe.entered:
						case err := <-forkDone:
							t.Fatalf("fork did not enter preparation: %v", err)
						case <-time.After(10 * time.Second):
							t.Fatal("preparation was not admitted")
						}
					}
					resetDone := make(chan servedJSONRPCEnvelope, 1)
					go func() {
						resetDone <- requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", map[string]any{
							"include_source_artifacts": false, "idempotency_key": "reset",
						})
					}()
					if first == "admission" {
						select {
						case <-probe.canceled:
						case <-time.After(10 * time.Second):
							t.Fatal("reset did not fence the accepted preparation")
						}
						select {
						case response := <-resetDone:
							t.Fatalf("reset skipped accepted preparation settlement: %+v", response)
						default:
						}
						var count int
						if err := rt.DB.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id=$1", seed.RunID).Scan(&count); err != nil || count != 1 {
							t.Fatalf("cleanup preceded selected join: count=%d err=%v", count, err)
						}
						release.Do(func() { close(probe.release) })
						if err := <-forkDone; !errors.Is(err, context.Canceled) {
							t.Fatalf("fenced preparation lost its cancellation: %v", err)
						}
					}
					select {
					case response := <-resetDone:
						if response.Error != nil {
							t.Fatalf("reset did not converge after settlement: %+v", response.Error)
						}
					case <-time.After(10 * time.Second):
						t.Fatal("reset did not finish")
					}
					ctx := worklifetime.WithProcess(context.Background(), supervisor.selectedProcess)
					if _, err := predecessor.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{SourceLoader: probe}); !errors.Is(err, worklifetime.ErrRetired) {
						t.Fatalf("late predecessor preparation was admitted: %v", err)
					}
					if first == "reset" {
						select {
						case <-probe.entered:
							t.Fatal("post-reset admission reached the source loader")
						default:
						}
					}
				})
			}
		})
	}
}
