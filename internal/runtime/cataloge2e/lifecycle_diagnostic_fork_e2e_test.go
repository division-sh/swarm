package cataloge2e

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type diagnosticForkActivationProbe struct {
	forkexecution.SelectedContractForkLifecycle
	forkexecution.SelectedContractRuntimeExecutionLifecycle
	t        *testing.T
	h        *runtimeHarness
	scenario string
	settled  chan error
}

func (p *diagnosticForkActivationProbe) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	authority, ok := effects.AuthorityFromContext(ctx)
	if !ok {
		p.t.Fatal("activation probe requires its exact selected authority")
	}
	if err := p.RequireRunForkSelectedContractExecutionSettlement(ctx, authority, req.AllowedSourceEventIDs, req.ExecutionSource); err != nil {
		return runfork.RunForkActivation{}, err
	}
	return p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
}

func (p *diagnosticForkActivationProbe) RequireRunForkSelectedContractExecutionSettlement(ctx context.Context, authority effects.Authority, allowed []string, source semanticview.Source) (err error) {
	defer func() { p.settled <- err }()
	forkRunID := authority.SelectedFork.ForkRunID
	var selected manager.AgentLifecycleDiagnosticPersistence = p.h.pg
	if p.h.sqlite != nil {
		selected = p.h.sqlite
	}
	storage, err := storetest.ReadSelectedForkLifecycleDiagnosticStorage(ctx, selected, forkRunID)
	if err != nil {
		p.t.Fatal(err)
	}
	pending, err := selected.ListPendingAgentLifecycleDiagnostics(ctx, 1000)
	if err != nil || len(pending) == 1000 {
		p.t.Fatalf("bounded diagnostic observation: pending=%d err=%v", len(pending), err)
	}
	for _, diagnostic := range pending {
		if diagnostic.Identity.RunID == forkRunID {
			p.t.Fatal("terminal diagnostic projection retains a pending exact-run observation")
		}
	}
	if len(storage.Receipts) == 0 {
		p.t.Fatal("terminal diagnostic projection lacks acknowledged observations")
	}
	for _, receipt := range storage.Receipts {
		if len(receipt.Projection) == 0 {
			p.t.Fatal("terminal diagnostic receipt lacks its projection")
		}
	}
	if p.scenario != "clean" {
		id := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:lifecycle-diagnostic:"+storage.Receipts[0].OutboxID)).String()
		observation, found := activityLineageEvents(p.t, ctx, p.h, forkRunID)[id]
		if !found || observation.SourceEventID != "" {
			p.t.Fatalf("expected exact parentless observation: found=%t observation=%+v", found, observation)
		}
		if p.scenario == "corrupt_ack" {
			(&activityLineageProofStore{t: p.t, h: p.h}).writePayload(ctx, id, map[string]any{})
		} else {
			var selected interface {
				PersistRuntimeLog(context.Context, runtimepkg.RuntimeLogPersistenceRecord) error
			} = p.h.pg
			if p.h.sqlite != nil {
				selected = p.h.sqlite
			}
			detail := map[string]any{"component": "diagnostic-forgery", "action": "not-lifecycle"}
			parent := ""
			if p.scenario == "observation_child" {
				parent = id
			} else {
				detail["runtime_lineage_owner"] = runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner
				detail["runtime_lineage_run_id"] = forkRunID
				detail["runtime_lineage_row_category"] = "diagnostic"
				detail["runtime_lineage_selected_fork_owner"] = runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner
				detail["runtime_lineage_classification"] = "fork_local"
				detail["runtime_lineage_selected_fork_context"] = true
			}
			payload, err := json.Marshal(map[string]any{"log_level": "info", "message": "forgery proof", "details": detail})
			if err != nil {
				p.t.Fatal(err)
			}
			admissionEvent := eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			admit := runtimepkg.NewRuntimePayloadAdmitter(nil, semanticview.Wrap(p.h.bundle), catalogSourceArtifactFact(p.t, p.h.bundle))
			admission, err := admit(ctx, admissionEvent, "")
			if err != nil {
				p.t.Fatal(err)
			}
			if err := selected.PersistRuntimeLog(ctx, runtimepkg.RuntimeLogPersistenceRecord{EventID: uuid.NewString(), CreatedAt: time.Now().UTC(), RunID: forkRunID, ParentEventID: parent, ExecutionMode: executionmode.Live, Payload: admission.Payload(), PayloadAdmission: admission}); err != nil {
				p.t.Fatal(err)
			}
		}
	}
	return p.SelectedContractRuntimeExecutionLifecycle.RequireRunForkSelectedContractExecutionSettlement(ctx, authority, allowed, source)
}

func TestLifecycleDiagnosticForkActivationBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			for _, scenario := range []string{"clean", "forged_tags", "observation_child", "corrupt_ack"} {
				t.Run(scenario, func(t *testing.T) {
					root := localReadinessFixture(t, 1, "agent")
					h := newLiveForkCatalogHarness(t, root, backend)
					ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
					frontierID := activateLocalReadinessFrontier(t, ctx, h, "worker.ready")
					var sourceStore interface {
						storetest.DurableDataCatalogStore
						forkexecution.SourceArtifactSelectedContractSourceStore
					} = h.pg
					var forkStore forkexecution.SelectedContractForkLifecycle = h.pg
					if h.sqlite != nil {
						sourceStore, forkStore = h.sqlite, h.sqlite
					}
					loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, sourceStore)
					installCatalogSelectedSourceTopology(t, ctx, h, loaded)
					cfg := testRuntimeConfig()
					cfg.LLM.Backend = "anthropic"
					probe := &diagnosticForkActivationProbe{
						SelectedContractForkLifecycle:             forkStore,
						SelectedContractRuntimeExecutionLifecycle: forkStore.(forkexecution.SelectedContractRuntimeExecutionLifecycle),
						t: t, h: h, scenario: scenario, settled: make(chan error, 1),
					}
					result, err := executeCatalogLiveFork(t, h, ctx, forkexecution.SelectedContractExecutionRequest{
						SourceRunID: catalogRuntimeRunID, At: frontierID, AllowSourceFreeze: true,
						Owner: selectedContractExecutionOwnerForCatalogHarness(t, h, probe), SourceLoader: loader, ContractSelection: selection,
						AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg),
					})
					if scenario == "clean" && (err != nil || !result.Activation.Activated) {
						t.Fatalf("activation acknowledgment: %+v err=%v", result.Activation, err)
					}
					if scenario != "clean" && (err == nil || result.Activation.Activated) {
						t.Fatalf("hostile diagnostic crossed the live handoff: %+v err=%v", result.Activation, err)
					}
					select {
					case err = <-probe.settled:
					case <-time.After(10 * time.Second):
						t.Fatal("never reached native final settlement")
					}
					if scenario == "clean" {
						if err != nil {
							t.Fatalf("clean lifecycle settlement: %v", err)
						}
					} else {
						if err == nil {
							t.Fatalf("hostile %s settled", scenario)
						}
						if scenario != "corrupt_ack" && !strings.Contains(err.Error(), "fork_events_not_selected_contract_lineage") {
							t.Fatalf("wrong refusal: %v", err)
						}
					}
				})
			}
		})
	}
}
