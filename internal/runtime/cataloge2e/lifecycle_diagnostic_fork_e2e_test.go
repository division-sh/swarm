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
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type diagnosticForkActivationProbe struct {
	forkexecution.SelectedContractForkLifecycle
	t        *testing.T
	h        *runtimeHarness
	scenario string
	checked  bool
}

func (p *diagnosticForkActivationProbe) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	p.checked = true
	var observations, pending int
	if err := p.h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1 AND projected_at IS NOT NULL`, req.ForkRunID).Scan(&observations); err != nil {
		p.t.Fatal(err)
	}
	if err := p.h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1 AND projected_at IS NULL`, req.ForkRunID).Scan(&pending); err != nil {
		p.t.Fatal(err)
	}
	if observations == 0 || pending != 0 {
		p.t.Fatalf("pre-activation diagnostic projection acknowledged=%d pending=%d", observations, pending)
	}
	if p.scenario != "clean" {
		var id string
		if err := p.h.db.QueryRowContext(ctx, `SELECT outbox_id FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1 AND projected_at IS NOT NULL ORDER BY created_at,outbox_id LIMIT 1`, req.ForkRunID).Scan(&id); err != nil {
			p.t.Fatal(err)
		}
		id = uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:lifecycle-diagnostic:"+id)).String()
		var parent string
		if err := p.h.db.QueryRowContext(ctx, `SELECT COALESCE(CAST(source_event_id AS TEXT),'') FROM events WHERE event_id=$1`, id).Scan(&parent); err != nil || parent != "" {
			p.t.Fatalf("expected exact parentless observation: parent=%s err=%v", parent, err)
		}
		if p.scenario == "corrupt_ack" {
			if _, err := p.h.db.ExecContext(ctx, `UPDATE events SET payload='{}',payload_bytes=$2 WHERE event_id=$1`, id, []byte(`{}`)); err != nil {
				p.t.Fatal(err)
			}
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
				detail["runtime_lineage_run_id"] = req.ForkRunID
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
			if err := selected.PersistRuntimeLog(ctx, runtimepkg.RuntimeLogPersistenceRecord{EventID: uuid.NewString(), CreatedAt: time.Now().UTC(), RunID: req.ForkRunID, ParentEventID: parent, ExecutionMode: executionmode.Live, Payload: admission.Payload(), PayloadAdmission: admission}); err != nil {
				p.t.Fatal(err)
			}
		}
	}
	return p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
}

func TestLifecycleDiagnosticForkActivationBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			for _, scenario := range []string{"clean", "forged_tags", "observation_child", "corrupt_ack"} {
				t.Run(scenario, func(t *testing.T) {
					root := selectedForkReadinessCatalogFixture(t, 1, "agent")
					h := newRuntimeHarnessForBackend(t, root, backend, true)
					selected := runScopedCatalogStore(t, h)
					path := "worker-flow/worker-001"
					entity := materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path)
					ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
					if _, err := selected.PauseRunControl(ctx, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID, Reason: "diagnostic proof", ControlledBy: "cataloge2e"}); err != nil {
						t.Fatal(err)
					}
					event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(path+"/worker.ready"), "cataloge2e", "", nil, 0, catalogRuntimeRunID,
						events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entity), path), eventtest.ConcreteTemplateRoutingSource("worker-flow", path, entity), time.Now().UTC())
					if err := h.rt.Bus.PublishAndWait(ctx, event); err != nil {
						t.Fatal(err)
					}
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
					probe := &diagnosticForkActivationProbe{SelectedContractForkLifecycle: forkStore, t: t, h: h, scenario: scenario}
					result, err := forkexecution.ExecuteSelectedContractRunFork(ctx, forkexecution.SelectedContractExecutionRequest{
						SourceRunID: catalogRuntimeRunID, At: event.ID(), ConfirmSourceFreeze: true,
						Owner: selectedContractExecutionOwnerForCatalogHarness(t, h, probe), SourceLoader: loader, ContractSelection: selection,
						AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg),
					})
					if !probe.checked {
						t.Fatalf("never reached final fork validation: %v", err)
					}
					if scenario == "clean" {
						if err != nil || !result.Activation.Activated {
							t.Fatalf("clean lifecycle activation: %v", err)
						}
					} else {
						if err == nil || result.Activation.Activated {
							t.Fatalf("hostile %s activated: %v", scenario, err)
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
