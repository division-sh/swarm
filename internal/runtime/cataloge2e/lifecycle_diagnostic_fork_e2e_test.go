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

func (p *diagnosticForkActivationProbe) RequireRunForkSelectedContractExecutionSettlement(ctx context.Context, authority effects.Authority, allowed []string, source semanticview.Source) (err error) {
	defer func() { p.settled <- err }()
	forkRunID := authority.SelectedFork.ForkRunID
	var observations, pending int
	if err := p.h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1 AND projected_at IS NOT NULL`, forkRunID).Scan(&observations); err != nil {
		p.t.Fatal(err)
	}
	if err := p.h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1 AND projected_at IS NULL`, forkRunID).Scan(&pending); err != nil {
		p.t.Fatal(err)
	}
	if observations == 0 || pending != 0 {
		p.t.Fatalf("terminal diagnostic projection acknowledged=%d pending=%d", observations, pending)
	}
	if p.scenario != "clean" {
		var id string
		if err := p.h.db.QueryRowContext(ctx, `SELECT outbox_id FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1 AND projected_at IS NOT NULL ORDER BY created_at,outbox_id LIMIT 1`, forkRunID).Scan(&id); err != nil {
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
					h := newRuntimeHarnessForBackend(t, root, backend, true)
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
					result, err := forkexecution.ExecuteSelectedContractRunFork(ctx, forkexecution.SelectedContractExecutionRequest{
						SourceRunID: catalogRuntimeRunID, At: frontierID, AllowSourceFreeze: true,
						Owner: selectedContractExecutionOwnerForCatalogHarness(t, h, probe), SourceLoader: loader, ContractSelection: selection,
						AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg),
					})
					if err != nil || !result.Activation.Activated {
						t.Fatalf("activation acknowledgment: %+v err=%v", result.Activation, err)
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
