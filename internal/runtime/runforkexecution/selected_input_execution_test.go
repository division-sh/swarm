package runforkexecution

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedInputExecutionProbe struct {
	SelectedContractReplayPersistence
	mutate func(context.Context, []runfork.RunForkSelectedContractSourceEvent)
}

type selectedInputClaimProbe struct {
	SelectedContractRuntimeExecutionLifecycle
	mutate func(*effects.Authority)
}

func (p selectedInputClaimProbe) ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, execution runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (effects.Authority, error) {
	authority, err := p.SelectedContractRuntimeExecutionLifecycle.ClaimRunForkSelectedContractRuntimeExecution(ctx, execution, owner, lease)
	if err == nil {
		p.mutate(&authority)
	}
	return authority, err
}

func (p selectedInputExecutionProbe) LoadRunForkSelectedContractSourceEvents(ctx context.Context, sourceRun, forkRun string, ids []string, carriage semanticview.OriginalLoopCarriage) ([]runfork.RunForkSelectedContractSourceEvent, error) {
	events, err := p.SelectedContractReplayPersistence.LoadRunForkSelectedContractSourceEvents(ctx, sourceRun, forkRun, ids, carriage)
	if err == nil {
		p.mutate(ctx, events)
	}
	return events, err
}

// Inputs are committed through the semantic event owner. The fault is introduced
// only after real preparation/materialization, at the persisted evidence reader.
func TestSelectedInputExecutionEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fault := range []string{"control", "missing_input", "foreign_input", "source_event", "event_type", "payload", "missing_authority", "foreign_authority", "retired_execution"} {
			t.Run(backend+"/"+fault, func(t *testing.T) {
				var db *sql.DB
				var selected any
				var owner SelectedContractExecutionOwner
				var capabilityStore startupownership.Store
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, db, capabilityStore = s, storetest.Database(s), s
					owner = selectedContractSQLiteExecutionOwnerForTest(t, s)
				} else {
					_, db, _ = testutil.StartPostgres(t)
					s := storetest.AdmitPostgresRuntimeStore(t, db)
					selected, capabilityStore = s, s
					owner = selectedContractExecutionOwnerForTest(t, s)
				}
				ctx := runForkTestContext(t)
				root := runForkExecutionRepoRoot(t)
				loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: root, SourceRoot: filepath.Join(root, "tests/tier1-primitives/test-emits-multiple"), PlatformSpecPath: contracts.DefaultPlatformSpecFile(root)}
				loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				run, eventID, entity := uuid.NewString(), uuid.NewString(), uuid.NewString()
				input := eventtest.OperatorInjected(eventID, "item.received", "operator", "", []byte(`{}`), 0, run, nil, events.EventEnvelope{}, time.Unix(1700002200, 0).UTC())
				seedSelectedOperationSource(t, ctx, backend, db, selected, loaded, run, eventID, entity, input)
				claimed := false
				var claimedAuthority effects.Authority
				if fault == "missing_authority" || fault == "foreign_authority" || fault == "retired_execution" {
					owner.ports.runtimeExecution = selectedInputClaimProbe{SelectedContractRuntimeExecutionLifecycle: owner.ports.runtimeExecution, mutate: func(authority *effects.Authority) {
						claimed = true
						claimedAuthority = *authority
						if fault == "missing_authority" {
							*authority = effects.Authority{}
						} else if fault == "foreign_authority" {
							authority.SelectedFork.ForkRunID = run
						}
					}}
				}
				observed := false
				owner.ports.replay = selectedInputExecutionProbe{SelectedContractReplayPersistence: owner.ports.replay, mutate: func(callCtx context.Context, rows []runfork.RunForkSelectedContractSourceEvent) {
					if len(rows) != 1 {
						t.Fatalf("source input count %d", len(rows))
					}
					if _, ok := rows[0].InputPublication.Event(); !ok {
						t.Fatal("control did not exercise selected input validation")
					}
					observed = true
					switch fault {
					case "missing_input":
						rows[0].InputPublication = runfork.InputPublication{}
					case "foreign_input":
						other := eventtest.OperatorInjected(uuid.NewString(), "item.received", "operator", "", []byte(`{}`), 0, uuid.NewString(), nil, events.EventEnvelope{}, input.CreatedAt())
						other, err = eventtest.AdmitPayload(other, ".", "item.received")
						if err != nil {
							t.Fatal(err)
						}
						rows[0].InputPublication, err = runfork.InputPublicationFromEvent(other)
						if err != nil {
							t.Fatal(err)
						}
					case "source_event":
						rows[0].SourceEventID = uuid.NewString()
					case "event_type":
						rows[0].EventName = "item.processed"
					case "payload":
						rows[0].Payload = []byte(`{"changed":true}`)
					case "retired_execution":
						occurrence, ok := worklifetime.OccurrenceFromContext(callCtx)
						bound, selected := occurrence.(*worklifetime.SelectedForkOccurrence)
						if !ok || !selected {
							t.Fatal("missing exact execution occurrence")
						}
						if err := owner.ports.runtimeExecution.QuiesceRunForkSelectedContractRuntimeExecution(callCtx, claimedAuthority); err != nil {
							t.Fatal(err)
						}
						if err := owner.ports.runtimeExecution.CloseRunForkSelectedContractRuntimeExecution(callCtx, bound.Identity().ExecutionID); err != nil {
							t.Fatal(err)
						}
					}
				}}
				result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
					SourceRunID: run, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
					ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
					AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: selectedContractTestProcessCapability(t, ctx, capabilityStore)},
				})
				if !observed && !claimed {
					t.Fatalf("did not reach evidence consumer: %v", err)
				}
				if fault == "control" {
					if err != nil || len(result.ForkEvents) == 0 {
						t.Fatalf("input control: %+v %v", result, err)
					}
				} else {
					if err == nil {
						t.Fatal("stale selected evidence reached execution")
					}
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id<>$1 AND event_name='item.received'`, run).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatal("invalid selected input persisted delivery event")
					}
					if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id<>$1`, run).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatal("invalid selected input persisted receiver delivery")
					}
				}
			})
		}
	}
}
