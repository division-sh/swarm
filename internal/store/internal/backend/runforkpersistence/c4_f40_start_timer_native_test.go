package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

// Native canonical writer/readback and schema acceptance of an admitted
// component plan. Source capture and selected fork construction are deliberately
// not claimed: the real creation-start capture has a separate runtime proof.
func TestC4F40TypedStartTimerNativeComponentBothStores(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointRunStart, runfork.RunForkPointEvent} {
			t.Run(dialect+"/"+string(kind), func(t *testing.T) {
				source := workflowTimerProjectionSource(t, true)
				if dialect == "postgres" {
					// JSONB readback retains this spelling; lineage assertions stay exact.
					source.Payload = []byte(`{"retained": true}`)
				}
				point := runfork.RunForkPoint{Kind: kind, Revision: 1}
				if kind == runfork.RunForkPointEvent {
					point.EventID = "55555555-5555-4555-8555-555555555555"
				}
				plan := workflowTimerMaterializerPlan(source, point)
				bornAt := source.CreatedAt.Add(2 * time.Hour)
				f := openTimerHistoryNativeFixture(t, dialect)
				sourceCtx := f.runContext(t, source.RunID, source.CreatedAt)
				c4PersistNativeComponentSourceTimer(t, f, sourceCtx, source, point)
				ctx := f.runContext(t, workflowTimerProjectionChildRun, bornAt)
				refusal := errors.New("C4 timer component rollback after native write")
				rolledBack := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error) {
					if _, err := materializeRunForkWorkflowTimers(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, f.pipeline, bornAt); err != nil {
						return runfork.RunForkReplayResumeAdmission{}, err
					}
					rows, err := f.pipeline.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, workflowTimerProjectionChildRun)
					if err != nil || len(rows) != 1 {
						return runfork.RunForkReplayResumeAdmission{}, errors.New("native write not visible before rollback")
					}
					return runfork.RunForkReplayResumeAdmission{}, refusal
				})
				if rolledBack.Acknowledged() || !errors.Is(rolledBack.Err(), refusal) {
					t.Fatalf("rollback boundary: acknowledged=%t err=%v", rolledBack.Acknowledged(), rolledBack.Err())
				}
				var retained pipeline.WorkflowTimerActivation
				read := func(want int) {
					t.Helper()
					result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error) {
						if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
							count, err := runforkrevision.CountWorkflowTimerRevisionFactsForTest(ctx, tx, workflowTimerProjectionChildRun)
							if err != nil {
								return err
							}
							if count != want {
								return errors.New("native timer history changed on rollback/replay")
							}
							return nil
						}); err != nil {
							return runfork.RunForkReplayResumeAdmission{}, err
						}
						rows, err := f.pipeline.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, workflowTimerProjectionChildRun)
						if err != nil {
							return runfork.RunForkReplayResumeAdmission{}, err
						}
						if len(rows) != want || (want == 1 && !reflect.DeepEqual(rows[0], retained)) {
							return runfork.RunForkReplayResumeAdmission{}, errors.New("native inventory changed cardinality or exact lineage")
						}
						return runfork.RunForkReplayResumeAdmission{}, nil
					})
					if !result.Acknowledged() || result.Err() != nil {
						t.Fatalf("require-only inventory: %v", result.Err())
					}
				}
				read(0)
				for pass := 0; pass < 3; pass++ {
					result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error) {
						if _, err := materializeRunForkWorkflowTimers(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, f.pipeline, bornAt); err != nil {
							return runfork.RunForkReplayResumeAdmission{}, err
						}
						rows, err := f.pipeline.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, workflowTimerProjectionChildRun)
						if err != nil || len(rows) != 1 {
							return runfork.RunForkReplayResumeAdmission{}, errors.New("native timer missing on retry")
						}
						if pass == 0 {
							retained = rows[0]
						} else if !reflect.DeepEqual(rows[0], retained) {
							return runfork.RunForkReplayResumeAdmission{}, errors.New("native retry reset retained timer")
						}
						_, err = readRunForkWorkflowTimerInventory(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, f.pipeline, bornAt, runForkWorkflowTimerAtCut)
						return runfork.RunForkReplayResumeAdmission{}, err
					})
					if !result.Acknowledged() || result.Err() != nil {
						t.Fatalf("native component pass=%d: %v", pass, result.Err())
					}
					read(1)
				}
				if retained.ForkedFromPointKind != kind || retained.ForkedFromPointRevision != 1 || retained.ForkedFromEventID != point.EventID ||
					retained.SourceTimerID != source.Ref.ActivationID || retained.ForkedFromRunID != source.RunID ||
					!retained.SourceArmedAt.Equal(source.CreatedAt) || !retained.FireAt.Equal(source.FireAt) || !retained.CreatedAt.Equal(bornAt) {
					t.Fatalf("native typed point reset original timer coordinates: %+v", retained)
				}
				for _, tc := range []struct {
					name string
					edit func(*pipeline.WorkflowTimerActivation)
				}{
					{"missing_point", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointKind = "" }},
					{"missing_revision", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointRevision = 0 }},
					{"missing_arm", func(a *pipeline.WorkflowTimerActivation) { a.SourceArmedAt = time.Time{} }},
					{"missing_source_timer", func(a *pipeline.WorkflowTimerActivation) { a.SourceTimerID = "" }},
					{"missing_source_run", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromRunID = "" }},
					{"missing_owner", func(a *pipeline.WorkflowTimerActivation) { a.ReconstructionOwner = "" }},
					{"unexpected_event", func(a *pipeline.WorkflowTimerActivation) {
						a.ForkedFromEventID = "77777777-7777-4777-8777-777777777777"
					}},
					{"foreign_source", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromRunID = "66666666-6666-4666-8666-666666666666" }},
					{"wrong_revision", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointRevision++ }},
					{"wrong_point", func(a *pipeline.WorkflowTimerActivation) {
						a.ForkedFromPointKind = forkpoint.DeploymentRevision
						a.ForkedFromEventID = ""
					}},
				} {
					t.Run(tc.name, func(t *testing.T) {
						bad := retained.Canonical()
						tc.edit(&bad)
						result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error) {
							return runfork.RunForkReplayResumeAdmission{}, f.pipeline.MaterializeRunForkWorkflowTimerTx(ctx, attempt, bad, false)
						})
						if result.Acknowledged() || result.Err() == nil {
							t.Fatal("native canonical replay admitted missing/foreign/wrong typed lineage")
						}
						read(1)
					})
				}
			})
		}
	}
}

// This post-creation fixture construction supplies the physical source FK;
// it does not certify the component plan's purported source run-start cut.
func c4NativeComponentSourceConstruction(source pipeline.WorkflowTimerActivation) pipeline.FlowInstanceActivationPlan {
	identity := flowidentity.Stored(nil, source.Route.ScopeKey, source.Route.InstancePath, source.Route.InstanceID, source.EntityID, "")
	return pipeline.FlowInstanceActivationPlan{
		Identity: identity, OccurredAt: source.CreatedAt,
		Instance: pipeline.WorkflowInstance{
			InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID,
			WorkflowName: identity.TemplateID, WorkflowVersion: "c4-source-component", Mode: "template", Status: "active",
			CurrentState: "waiting", StageDefined: true, EnteredStageAt: source.CreatedAt, CreatedAt: source.CreatedAt,
		},
		Readiness: pipeline.DynamicFlowRuntimeReadinessPlan{
			Identity: identity, RunID: source.RunID, BundleHash: sourceartifactfixture.BundleHash,
			WorkflowVersion: "c4-source-component", ExecutionMode: source.ExecutionMode,
		},
		Lifecycle: pipeline.WorkflowLifecycleMutationPlan{Timers: []pipeline.WorkflowTimerMutation{{Kind: pipeline.WorkflowTimerMutationInsert, Activation: source}}},
	}
}

func c4PersistNativeComponentSourceTimer(t *testing.T, f *timerHistoryNativeFixture, ctx context.Context, source pipeline.WorkflowTimerActivation, point runfork.RunForkPoint) {
	t.Helper()
	var eventRecord eventrecord.Record
	if point.Kind == runfork.RunForkPointEvent {
		// Supply the component's real physical event FK, not a start-cut event
		// or a claim that this fixture captures the actual historical cut.
		event := eventtest.ExistingRunRootIngress(point.EventID, "c4.component.cut", "c4-component", "", json.RawMessage(`{}`), 0,
			source.RunID, events.EventEnvelope{}, source.CreatedAt)
		binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
			BundleHash: sourceartifactfixture.BundleHash, FlowID: ".", EventKey: string(event.Type()),
			SchemaDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000", SchemaClass: events.PayloadSchemaSchemaLess,
		})
		if err != nil {
			t.Fatal(err)
		}
		payload, err := events.NewPayloadAdmission(event.Payload(), binding)
		if err != nil {
			t.Fatal(err)
		}
		event, err = events.ApplyPayloadAdmission(event, payload)
		if err != nil {
			t.Fatal(err)
		}
		admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
		if err != nil {
			t.Fatal(err)
		}
		ledger, err := events.NewConnectEvaluationLedger(nil)
		if err != nil {
			t.Fatal(err)
		}
		settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
		if err != nil {
			t.Fatal(err)
		}
		eventRecord, err = eventrecord.FromAdmitted(admitted, settlement)
		if err != nil {
			t.Fatal(err)
		}
	}
	owner := f.pipeline.(interface {
		CommitFlowInstanceActivationsTx(context.Context, *mutationprotocol.Attempt, []pipeline.FlowInstanceActivationPlan) ([]pipeline.CommittedFlowInstanceActivation, error)
	})
	write := func(ctx context.Context, attempt *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error) {
		constructed, err := owner.CommitFlowInstanceActivationsTx(ctx, attempt, []pipeline.FlowInstanceActivationPlan{c4NativeComponentSourceConstruction(source)})
		if err != nil {
			return runfork.RunForkReplayResumeAdmission{}, err
		}
		if len(constructed) != 1 || !constructed[0].Created {
			return runfork.RunForkReplayResumeAdmission{}, errors.New("component source timer requires its canonical construction")
		}
		if eventRecord.EventID != "" {
			var inserted bool
			if f.postgres != nil {
				inserted, err = eventrecordpostgres.Insert(ctx, attempt, eventRecord)
			} else {
				inserted, err = eventrecordsqlite.Insert(ctx, attempt, eventRecord)
			}
			if err != nil {
				return runfork.RunForkReplayResumeAdmission{}, err
			}
			if !inserted {
				return runfork.RunForkReplayResumeAdmission{}, errors.New("component event reference was not created")
			}
		}
		return runfork.RunForkReplayResumeAdmission{}, nil
	}
	var result mutationprotocol.Result[runfork.RunForkReplayResumeAdmission]
	if f.postgres != nil {
		result = mutationprotocol.RunPostgres(ctx, f.postgres, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	} else {
		result = mutationprotocol.RunSQLite(ctx, f.sqlite, "C4 native component source timer construction", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	}
	if !result.Acknowledged() || result.Err() != nil {
		t.Fatalf("canonical component source timer setup: %v", result.Err())
	}
	readback := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error) {
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			snapshot, err := loadRunForkRevisionSnapshot(ctx, tx, source.RunID, 2)
			if err != nil {
				return err
			}
			if len(snapshot.Timers) != 1 {
				return errors.New("component source timer absent from canonical construction history")
			}
			actual, err := workflowTimerActivationFromSnapshot(snapshot.Timers[0].TimerSnapshot)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(actual.Canonical(), source.Canonical()) {
				return fmt.Errorf("component source timer changed during canonical construction: got=%+v payload=%q want=%+v payload=%q", actual.Canonical(), actual.Payload, source.Canonical(), source.Payload)
			}
			return nil
		})
		return runfork.RunForkReplayResumeAdmission{}, err
	})
	if !readback.Acknowledged() || readback.Err() != nil {
		t.Fatalf("canonical component source timer readback: %v", readback.Err())
	}
}

func TestC4F40TypedStartTimerCodecAndPreparationHash(t *testing.T) {
	snapshot := inheritedTimerSnapshot(t)
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeRunForkTimerSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	source, err := workflowTimerActivationFromSnapshot(decoded)
	if err != nil {
		t.Fatal(err)
	}
	plan := workflowTimerMaterializerPlan(source, runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 7}).WithHistoricalEvents(7, nil)
	fingerprint := func(plan runfork.RunForkPlan) string {
		t.Helper()
		hash, err := runfork.SelectedPreparationPlanFingerprint(plan, runfork.RunForkContractFrontierAdmission{}, runfork.RunForkSelectedContractRecipientPlanning{}, "c4-timer")
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	want := fingerprint(plan)
	for _, tc := range []struct {
		name string
		edit func(*pipeline.WorkflowTimerActivation)
	}{
		{"source_timer", func(a *pipeline.WorkflowTimerActivation) { a.SourceTimerID = "77777777-7777-4777-8777-777777777777" }},
		{"source_run", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromRunID = "88888888-8888-4888-8888-888888888888" }},
		{"source_arm", func(a *pipeline.WorkflowTimerActivation) { a.SourceArmedAt = a.SourceArmedAt.Add(-time.Microsecond) }},
		{"point_revision", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointRevision++ }},
		{"point_kind", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointKind = forkpoint.DeploymentRevision }},
		{"owner", func(a *pipeline.WorkflowTimerActivation) { a.ReconstructionOwner = "other-cut" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := source.Canonical()
			tc.edit(&changed)
			if err := changed.Validate(); err != nil {
				t.Fatalf("hash counterexample must be a valid distinct lineage: %v", err)
			}
			candidate := plan
			candidate.WorkflowTimers = []pipeline.WorkflowTimerActivationPersistenceRecord{changed.PersistenceRecord()}
			if fingerprint(candidate) == want {
				t.Fatal("preparation hash omitted typed timer lineage")
			}
		})
	}
	if fingerprint(plan) != want {
		t.Fatal("counterexamples mutated fixed source evidence")
	}
}
