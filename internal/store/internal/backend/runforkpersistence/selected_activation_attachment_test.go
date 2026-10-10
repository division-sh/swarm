package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/google/uuid"
)

type selectedAttachmentFixture struct {
	evidence     runForkSelectedContractActivationEvidence
	record       selectedRecoveryRecord
	authority    effects.Authority
	snapshot     runlifecycle.Snapshot
	execution    runfork.SelectedContractRuntimeExecution
	declarations agenttopology.SelectedDeclarationPlan
}

func newSelectedAttachmentFixture(t *testing.T, kind runfork.RunForkPointKind) selectedAttachmentFixture {
	t.Helper()
	now := time.Unix(100, 0).UTC()
	source, child := uuid.NewString(), uuid.NewString()
	bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	point := runfork.RunForkPoint{Kind: kind, Revision: 1}
	if kind == runfork.RunForkPointEvent {
		point.EventID = uuid.NewString()
	}
	binding, err := normalizeRunForkSelectedContractBinding(runfork.RunForkSelectedContractBindingRequest{
		ForkRunID: child, SourceRunID: source, ForkPoint: point,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	binding.BindingID = uuid.NewString()
	declarations, err := agenttopology.NewSelectedDeclarationPlan(bundle, []agenttopology.DesiredAgent{})
	if err != nil {
		t.Fatal(err)
	}
	preparation := runfork.SelectedForkPreparationBinding{
		ForkRunID: child,
		SelectedForkPreparation: runfork.SelectedForkPreparation{
			PreparationID: uuid.NewString(), ProcessGeneration: 1,
			SourceRunID: source, ForkPoint: point, ForkEventID: point.EventID,
			DeclarationPlanFingerprint: declarations.Revision,
			Coordinates: managedcapabilities.SelectedForkPreparationCoordinates{
				ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "process", ProcessBootID: uuid.NewString(),
				BundleHash: bundle, SourceFingerprint: strings.Repeat("b", 64),
				AdmittedPlanFingerprint: strings.Repeat("c", 64), ConfigurationFingerprint: strings.Repeat("d", 64),
				CatalogFingerprint: strings.Repeat("e", 64),
			},
			Actors: []runfork.SelectedForkPreparedActor{},
		},
	}
	fingerprint, err := preparation.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	execution := runfork.SelectedContractRuntimeExecution{
		ExecutionID: uuid.NewString(), ForkRunID: child, SourceRunID: source,
		ForkPoint: point, ForkEventID: point.EventID, Generation: 2, FenceGeneration: 3,
		AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors",
		EffectiveConfigFingerprint: "config", DeclarationPlanFingerprint: declarations.Revision,
		PreparationFingerprint: fingerprint,
	}
	execution.ExecutableCoordinateFingerprint, err = execution.CoordinateFingerprint()
	if err != nil {
		t.Fatal(err)
	}
	origin, err := runlifecycle.ForkMaterializationRunOrigin(source, kind, point.Revision, point.EventID)
	if err != nil {
		t.Fatal(err)
	}
	return selectedAttachmentFixture{
		evidence: runForkSelectedContractActivationEvidence{
			lineage: runForkActivationLineage{
				ForkRunID: child, SourceRunID: source, ForkPoint: point, ForkEventID: point.EventID,
				ForkEventRevision: 1, ForkBundleHash: bundle, ForkStatus: runfork.RunForkMaterializedStatus,
				SourceRunStatus: "running", EntityIDs: []string{child},
			},
			binding: binding,
			plan: runfork.RunForkPlan{SourceRunID: source, ForkPoint: point,
				RouteHistory: runfork.RunForkRouteHistoryProjection{State: runfork.RunForkRouteHistoryNotApplicable},
				Entities: []runfork.RunForkEntityState{{EntityID: source, MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
					Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
					FlowInstance: source,
				}}}},
			request: runfork.RunForkSelectedContractExecutionActivateRequest{ForkRunID: child, AllowSourceFreeze: true},
		},
		record: selectedRecoveryRecord{
			SelectedForkRecoveryResult: runfork.SelectedForkRecoveryResult{RunID: child, ExecutionID: execution.ExecutionID},
			binding:                    binding, preparation: preparation, state: "running", hasExecution: true,
		},
		authority: effects.Authority{
			Kind: effects.AuthoritySelectedContractFork, ID: execution.ExecutionID, ExecutionOwner: "executor",
			LeaseExpiresAt: now.Add(time.Hour), FenceGeneration: execution.FenceGeneration, ExecutionMode: executionmode.Live,
			SelectedFork: effects.SelectedContractForkAuthority{
				ExecutionID: execution.ExecutionID, ForkRunID: child, Generation: execution.Generation,
				AdmissionFingerprint: execution.AdmissionFingerprint, ContainerPlanFingerprint: execution.ContainerPlanFingerprint,
				ActorCensusFingerprint: execution.ActorCensusFingerprint, EffectiveConfigFingerprint: execution.EffectiveConfigFingerprint,
			},
		},
		snapshot:  runlifecycle.Snapshot{RunID: child, State: runlifecycle.StatePaused, BundleHash: bundle, Origin: origin, StartedAt: now},
		execution: execution, declarations: declarations,
	}
}

func TestSelectedActivationAttachmentRequiresExactConstructedCensus(t *testing.T) {
	f := newSelectedAttachmentFixture(t, runfork.RunForkPointRunStart)
	if err := validateSelectedContractStagedConstruction(f.evidence); err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]string{nil, {uuid.NewString()}, {f.evidence.lineage.ForkRunID, uuid.NewString()}, {f.evidence.lineage.ForkRunID, f.evidence.lineage.ForkRunID}} {
		f.evidence.lineage.EntityIDs = ids
		if err := validateSelectedContractStagedConstruction(f.evidence); err == nil {
			t.Fatalf("inexact child census admitted: %v", ids)
		}
	}
}

func TestSelectedActivationConstructionCensusIsOrderIndependent(t *testing.T) {
	f := newSelectedAttachmentFixture(t, runfork.RunForkPointRunStart)
	member := uuid.NewString()
	f.evidence.plan.Entities = append(f.evidence.plan.Entities, runfork.RunForkEntityState{
		EntityID: member, MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
			Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
			FlowInstance: "orders/member", FlowTemplate: "orders",
		},
	})
	root := f.evidence.lineage.ForkRunID
	for _, ids := range [][]string{{root, member}, {member, root}} {
		for i := 0; i < 16; i++ {
			f.evidence.lineage.EntityIDs = ids
			before := append([]string(nil), ids...)
			if err := validateSelectedContractStagedConstruction(f.evidence); err != nil {
				t.Fatalf("same exact construction inventory changed with order %v: %v", ids, err)
			}
			if !reflect.DeepEqual(ids, before) {
				t.Fatal("construction validation mutated retained native inventory")
			}
		}
	}
	for _, ids := range [][]string{{root}, {member}, {root, root}, {member, member}, {root, uuid.NewString()}, {root, member, uuid.NewString()}, {" " + root, member}} {
		f.evidence.lineage.EntityIDs = ids
		if err := validateSelectedContractStagedConstruction(f.evidence); err == nil {
			t.Fatalf("inexact construction inventory admitted: %v", ids)
		}
	}
}

func TestSelectedActivationAttachmentRequiresExactAuthorityAndCancellation(t *testing.T) {
	f := newSelectedAttachmentFixture(t, runfork.RunForkPointEvent)
	ctx := effects.WithAuthority(context.Background(), f.authority)
	got, err := selectedContractActivationAuthority(ctx, f.evidence.lineage.ForkRunID)
	if err != nil || !reflect.DeepEqual(got, f.authority) {
		t.Fatalf("authority=%+v err=%v", got, err)
	}
	for _, candidate := range []context.Context{context.Background(), effects.WithAuthority(context.Background(), effects.Authority{})} {
		if _, err := selectedContractActivationAuthority(candidate, f.evidence.lineage.ForkRunID); err == nil {
			t.Fatal("missing/invalid authority admitted")
		}
	}
	if _, err := selectedContractActivationAuthority(ctx, uuid.NewString()); err == nil {
		t.Fatal("another child admitted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := selectedContractActivationAuthority(canceled, f.evidence.lineage.ForkRunID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestSelectedActivationAttachmentAllowsOwedWorkAndSourceAdvancement(t *testing.T) {
	for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointEvent, runfork.RunForkPointRunStart, runfork.RunForkPointDeploymentRevision} {
		t.Run(string(kind), func(t *testing.T) {
			f := newSelectedAttachmentFixture(t, kind)
			f.evidence.plan.SourceRunStatus = "completed"
			ended := time.Unix(200, 0).UTC()
			f.evidence.plan.SourceRunEndedAt = &ended
			// No executed event or delivered receiver is needed to attach. Current
			// source status is not part of immutable prepared-execution identity.
			if err := validateSelectedContractPreparedAttachment(f.record, f.authority, f.evidence); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSelectedActivationAttachmentRejectsCrossedEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*selectedAttachmentFixture)
	}{
		{"missing", func(f *selectedAttachmentFixture) { f.record.hasExecution = false }},
		{"prepared", func(f *selectedAttachmentFixture) { f.record.state = "prepared" }},
		{"quiesced", func(f *selectedAttachmentFixture) { f.record.state = "quiesced" }},
		{"failed", func(f *selectedAttachmentFixture) { f.record.state = "failed" }},
		{"closed", func(f *selectedAttachmentFixture) { f.record.state = "closed" }},
		{"execution", func(f *selectedAttachmentFixture) { f.record.ExecutionID = uuid.NewString() }},
		{"source", func(f *selectedAttachmentFixture) { f.record.preparation.SourceRunID = uuid.NewString() }},
		{"child", func(f *selectedAttachmentFixture) { f.record.preparation.ForkRunID = uuid.NewString() }},
		{"point", func(f *selectedAttachmentFixture) { f.record.preparation.ForkPoint.Revision++ }},
		{"event", func(f *selectedAttachmentFixture) { f.record.preparation.ForkEventID = uuid.NewString() }},
		{"bundle", func(f *selectedAttachmentFixture) { f.record.preparation.Coordinates.BundleHash = "other" }},
		{"inputs", func(f *selectedAttachmentFixture) {
			f.evidence.request.AllowedSourceEventIDs = []string{uuid.NewString()}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newSelectedAttachmentFixture(t, runfork.RunForkPointEvent)
			test.mutate(&f)
			if err := validateSelectedContractPreparedAttachment(f.record, f.authority, f.evidence); err == nil {
				t.Fatal("crossed evidence admitted")
			}
		})
	}
}

func expectSelectedAttachmentBinding(mock sqlmock.Sqlmock, binding runfork.RunForkSelectedContractBinding) {
	mock.ExpectQuery(`FROM run_fork_selected_contract_bindings`).WithArgs(binding.ForkRunID).
		WillReturnRows(sqlmock.NewRows([]string{"binding", "child", "source", "kind", "revision", "event", "mode", "bundle", "created"}).
			AddRow(binding.BindingID, binding.ForkRunID, binding.SourceRunID, binding.ForkPoint.Kind, binding.ForkPoint.Revision,
				binding.ForkEventID, binding.ContractSelection.Mode, binding.ContractSelection.BundleHash, binding.CreatedAt))
}

type selectedAttachmentAuthorityProbe struct {
	got     effects.Authority
	err     error
	live    effects.Authority
	liveErr error
}

func (p *selectedAttachmentAuthorityProbe) RequireCurrentExternalEffectAuthorityTx(_ context.Context, _ *sql.Tx, authority effects.Authority) error {
	p.got = authority
	return p.err
}

func (p *selectedAttachmentAuthorityProbe) RequireCompletionAuthorityNoLiveAttemptsTx(_ context.Context, _ *sql.Tx, authority effects.Authority) error {
	p.live = authority
	return p.liveErr
}

func expectSelectedAttachmentRecord(t *testing.T, mock sqlmock.Sqlmock, f selectedAttachmentFixture, sqlite, lock bool) {
	t.Helper()
	expectSelectedAttachmentBinding(mock, f.evidence.binding)
	mock.ExpectQuery(`SELECT CAST\(operation_id AS TEXT\) FROM run_fork_operations`).WithArgs(f.execution.ForkRunID).
		WillReturnRows(sqlmock.NewRows([]string{"operation"}))
	prep, err := canonicaljson.Bytes(f.record.preparation)
	if err != nil {
		t.Fatal(err)
	}
	declarations, err := canonicaljson.Bytes(f.declarations)
	if err != nil {
		t.Fatal(err)
	}
	query := `FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=\$1 ORDER BY generation DESC LIMIT 1`
	if !sqlite && lock {
		query += ` FOR UPDATE`
	}
	e := f.execution
	mock.ExpectQuery(query).WithArgs(e.ForkRunID).WillReturnRows(sqlmock.NewRows([]string{
		"execution", "state", "preparation", "preparation_fingerprint", "failure", "source", "binding", "kind", "revision", "event",
		"generation", "fence", "coordinate", "admission", "container", "actors", "config", "declaration_fingerprint", "declarations",
	}).AddRow(e.ExecutionID, f.record.state, prep, e.PreparationFingerprint, nil, e.SourceRunID, f.evidence.binding.BindingID,
		e.ForkPoint.Kind, e.ForkPoint.Revision, e.ForkEventID, e.Generation, e.FenceGeneration, e.ExecutableCoordinateFingerprint,
		e.AdmissionFingerprint, e.ContainerPlanFingerprint, e.ActorCensusFingerprint, e.EffectiveConfigFingerprint, e.DeclarationPlanFingerprint, declarations))
}

func TestSelectedActivationAttachmentRetainedEvidenceBeforeCurrentFence(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		t.Run(map[bool]string{false: "postgres", true: "sqlite"}[sqlite], func(t *testing.T) {
			f := newSelectedAttachmentFixture(t, runfork.RunForkPointRunStart)
			tx, mock := startSnapshotTransaction(t)
			expectSelectedAttachmentRecord(t, mock, f, sqlite, true)
			a := f.authority
			mock.ExpectExec(`UPDATE run_fork_selected_contract_runtime_executions SET updated_at=updated_at`).
				WithArgs(a.SelectedFork.ExecutionID, a.SelectedFork.ForkRunID, a.SelectedFork.Generation,
					a.ExecutionOwner, a.FenceGeneration, a.SelectedFork.AdmissionFingerprint,
					a.SelectedFork.ContainerPlanFingerprint, a.SelectedFork.ActorCensusFingerprint, a.SelectedFork.EffectiveConfigFingerprint).
				WillReturnResult(sqlmock.NewResult(0, 0))
			var owner selectedContractCurrentAuthorityOwner = &effectpersistence.EffectPostgresOwner{}
			if sqlite {
				owner = &effectpersistence.EffectSQLiteOwner{}
			}
			ctx := effects.WithAuthority(context.Background(), f.authority)
			if err := requireSelectedContractPreparedAttachmentTx(ctx, tx, f.snapshot, f.evidence, sqlite, owner); err == nil {
				t.Fatalf("current fence not required: %v", err)
			}
		})
	}
}

func TestSelectedActivationSettlementEmptyInputRequiresNoInventedFeed(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres], func(t *testing.T) {
			tx, mock := startSnapshotTransaction(t)
			child := uuid.NewString()
			mock.ExpectQuery(`SELECT COUNT\(\*\) FROM fan_out_intents`).WithArgs(child).
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			mock.ExpectQuery(`SELECT event_id, task_id FROM events`).WithArgs(child).
				WillReturnRows(sqlmock.NewRows([]string{"event", "task"}))
			expectEmptyGenericOccurrenceCensus(mock, child)
			mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM fan_out_intents`).WithArgs(child).
				WillReturnRows(sqlmock.NewRows([]string{"present"}).AddRow(false))
			for _, table := range []string{"event_deliveries", "events", "agent_sessions", "agent_conversation_audits", "agent_turns"} {
				mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM ` + table + ` WHERE run_id`).WithArgs(child).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
			}
			var err error
			if postgres {
				err = (&RunForkPostgresOwner{}).requireRunForkSelectedContractExecutionSettlementTx(context.Background(), tx, child, nil, nil)
			} else {
				err = (&RunForkSQLiteOwner{}).requireRunForkSelectedContractExecutionSettlementTx(context.Background(), tx, child, nil, nil)
			}
			if err != nil {
				t.Fatalf("explicit empty input settlement: %v", err)
			}
		})
	}
}

func TestSelectedActivationSettlementAllowsTerminalChildWithExactFence(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		for _, live := range []bool{false, true} {
			t.Run(map[bool]string{false: "postgres", true: "sqlite"}[sqlite]+map[bool]string{false: "/settled", true: "/live_attempt"}[live], func(t *testing.T) {
				f := newSelectedAttachmentFixture(t, runfork.RunForkPointRunStart)
				f.snapshot.State = runlifecycle.StateCompleted
				tx, mock := startSnapshotTransaction(t)
				expectSelectedAttachmentBinding(mock, f.evidence.binding)
				expectSelectedAttachmentRecord(t, mock, f, sqlite, true)
				owner := &selectedAttachmentAuthorityProbe{}
				if live {
					owner.liveErr = errors.New("live completion attempt")
				}
				err := requireSelectedContractSettlementAuthorityTx(context.Background(), tx, f.snapshot, f.authority, owner, sqlite)
				if !errors.Is(err, owner.liveErr) {
					t.Fatalf("terminal settlement err=%v want=%v", err, owner.liveErr)
				}
				if !reflect.DeepEqual(owner.got, f.authority) || owner.live.SelectedFork.ExecutionID != f.authority.SelectedFork.ExecutionID {
					t.Fatal("current fence or no-live-attempt proof skipped")
				}
			})
		}
	}
}

func TestSelectedActivationAttachmentPrecedesDurableAcknowledgment(t *testing.T) {
	for _, test := range []struct {
		name                      string
		attachmentErr, arrivalErr error
	}{
		{name: "acknowledged"},
		{name: "refused", attachmentErr: errors.New("attachment refused")},
		{name: "arrival_refused", arrivalErr: errors.New("arrival inventory refused")},
	} {
		t.Run(test.name, func(t *testing.T) {
			attachmentErr := test.attachmentErr
			f := newSelectedAttachmentFixture(t, runfork.RunForkPointEvent)
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			backend, err := postgresbackend.New(db)
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectBegin()
			expectSelectedAttachmentBinding(mock, f.evidence.binding)
			if test.arrivalErr == nil {
				mock.ExpectQuery(`SELECT DISTINCT family`).WithArgs(f.evidence.lineage.SourceRunID, int64(1)).WillReturnRows(sqlmock.NewRows([]string{"family"}))
				mock.ExpectQuery(`SELECT clock_timestamp\(\)`).WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(time.Unix(200, 0).UTC()))
				mock.ExpectQuery(`FROM event_deliveries d`).WithArgs(f.evidence.lineage.SourceRunID).WillReturnRows(sqlmock.NewRows([]string{"delivery_id"}))
			}
			if attachmentErr == nil && test.arrivalErr == nil {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			steps := []string{}
			arrivals := &arrivalJoinInventoryOwner{err: test.arrivalErr}
			cleanup := errors.New("post-commit cleanup")
			port := runForkSelectedContractActivationPort{
				requireCurrent: func() error { return nil },
				runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error) {
					outcome := mutationprotocol.RunPostgres(ctx, backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil,
						func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
							return struct{}{}, attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
								return operation(ctx, tx, attempt)
							})
						})
					if !outcome.Acknowledged() {
						return false, outcome.Err()
					}
					return true, cleanup
				},
				loadLineage: func(context.Context, *sql.Tx, string) (runForkActivationLineage, error) {
					steps = append(steps, "lineage")
					return f.evidence.lineage, nil
				},
				lockFrontier: func(context.Context, *sql.Tx, *runForkActivationLineage) error {
					steps = append(steps, "heads")
					return nil
				},
				plan: func(context.Context, *sql.Tx, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
					steps = append(steps, "plan")
					return f.evidence.plan, nil
				},
				loadSnapshot: func(context.Context, *sql.Tx, string) (runlifecycle.Snapshot, error) {
					steps = append(steps, "timer_readback")
					return f.snapshot, nil
				},
				workflowTimers:   &workflowTimerMaterializerOwner{},
				arrivalSchedules: arrivals,
				deliveries:       postgresDeliveryAdapter,
				attachment: func(context.Context, *sql.Tx, runForkSelectedContractActivationEvidence) error {
					steps = append(steps, "attachment")
					return attachmentErr
				},
				transition: func(context.Context, *mutationprotocol.Attempt, runlifecycle.ActiveTransitionRequest) error {
					t.Fatal("unexpected advanced-source transition")
					return nil
				},
				diverge: func(context.Context, *sql.Tx, runfork.RunForkSelectedContractBranchDivergence) error {
					t.Fatal("unexpected divergence")
					return nil
				},
				freeze: func(context.Context, *sql.Tx, *mutationprotocol.Attempt, runForkActivationLineage, time.Time, bool) error {
					steps = append(steps, "activate")
					return nil
				},
				now: func() time.Time { return time.Unix(200, 0).UTC() },
			}
			ctx := correlation.WithRunID(context.Background(), f.evidence.lineage.ForkRunID)
			result, err := activateRunForkForSelectedContractExecution(ctx, f.evidence.request, port)
			wantSteps := []string{"lineage", "heads", "plan", "timer_readback", "attachment"}
			if test.arrivalErr != nil {
				wantSteps = wantSteps[:4]
				if result.Activated || !errors.Is(err, test.arrivalErr) {
					t.Fatalf("arrival refusal did not stop activation: result=%+v err=%v", result, err)
				}
			} else if attachmentErr == nil {
				wantSteps = append(wantSteps, "activate")
				if !result.Activated || !result.SourceFrozen || !errors.Is(err, cleanup) {
					t.Fatalf("durable acknowledgment lost: result=%+v err=%v", result, err)
				}
			} else if result.Activated || !errors.Is(err, attachmentErr) {
				t.Fatalf("attachment refusal lost: result=%+v err=%v", result, err)
			}
			if !reflect.DeepEqual(steps, wantSteps) {
				t.Fatalf("ordering=%v want=%v", steps, wantSteps)
			}
			if arrivals.reads != 1 {
				t.Fatal("activation bypassed complete arrival inventory")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSelectedActivationSettlementPreservesDrainAndLineageBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, unfinished := range []bool{false, true} {
			t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres]+map[bool]string{false: "/lineage", true: "/drain"}[unfinished], func(t *testing.T) {
				tx, mock := startSnapshotTransaction(t)
				child, event := uuid.NewString(), uuid.NewString()
				feeds := 0
				if unfinished {
					feeds = 1
				}
				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM fan_out_intents`).WithArgs(child).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(feeds))
				want := "fork_selected_contract_execution_lineage_missing"
				if unfinished {
					want = "unfinished feed"
					mock.ExpectQuery(`status<>'closed'`).WithArgs(child).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
					mock.ExpectQuery(`SELECT status,cursor,cardinality`).WithArgs(child).WillReturnRows(sqlmock.NewRows([]string{"status", "cursor", "cardinality"}).AddRow("open", 0, 1))
				} else {
					mock.ExpectQuery(`SELECT event_id, task_id FROM events`).WithArgs(child).
						WillReturnRows(sqlmock.NewRows([]string{"event", "task"}))
					expectEmptyGenericOccurrenceCensus(mock, child)
					if postgres {
						mock.ExpectQuery(`FROM unnest`).WillReturnRows(sqlmock.NewRows([]string{"missing"}).AddRow(1))
					} else {
						mock.ExpectQuery(`FROM run_fork_selected_contract_executions WHERE fork_run_id`).WithArgs(child, event).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
					}
				}
				var err error
				if postgres {
					err = (&RunForkPostgresOwner{}).requireRunForkSelectedContractExecutionSettlementTx(context.Background(), tx, child, []string{event}, nil)
				} else {
					err = (&RunForkSQLiteOwner{}).requireRunForkSelectedContractExecutionSettlementTx(context.Background(), tx, child, []string{event}, nil)
				}
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("completion requirement lost: %v", err)
				}
			})
		}
	}
}
