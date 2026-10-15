package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

type projectionShapeFaultCase struct {
	name   string
	apply  func(context.Context, any, string, string) (int64, error)
	entity bool
	column int
	value  string
}

func projectionShapeFaultCases() []projectionShapeFaultCase {
	return []projectionShapeFaultCase{
		{"ConflictingEntityType", SetWorkflowProjectionConflictingEntityTypeForTest, true, 5, `"wrong_entity_type"`},
		{"PlatformBookkeeping", SetWorkflowProjectionPlatformBookkeepingForTest, false, 4, `{"platform_fact":"preserve"}`},
		{"FieldsArray", SetWorkflowProjectionFieldsArrayForTest, true, 0, "[]"},
		{"NumericGate", SetWorkflowProjectionNumericGateForTest, false, 1, "{\"g_ready\":1}"},
		{"AccumulatorArray", SetWorkflowProjectionAccumulatorArrayForTest, false, 2, "[]"},
		{"MalformedTransitionHistory", SetWorkflowProjectionMalformedTransitionHistoryForTest, false, 3, "{\"workflow_version\":\"1.0.0\",\"instance_id\":\"inst-1\",\"storage_ref\":\"storage-ref\",\"transition_history\":\"bad\"}"},
		{"ConflictingInstanceID", SetWorkflowProjectionConflictingInstanceIDForTest, false, 3, "{\"workflow_version\":\"1.0.0\",\"instance_id\":\"inst-2\",\"storage_ref\":\"storage-ref\",\"flow_path\":\"storage-ref\"}"},
		{"SlashOnlyFlowPath", SetWorkflowProjectionSlashOnlyFlowPathForTest, false, 3, "{\"workflow_version\":\"1.0.0\",\"instance_id\":\"inst-1\",\"storage_ref\":\"storage-ref\",\"flow_path\":\"/\"}"},
	}
}

func readProjectionShapeStorage(t *testing.T, f receiverConfigActivationFixture, run, entity string) [6]any {
	t.Helper()
	var raw [5][]byte
	var entityType string
	if err := f.db.QueryRowContext(f.ctx, `SELECT es.fields,fi.gates,fi.accumulator,fi.config,fi.bookkeeping,es.entity_type FROM entity_state es JOIN flow_instances fi ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance AND fi.entity_id=es.entity_id WHERE es.run_id=$1 AND es.entity_id=$2`, run, entity).Scan(&raw[0], &raw[1], &raw[2], &raw[3], &raw[4], &entityType); err != nil {
		t.Fatal(err)
	}
	var out [6]any
	for i := range raw {
		if err := json.Unmarshal(raw[i], &out[i]); err != nil {
			t.Fatal(err)
		}
	}
	out[5] = entityType
	return out
}

func TestWorkflowPrivateBookkeepingWitnessPreservesOtherFactsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, plan := newWorkflowTargetConstructionFixture(t, backend)
			if committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !committed.Created || !committed.Acknowledged {
				t.Fatalf("lawful construction: %+v err=%v", committed, err)
			}
			run, entity, path := correlation.RunIDFromContext(f.ctx), plan.Instance.EntityID, plan.Instance.StorageRef
			before := readProjectionShapeStorage(t, f, run, entity)
			for _, key := range [][2]string{{uuid.NewString(), path}, {run, "foreign/path"}} {
				if changed, err := AddWorkflowProjectionPrivateBookkeepingForTest(f.ctx, f.store, key[0], key[1]); err == nil || changed != 0 {
					t.Fatalf("bookkeeping witness changed a foreign scope: rows=%d err=%v", changed, err)
				}
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if changed, err := AddWorkflowProjectionPrivateBookkeepingForTest(cancelled, f.store, run, path); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled bookkeeping witness: rows=%d err=%v", changed, err)
			}
			if actual := readProjectionShapeStorage(t, f, run, entity); !reflect.DeepEqual(actual, before) {
				t.Fatalf("refused witness changed unrelated facts: actual=%+v before=%+v", actual, before)
			}
			if changed, err := AddWorkflowProjectionPrivateBookkeepingForTest(f.ctx, f.store, run, path); err != nil || changed != 1 {
				t.Fatalf("exact bookkeeping witness: rows=%d err=%v", changed, err)
			}
			expected := before
			bookkeeping := maps.Clone(before[4].(map[string]any))
			bookkeeping["private_fact"] = "must-not-leak"
			expected[4] = bookkeeping
			if actual := readProjectionShapeStorage(t, f, run, entity); !reflect.DeepEqual(actual, expected) {
				t.Fatalf("bookkeeping witness lost a kept fact: actual=%+v expected=%+v", actual, expected)
			}
		})
	}
}

func TestWorkflowIdentityLookalikeFaultPreservesConstructionBothStores(t *testing.T) {
	run, entity := uuid.NewString(), uuid.NewString()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if changed, err := SetWorkflowProjectionIdentityLookalikesForTest(context.Background(), selected, run, entity, "review/item"); err == nil || changed != 0 {
			t.Fatalf("identity lookalikes accepted an unowned handle: %T rows=%d err=%v", selected, changed, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, plan := newWorkflowTargetConstructionFixture(t, backend)
			if committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !committed.Created || !committed.Acknowledged {
				t.Fatalf("lawful construction: %+v err=%v", committed, err)
			}
			record, err := plan.PersistenceRecord()
			if err != nil {
				t.Fatal(err)
			}
			run, entity, path := record.Identity.RunID, record.State.EntityID, record.Identity.Route.InstancePath
			reader := f.store.(pipeline.WorkflowTargetPersistenceReader)
			before, err := reader.LoadWorkflowTargetPersistence(f.ctx, record.Identity, identity.NormalizeEntityID(entity))
			if err != nil {
				t.Fatal(err)
			}
			history, err := ReadTrackedEntityMutationProjectionStorageForTest(f.ctx, f.store, run, entity)
			if err != nil {
				t.Fatal(err)
			}
			physical := readProjectionShapeStorage(t, f, run, entity)
			for _, key := range [][3]string{{"", entity, path}, {run, "invalid", path}, {run, entity, " spaced "}, {uuid.NewString(), entity, path}, {run, uuid.NewString(), path}, {run, entity, "foreign/path"}, {run, run, run}} {
				if changed, err := SetWorkflowProjectionIdentityLookalikesForTest(f.ctx, f.store, key[0], key[1], key[2]); err == nil || changed != 0 {
					t.Fatalf("identity lookalikes admitted invalid/foreign/fieldless scope: %v rows=%d err=%v", key, changed, err)
				}
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if changed, err := SetWorkflowProjectionIdentityLookalikesForTest(cancelled, f.store, run, entity, path); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled identity lookalikes: rows=%d err=%v", changed, err)
			}
			if actual := readProjectionShapeStorage(t, f, run, entity); !reflect.DeepEqual(actual, physical) {
				t.Fatalf("refused identity lookalikes changed physical facts: actual=%+v before=%+v", actual, physical)
			}
			if changed, err := SetWorkflowProjectionIdentityLookalikesForTest(f.ctx, f.store, run, entity, path); err != nil || changed != 1 {
				t.Fatalf("exact identity lookalikes: rows=%d err=%v", changed, err)
			}
			expected := physical
			expected[0] = map[string]any{"entity_id": "authored-lookalike", "flow_instance": "authored/lookalike"}
			expected[4] = map[string]any{"entity_id": "bookkeeping-lookalike", "flow_instance": "bookkeeping/lookalike"}
			if actual := readProjectionShapeStorage(t, f, run, entity); !reflect.DeepEqual(actual, expected) {
				t.Fatalf("identity lookalikes lost exact value maps or unrelated facts: actual=%+v expected=%+v", actual, expected)
			}
			after, err := reader.LoadWorkflowTargetPersistence(f.ctx, record.Identity, identity.NormalizeEntityID(entity))
			if err != nil {
				t.Fatal(err)
			}
			after.State.Fields = before.State.Fields
			after.Lifecycle.State.Bookkeeping = before.Lifecycle.State.Bookkeeping
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("identity lookalikes changed constructed identity, status, clocks or revision: before=%+v after=%+v", before, after)
			}
			keptHistory, err := ReadTrackedEntityMutationProjectionStorageForTest(f.ctx, f.store, run, entity)
			if err != nil || !reflect.DeepEqual(keptHistory.Mutations, history.Mutations) {
				t.Fatalf("identity lookalikes rewrote immutable history: before=%+v after=%+v err=%v", history.Mutations, keptHistory.Mutations, err)
			}
			if changed, err := SetWorkflowProjectionConflictingEntityTypeForTest(f.ctx, f.store, run, entity); err != nil || changed != 1 {
				t.Fatalf("install foreign field contract: rows=%d err=%v", changed, err)
			}
			foreign := readProjectionShapeStorage(t, f, run, entity)
			if changed, err := SetWorkflowProjectionIdentityLookalikesForTest(f.ctx, f.store, run, entity, path); err == nil || changed != 0 {
				t.Fatalf("identity lookalikes admitted mismatched companions: rows=%d err=%v", changed, err)
			}
			if actual := readProjectionShapeStorage(t, f, run, entity); !reflect.DeepEqual(actual, foreign) {
				t.Fatalf("refused mismatched companion changed physical facts: actual=%+v before=%+v", actual, foreign)
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if changed, err := SetWorkflowProjectionIdentityLookalikesForTest(f.ctx, f.store, run, entity, path); err == nil || changed != 0 {
				t.Fatalf("identity lookalikes admitted closed owner: rows=%d err=%v", changed, err)
			}
		})
	}
}

func TestWorkflowEmptyFieldFixturePreservesHeaderAndHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, plan := newWorkflowTargetConstructionFixture(t, backend)
			if committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !committed.Created || !committed.Acknowledged {
				t.Fatalf("lawful construction: %+v err=%v", committed, err)
			}
			record, err := plan.PersistenceRecord()
			if err != nil {
				t.Fatal(err)
			}
			run, entity := record.Identity.RunID, record.State.EntityID
			reader := f.store.(pipeline.WorkflowTargetPersistenceReader)
			before, err := reader.LoadWorkflowTargetPersistence(f.ctx, record.Identity, identity.NormalizeEntityID(entity))
			if err != nil {
				t.Fatal(err)
			}
			historyBefore, err := ReadTrackedEntityMutationProjectionStorageForTest(f.ctx, f.store, run, entity)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range [][2]string{{uuid.NewString(), entity}, {run, uuid.NewString()}, {run, run}, {run, entity}} {
				if changed, err := AddWorkflowProjectionEmptyFieldsForTest(f.ctx, f.store, key[0], key[1]); err == nil || changed != 0 {
					t.Fatalf("empty-field fixture admitted absent/fieldless/duplicate scope: rows=%d err=%v", changed, err)
				}
			}
			if changed, err := RemoveWorkflowProjectionFieldsForTest(f.ctx, f.store, run, entity); err != nil || changed != 1 {
				t.Fatalf("remove original field row: rows=%d err=%v", changed, err)
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if changed, err := AddWorkflowProjectionEmptyFieldsForTest(cancelled, f.store, run, entity); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled empty-field fixture: rows=%d err=%v", changed, err)
			}
			if changed, err := AddWorkflowProjectionEmptyFieldsForTest(f.ctx, f.store, run, entity); err != nil || changed != 1 {
				t.Fatalf("exact empty-field fixture: rows=%d err=%v", changed, err)
			}
			after, err := reader.LoadWorkflowTargetPersistence(f.ctx, record.Identity, identity.NormalizeEntityID(entity))
			if err != nil || !reflect.DeepEqual(after.Lifecycle, before.Lifecycle) {
				t.Fatalf("empty-field fixture changed a kept header fact: before=%+v after=%+v err=%v", before.Lifecycle, after.Lifecycle, err)
			}
			expected := before.State
			expected.Fields = json.RawMessage(`{}`)
			expected.CreatedAt, expected.UpdatedAt = before.Lifecycle.CreatedAt, before.Lifecycle.State.UpdatedAt
			if !after.State.CreatedAt.Equal(expected.CreatedAt) || !after.State.UpdatedAt.Equal(expected.UpdatedAt) {
				t.Fatalf("empty-field fixture changed data-clock instants: after=%+v expected=%+v", after.State, expected)
			}
			expected.CreatedAt, expected.UpdatedAt = expected.CreatedAt.UTC(), expected.UpdatedAt.UTC()
			if !reflect.DeepEqual(after.State, expected) {
				t.Fatalf("empty-field fixture changed identity/display/clocks: after=%+v expected=%+v", after.State, expected)
			}
			historyAfter, err := ReadTrackedEntityMutationProjectionStorageForTest(f.ctx, f.store, run, entity)
			if err != nil || !reflect.DeepEqual(historyAfter.Mutations, historyBefore.Mutations) {
				t.Fatalf("physical fixture invented or changed immutable history: before=%+v after=%+v err=%v", historyBefore.Mutations, historyAfter.Mutations, err)
			}
		})
	}
}

func TestWorkflowProjectionShapeFaultsKeepExactPayloadScopeAndNativeTransactionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, tc := range projectionShapeFaultCases() {
				t.Run(tc.name, func(t *testing.T) {
					f, plan := newWorkflowTargetConstructionFixture(t, backend)
					if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Created || !result.Acknowledged {
						t.Fatalf("lawful construction: %+v %v", result, err)
					}
					run, entity := correlation.RunIDFromContext(f.ctx), plan.Instance.EntityID
					key := plan.Instance.StorageRef
					if tc.entity {
						key = entity
					}
					before := readProjectionShapeStorage(t, f, run, entity)
					for _, scope := range [][2]string{{uuid.NewString(), key}, {run, uuid.NewString()}} {
						if changed, err := tc.apply(f.ctx, f.store, scope[0], scope[1]); err == nil || changed != 0 {
							t.Fatalf("fault changed absent scope: rows=%d err=%v", changed, err)
						}
					}
					cancelled, cancel := context.WithCancel(f.ctx)
					cancel()
					if changed, err := tc.apply(cancelled, f.store, run, key); !errors.Is(err, context.Canceled) || changed != 0 {
						t.Fatalf("cancelled fault supplied success: rows=%d err=%v", changed, err)
					}
					if after := readProjectionShapeStorage(t, f, run, entity); !reflect.DeepEqual(after, before) {
						t.Fatalf("refused fault changed original projection: got=%+v want=%+v", after, before)
					}
					probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
					if err != nil {
						t.Fatal(err)
					}
					defer restore()
					if changed, err := tc.apply(f.ctx, f.store, run, key); err != nil || changed != 1 {
						t.Fatalf("exact named fault: rows=%d err=%v", changed, err)
					}
					if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 0 || counts.Active != 0 {
						t.Fatalf("fault escaped original write coordinator: %+v", counts)
					}
					expected := before
					if err := json.Unmarshal([]byte(tc.value), &expected[tc.column]); err != nil {
						t.Fatal(err)
					}
					if after := readProjectionShapeStorage(t, f, run, entity); !reflect.DeepEqual(after, expected) {
						t.Fatalf("fixed fault changed payload or other columns: got=%+v want=%+v", after, expected)
					}
				})
			}
		})
	}
}

func TestWorkflowProjectionShapeFaultsRefuseInvalidAndClosedOwnersBothStores(t *testing.T) {
	run, entity := uuid.NewString(), uuid.NewString()
	for _, tc := range projectionShapeFaultCases() {
		for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
			if changed, err := tc.apply(context.Background(), selected, run, entity); err == nil || changed != 0 {
				t.Fatalf("%s accepted invalid owner: %T rows=%d err=%v", tc.name, selected, changed, err)
			}
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, tc := range projectionShapeFaultCases() {
				for _, key := range []string{"", "invalid", uuid.Nil.String()} {
					if changed, err := tc.apply(testAuthorActivityContext(), f.store, key, entity); err == nil || changed != 0 {
						t.Fatalf("%s accepted invalid run: rows=%d err=%v", tc.name, changed, err)
					}
				}
				for _, key := range []string{"", " ", " spaced "} {
					if changed, err := tc.apply(testAuthorActivityContext(), f.store, run, key); err == nil || changed != 0 {
						t.Fatalf("%s accepted invalid coordinate: rows=%d err=%v", tc.name, changed, err)
					}
				}
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, tc := range projectionShapeFaultCases() {
				if changed, err := tc.apply(testAuthorActivityContext(), f.store, run, entity); err == nil || changed != 0 {
					t.Fatalf("%s accepted closed owner: rows=%d err=%v", tc.name, changed, err)
				}
			}
		})
	}
}

func TestWorkflowProjectionShapeFaultsRespectOriginalSQLiteWriterAdmission(t *testing.T) {
	for _, tc := range projectionShapeFaultCases() {
		t.Run(tc.name, func(t *testing.T) { verifyWorkflowProjectionFaultSQLiteAdmission(t, tc) })
	}
}

func verifyWorkflowProjectionFaultSQLiteAdmission(t *testing.T, tc projectionShapeFaultCase) {
	t.Helper()

	f, plan := newWorkflowTargetConstructionFixture(t, "sqlite")
	if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
		t.Fatal(err)
	}
	owner := f.store.(*SQLiteRuntimeStore)
	run, entity := correlation.RunIDFromContext(f.ctx), plan.Instance.EntityID
	key := plan.Instance.StorageRef
	if tc.entity {
		key = entity
	}
	before := readProjectionShapeStorage(t, f, run, entity)
	probe, restore, err := InstallTransactionProbeForTest(owner, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- owner.backend.RunTransaction(ctx, "projection fault writer-holder proof", func(context.Context, *sql.Tx) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	holderJoined := false
	defer func() {
		unblock()
		if !holderJoined {
			if err := <-holderDone; err != nil {
				t.Errorf("joined holder: %v", err)
			}
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	faultCtx, cancelFault := context.WithCancel(ctx)
	defer cancelFault()
	faultDone := make(chan error, 1)
	go func() {
		changed, err := tc.apply(faultCtx, owner, run, key)
		if changed != 0 {
			err = errors.Join(err, errors.New("queued fault changed a row"))
		}
		faultDone <- err
	}()
	joined := false
	defer func() {
		cancelFault()
		unblock()
		if !joined {
			<-faultDone
		}
	}()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for owner.backend.QueuedWritersForTest() != 1 {
		select {
		case err := <-faultDone:
			joined = true
			t.Fatalf("fault bypassed exact writer admission: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	if counts := probe.Snapshot(); counts.Total.Begun != 0 || counts.Total.WriteCommits != 0 || counts.Active != 1 {
		t.Fatalf("queued fault began another transaction: %+v", counts)
	}
	cancelFault()
	select {
	case err := <-faultDone:
		joined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if owner.backend.QueuedWritersForTest() != 0 {
		t.Fatal("cancelled fault retained writer admission")
	}
	if after := readProjectionShapeStorage(t, f, run, entity); !reflect.DeepEqual(after, before) {
		t.Fatalf("queued fault escaped before cancellation: %+v", after)
	}
	unblock()
	holderErr := <-holderDone
	holderJoined = true
	if holderErr != nil {
		t.Fatal(holderErr)
	}
	if changed, err := tc.apply(ctx, owner, run, key); err != nil || changed != 1 {
		t.Fatalf("named fault after admission release: rows=%d err=%v", changed, err)
	}
	if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.WriteCommits != 2 || counts.Active != 0 {
		t.Fatalf("joined holder and later fault escaped exact coordinator: %+v", counts)
	}
}
