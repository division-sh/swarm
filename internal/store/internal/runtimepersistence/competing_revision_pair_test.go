package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

type competingRevisionPairCase struct {
	name  string
	apply func(context.Context, any, pipeline.WorkflowEngineStateRecord) error
}

func competingRevisionPairCases() []competingRevisionPairCase {
	return []competingRevisionPairCase{
		{"selection", AdvanceSelectionRevisionPairForTest},
		{"prospective", AdvanceProspectiveRevisionPairForTest},
	}
}

func newCompetingRevisionPairFixture(t *testing.T, backend string) (receiverConfigActivationFixture, pipeline.WorkflowEngineStateRecord) {
	t.Helper()
	f, plan := newWorkflowTargetConstructionFixture(t, backend)
	if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Created || !result.Acknowledged {
		t.Fatalf("lawful competing-writer construction: %+v %v", result, err)
	}
	state := pipeline.WorkflowEngineStateRecord{Identity: flowidentity.RunScopedFlowInstance{RunID: correlation.RunIDFromContext(f.ctx), Route: plan.Identity.Route()}, EntityID: plan.Instance.EntityID}
	if state.Identity.RunID != correlation.RunIDFromContext(f.ctx) {
		t.Fatal("native competing-writer fixture lost its exact run")
	}
	if err := f.db.QueryRowContext(f.ctx, `SELECT revision FROM flow_instances WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3`, state.Identity.RunID, state.Identity.Route.InstancePath, state.EntityID).Scan(&state.ExpectedRevision); err != nil {
		t.Fatal(err)
	}
	return f, state
}

func competingRevisionPairSnapshot(t *testing.T, f receiverConfigActivationFixture) map[string]SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func assertCompetingRevisionPairSnapshot(t *testing.T, before, after map[string]SelectedForkStorageTableSnapshot, state pipeline.WorkflowEngineStateRecord, selection bool) {
	t.Helper()
	for _, table := range []string{"flow_instances", "entity_state"} {
		original, current := before[table], after[table]
		if !reflect.DeepEqual(original.Columns, current.Columns) || len(original.Rows) != len(current.Rows) {
			t.Fatalf("competing pair changed %s inventory", table)
		}
		var prior []SelectedForkSnapshotCell
		for _, encoded := range original.Rows {
			var cells []SelectedForkSnapshotCell
			if err := json.Unmarshal([]byte(encoded), &cells); err != nil {
				t.Fatal(err)
			}
			if competingRevisionPairTarget(t, cells, state) {
				if prior != nil {
					t.Fatalf("ambiguous original %s target", table)
				}
				prior = cells
			}
		}
		if prior == nil {
			t.Fatalf("missing original %s target", table)
		}
		current.Rows = append([]string(nil), current.Rows...)
		matched := 0
		for i, encoded := range current.Rows {
			var cells []SelectedForkSnapshotCell
			if err := json.Unmarshal([]byte(encoded), &cells); err != nil {
				t.Fatal(err)
			}
			if !competingRevisionPairTarget(t, cells, state) {
				continue
			}
			matched++
			for column, cell := range cells {
				if cell.Column == "revision" {
					var oldRevision, revision int64
					if json.Unmarshal(prior[column].Value, &oldRevision) != nil || json.Unmarshal(cell.Value, &revision) != nil || oldRevision != state.ExpectedRevision || revision != oldRevision+1 || cell.Type != prior[column].Type {
						t.Fatalf("%s competing revision lost exact increment: %s -> %s", table, prior[column].Value, cell.Value)
					}
					cells[column] = prior[column]
				}
				if selection && table == "entity_state" && cell.Column == "fields" {
					text, err := cell.Text()
					var fields map[string]any
					if err != nil || cell.Type != prior[column].Type || json.Unmarshal([]byte(text), &fields) != nil || !reflect.DeepEqual(fields, map[string]any{"marker": "second"}) {
						t.Fatalf("selection competing writer lost exact replacement fields: %s %v", text, err)
					}
					cells[column] = prior[column]
				}
			}
			normalized, err := json.Marshal(cells)
			if err != nil {
				t.Fatal(err)
			}
			current.Rows[i] = string(normalized)
		}
		if matched != 1 {
			t.Fatalf("%s competing writer target count=%d", table, matched)
		}
		sort.Strings(current.Rows)
		after[table] = current
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("competing writer changed another physical column, clock, row or application table")
	}
}

func competingRevisionPairTarget(t *testing.T, cells []SelectedForkSnapshotCell, state pipeline.WorkflowEngineStateRecord) bool {
	t.Helper()
	var run, entity string
	for _, cell := range cells {
		if cell.Column != "run_id" && cell.Column != "entity_id" {
			continue
		}
		value, err := cell.Text()
		if err != nil {
			t.Fatal(err)
		}
		if cell.Column == "run_id" {
			run = value
		} else {
			entity = value
		}
	}
	return run == state.Identity.RunID && entity == state.EntityID
}

func TestCompetingRevisionPairsUseOriginalWriterAndKeepRealScopeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, tc := range competingRevisionPairCases() {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				f, state := newCompetingRevisionPairFixture(t, backend)
				before := competingRevisionPairSnapshot(t, f)
				foreign := state
				foreign.Identity.RunID = uuid.NewString()
				if err := tc.apply(f.ctx, f.store, foreign); err == nil {
					t.Fatal("competing writer accepted a foreign run")
				}
				stale := state
				stale.ExpectedRevision++
				if err := tc.apply(f.ctx, f.store, stale); err == nil {
					t.Fatal("competing writer accepted a foreign revision")
				}
				if tc.name == "prospective" {
					foreign = state
					foreign.Identity.Route.InstancePath += "/foreign"
					if err := tc.apply(f.ctx, f.store, foreign); err == nil {
						t.Fatal("prospective competing writer dropped its exact path")
					}
				}
				cancelled, cancel := context.WithCancel(f.ctx)
				cancel()
				if err := tc.apply(cancelled, f.store, state); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled competing writer: %v", err)
				}
				if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
					t.Fatal("refused competing pair changed physical storage")
				}
				probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				if err := tc.apply(f.ctx, f.store, state); err != nil {
					t.Fatal(err)
				}
				if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 0 || counts.Active != 0 {
					t.Fatalf("competing pair escaped original transaction: %+v", counts)
				}
				assertCompetingRevisionPairSnapshot(t, before, competingRevisionPairSnapshot(t, f), state, tc.name == "selection")
			})
		}
	}
}

func installCompetingRevisionSecondStatementFault(t *testing.T, f receiverConfigActivationFixture, backend string) {
	t.Helper()
	install := []string{`CREATE TRIGGER competing_pair_second BEFORE UPDATE OF revision ON entity_state BEGIN SELECT RAISE(ABORT,'competing_pair_second_cut'); END`}
	remove := []string{`DROP TRIGGER IF EXISTS competing_pair_second`}
	if backend == "postgres" {
		install = []string{`CREATE FUNCTION competing_pair_second_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'competing_pair_second_cut'; RETURN NEW; END $$`, `CREATE TRIGGER competing_pair_second BEFORE UPDATE OF revision ON entity_state FOR EACH ROW EXECUTE FUNCTION competing_pair_second_fn()`}
		remove = []string{`DROP TRIGGER IF EXISTS competing_pair_second ON entity_state`, `DROP FUNCTION IF EXISTS competing_pair_second_fn()`}
	}
	t.Cleanup(func() {
		if err := runWorkflowProjectionFault(f.ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
			for _, statement := range remove {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Error(err)
		}
	})
	if err := runWorkflowProjectionFault(f.ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
		for _, statement := range install {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCompetingRevisionPairSecondFailureRollsBackHeader(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, tc := range competingRevisionPairCases() {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				f, state := newCompetingRevisionPairFixture(t, backend)
				installCompetingRevisionSecondStatementFault(t, f, backend)
				before := competingRevisionPairSnapshot(t, f)
				probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				if err := tc.apply(f.ctx, f.store, state); err == nil || !strings.Contains(err.Error(), "competing_pair_second_cut") {
					t.Fatalf("did not reach real second statement: %v", err)
				}
				if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 0 || counts.Total.RollbackAttempts != 1 || counts.Active != 0 {
					t.Fatalf("failed pair did not roll back original transaction: %+v", counts)
				}
				if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
					t.Fatal("second statement failure committed a header, field, clock or journal change")
				}
			})
		}
	}
}

func TestCompetingRevisionPairsRefuseRawAndClosedOwners(t *testing.T) {
	var postgres *PostgresStore
	var sqlite *SQLiteRuntimeStore
	for _, tc := range competingRevisionPairCases() {
		for _, selected := range []any{nil, postgres, sqlite, &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
			if err := tc.apply(context.Background(), selected, pipeline.WorkflowEngineStateRecord{}); err == nil {
				t.Fatalf("%s accepted invalid owner %T", tc.name, selected)
			}
		}
		for _, backend := range []string{"sqlite", "postgres"} {
			t.Run(tc.name+"/"+backend, func(t *testing.T) {
				f, state := newCompetingRevisionPairFixture(t, backend)
				if err := f.store.(interface{ Close() error }).Close(); err != nil {
					t.Fatal(err)
				}
				if err := tc.apply(f.ctx, f.store, state); err == nil {
					t.Fatal("competing pair accepted closed native owner")
				}
			})
		}
	}
}

func TestCompetingRevisionPairsRespectOriginalSQLiteWriterAdmission(t *testing.T) {
	for _, tc := range competingRevisionPairCases() {
		t.Run(tc.name, func(t *testing.T) { verifyCompetingRevisionPairSQLiteAdmission(t, tc) })
	}
}

func verifyCompetingRevisionPairSQLiteAdmission(t *testing.T, tc competingRevisionPairCase) {
	t.Helper()
	f, state := newCompetingRevisionPairFixture(t, "sqlite")
	owner := f.store.(*SQLiteRuntimeStore)
	before := competingRevisionPairSnapshot(t, f)
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
		holderDone <- owner.backend.RunTransaction(ctx, "competing revision admission holder", func(context.Context, *sql.Tx) error {
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
				t.Error(err)
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
	go func() { faultDone <- tc.apply(faultCtx, owner, state) }()
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
			t.Fatalf("competing pair bypassed original admission: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	if counts := probe.Snapshot(); counts.Total.Begun != 0 || counts.Active != 1 {
		t.Fatalf("queued pair opened another transaction: %+v", counts)
	}
	cancelFault()
	if err := <-faultDone; !errors.Is(err, context.Canceled) {
		joined = true
		t.Fatalf("queued cancellation: %v", err)
	}
	joined = true
	unblock()
	if err := <-holderDone; err != nil {
		holderJoined = true
		t.Fatal(err)
	}
	holderJoined = true
	if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Active != 0 {
		t.Fatalf("queued pair escaped original holder: %+v", counts)
	}
	if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
		t.Fatalf("cancelled queued %s writer changed storage", tc.name)
	}
}
