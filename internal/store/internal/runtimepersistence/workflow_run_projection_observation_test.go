package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestWorkflowRunPhysicalProjectionReadersRetainOriginalScopesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			runID := state.Identity.RunID
			var expected WorkflowRunFieldsRevisionStorage
			var revision int64
			var fields string
			var count int
			if err := f.db.QueryRow(`SELECT CAST(fields AS TEXT), revision FROM entity_state WHERE run_id=$1`, runID).Scan(&expected.Fields, &expected.Revision); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRow(`SELECT revision FROM flow_instances WHERE run_id=$1`, runID).Scan(&revision); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1`, runID).Scan(&fields); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, runID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if row, err := ReadWorkflowRunFieldsRevisionStorageForTest(context.Background(), f.store, runID); err != nil || row != expected {
				t.Fatalf("original text/revision snapshot: %+v want %+v err=%v", row, expected, err)
			}
			if got, err := ReadWorkflowRunHeaderRevisionStorageForTest(context.Background(), f.store, runID); err != nil || got != revision {
				t.Fatalf("independent header revision: %d want %d err=%v", got, revision, err)
			}
			if got, err := ReadWorkflowRunFieldsStorageForTest(context.Background(), f.store, runID); err != nil || got != fields {
				t.Fatalf("independent exact field text: %s want %s err=%v", got, fields, err)
			}
			if got, err := CountWorkflowRunFieldRowsForTest(context.Background(), f.store, runID); err != nil || got != count {
				t.Fatalf("run field multiplicity: %d want %d err=%v", got, count, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 4 || counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("four original independent observation cuts changed: %+v", counts)
			}
			for _, key := range []string{uuid.NewString()} {
				if _, err := ReadWorkflowRunFieldsRevisionStorageForTest(f.ctx, f.store, key); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("absent field/revision lost SQL refusal: %v", err)
				}
				if _, err := ReadWorkflowRunHeaderRevisionStorageForTest(f.ctx, f.store, key); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("absent header lost SQL refusal: %v", err)
				}
				if _, err := ReadWorkflowRunFieldsStorageForTest(f.ctx, f.store, key); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("absent fields lost SQL refusal: %v", err)
				}
				if got, err := CountWorkflowRunFieldRowsForTest(f.ctx, f.store, key); err != nil || got != 0 {
					t.Fatalf("absent scope changed count: %d %v", got, err)
				}
			}
			assertWorkflowRunEmptyScopePhysicalContract(t, f)
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			for _, err := range []error{
				workflowRunProjectionReadError(cancelled, f.store, runID, 0),
				workflowRunProjectionReadError(cancelled, f.store, runID, 1),
				workflowRunProjectionReadError(cancelled, f.store, runID, 2),
				workflowRunProjectionReadError(cancelled, f.store, runID, 3),
			} {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled native read: %v", err)
				}
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("physical projection observation changed storage or clocks")
			}
		})
	}
}

func assertWorkflowRunEmptyScopePhysicalContract(t *testing.T, f receiverConfigActivationFixture) {
	t.Helper()
	for kind, query := range []string{
		`SELECT CAST(fields AS TEXT), revision FROM entity_state WHERE run_id=$1`,
		`SELECT revision FROM flow_instances WHERE run_id=$1`,
		`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1`,
		`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`,
	} {
		var first, second any
		destinations := []any{&first}
		if kind == 0 {
			destinations = append(destinations, &second)
		}
		expected := f.db.QueryRow(query, "").Scan(destinations...)
		actual := workflowRunProjectionReadError(context.Background(), f.store, "", kind)
		if (expected == nil) != (actual == nil) || expected != nil && expected.Error() != actual.Error() {
			t.Fatalf("read%d changed original empty-scope SQL contract: got=%v want=%v", kind, actual, expected)
		}
		if kind == 3 && expected == nil {
			count, err := CountWorkflowRunFieldRowsForTest(context.Background(), f.store, "")
			if err != nil || int64(count) != first.(int64) {
				t.Fatalf("empty-scope multiplicity: %d %v; physical=%v", count, err, first)
			}
		}
	}
}

func workflowRunProjectionReadError(ctx context.Context, selected any, runID string, kind int) error {
	switch kind {
	case 0:
		_, err := ReadWorkflowRunFieldsRevisionStorageForTest(ctx, selected, runID)
		return err
	case 1:
		_, err := ReadWorkflowRunHeaderRevisionStorageForTest(ctx, selected, runID)
		return err
	case 2:
		_, err := ReadWorkflowRunFieldsStorageForTest(ctx, selected, runID)
		return err
	default:
		_, err := CountWorkflowRunFieldRowsForTest(ctx, selected, runID)
		return err
	}
}

func TestWorkflowRunPhysicalProjectionReadersRefuseRawAndClosedOwners(t *testing.T) {
	var postgres *PostgresStore
	var sqlite *SQLiteRuntimeStore
	for _, selected := range []any{nil, postgres, sqlite, &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		for kind := 0; kind < 4; kind++ {
			if err := workflowRunProjectionReadError(context.Background(), selected, uuid.NewString(), kind); err == nil {
				t.Fatalf("read%d accepted invalid owner %T", kind, selected)
			}
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			if err := f.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for kind := 0; kind < 4; kind++ {
				if err := workflowRunProjectionReadError(f.ctx, f.store, state.Identity.RunID, kind); err == nil {
					t.Fatalf("read%d accepted closed native owner", kind)
				}
			}
		})
	}
}
