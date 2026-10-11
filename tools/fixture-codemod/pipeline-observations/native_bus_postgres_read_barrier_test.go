package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func normalizeOriginReadCleanup(source string) string {
	for _, pair := range [][2]string{
		{"\t\t\tvar barrier *storetest.PostgresRunTableReadBarrier\n\t\t\tvar releaseOnce sync.Once\n\t\t\treleaseReader := func() { releaseOnce.Do(func() { close(reader.release) }) }\n\t\t\tcompleted := make(chan error, 1)\n\t\t\tworkerStarted, workerJoined := false, false\n\t\t\tgeneration := &completeEventDispatchGeneration{}\n\t\t\tt.Cleanup(func() {\n\t\t\t\tclean := true\n\t\t\t\tcancel()\n\t\t\t\tif c != nil {\n\t\t\t\t\twait, stop := context.WithCancel(context.Background())\n\t\t\t\t\tstop()\n\t\t\t\t\t_ = c.Retire(wait)\n\t\t\t\t}\n\t\t\t\tif barrier != nil {\n\t\t\t\t\tif err := barrier.Close(); err != nil {\n\t\t\t\t\t\tclean = false\n\t\t\t\t\t\tt.Error(err)\n\t\t\t\t\t}\n\t\t\t\t}\n\t\t\t\treleaseReader()\n\t\t\t\tif err := generation.close(); err != nil {\n\t\t\t\t\tclean = false\n\t\t\t\t\tt.Error(err)\n\t\t\t\t}\n\t\t\t\tif workerStarted && !workerJoined {\n\t\t\t\t\tselect {\n\t\t\t\t\tcase <-completed:\n\t\t\t\t\t\tworkerJoined = true\n\t\t\t\t\tcase <-time.After(5 * time.Second):\n\t\t\t\t\t\tclean = false\n\t\t\t\t\t\tt.Error(\"failed origin proof left an unjoined worker\")\n\t\t\t\t\t}\n\t\t\t\t}\n\t\t\t\tif clean && (owner == nil || owner.ActiveCount() == 0) && (!workerStarted || workerJoined) {\n\t\t\t\t\tt.Log(\"origin proof cleanup joined\")\n\t\t\t\t}\n\t\t\t})\n", ""},
		{"\t\t\t\tgeneration.process = process\n", ""},
		{"\t\t\t\tgeneration.owner = owner\n", ""},
		{"\t\t\t\tgeneration.coordinator = c\n", ""},
		{"\t\t\t\tworkerStarted = true\n", ""},
		{"\t\t\t\tworkerJoined = true\n", ""},
		{"\t\t\treader.enabled.Store(true)\n", "\t\t\treader.enabled.Store(true)\n\t\t\tcompleted := make(chan error, 1)\n"},
		{"\t\t\tvar err error\n\t\t\tbarrier, err = storetest.HoldPostgresRunTableReadBarrier(context.Background(), f.store)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\treleaseReader()\n\t\t\twaitOriginSQLLock(t, f.store)", "\t\t\tlock, err := f.db.BeginTx(context.Background(), nil)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tdefer lock.Rollback()\n\t\t\tif _, err := lock.Exec(`LOCK TABLE runs IN ACCESS EXCLUSIVE MODE`); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tclose(reader.release)\n\t\t\twaitOriginSQLLock(t, f.db)"},
		{"barrier.Close()", "lock.Rollback()"},
		{"\t\t\t\tif err := generation.close(); err != nil {\n\t\t\t\t\tt.Error(err)\n\t\t\t\t}", "\t\t\t\tif _, err := owner.RetireAndWait(context.Background()); err != nil {\n\t\t\t\t\tt.Error(err)\n\t\t\t\t}\n\t\t\t\tprocess.Retire()\n\t\t\t\tif _, err := process.Join(context.Background()); err != nil {\n\t\t\t\t\tt.Error(err)\n\t\t\t\t}"},
	} {
		source = strings.ReplaceAll(source, pair[0], pair[1])
	}
	return source
}

func TestNativeBusPostgresReadBarrierPreservesEveryOriginAndRetirementCut(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-postgres-read-barrier" {
			continue
		}
		matched++
		before := row.Before
		switch row.Function {
		case "waitOriginSQLLock":
			before = strings.ReplaceAll(before, "db *sql.DB", "selected completeEventDispatchStore")
			before = strings.ReplaceAll(before, "originSQLLockCount(db)", "originSQLLockCount(selected)")
		case "originSQLLockCount":
			owner := selectedCausalObservationBody(t, "internal/store/internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadPostgresRunOriginLockCountTx")
			if !reflect.DeepEqual(eventDeliveryDiagnosticSQL(t, before), eventDeliveryDiagnosticSQL(t, owner)) || !strings.Contains(row.After, "ReadPostgresRunOriginLockCount(context.Background(), selected)") {
				t.Fatal("origin observer changed its exact prefix/current-database/lock predicate")
			}
			before = row.After
		case "waitPostgresReadLockCount":
			owner := selectedCausalObservationBody(t, "internal/store/internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadPostgresDatabaseLockCountTx")
			if !reflect.DeepEqual(eventDeliveryDiagnosticSQL(t, before), eventDeliveryDiagnosticSQL(t, owner)) {
				t.Fatal("database observer changed its physical lock predicate")
			}
			before = strings.ReplaceAll(before, "db *sql.DB", "selected completeEventDispatchStore")
			before = strings.Replace(before, "\t\tvar blocked int\n\t\tif err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'`).Scan(&blocked); err != nil {", "\t\tblocked, err := storetest.ReadPostgresDatabaseLockCount(context.Background(), selected)\n\t\tif err != nil {", 1)
		case "TestOriginSQLLockObserverPostgres":
			before = strings.Replace(before, "f.db.BeginTx(ctx, nil)", "storetest.HoldPostgresRunTableReadBarrier(ctx, f.store)", 1)
			before = strings.Replace(before, "lock.Rollback()", "lock.Close()", 1)
			before = strings.Replace(before, "\t\t\tif _, err := lock.Exec(`LOCK TABLE runs IN ACCESS EXCLUSIVE MODE`); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n", "", 1)
			before = strings.ReplaceAll(before, "t, f.db", "t, f.store")
			before = strings.ReplaceAll(before, "originSQLLockCount(f.db)", "originSQLLockCount(f.store)")
		case "TestContinuationOriginReadPostgresCancellationCausality":
			if normalizeOriginReadCleanup(row.After) != before {
				t.Fatal("origin drain/cancellation changed a runtime lease, native 57014, blocked-read, retirement or no-mutation assertion")
			}
			before = row.After
		default:
			t.Fatal("unknown origin-read consumer")
		}
		if before != row.After || selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatalf("%s: origin-read cut diverged from finite migration", row.Function)
		}
	}
	if matched != 5 {
		t.Fatalf("origin-read recipes=%d,want5", matched)
	}
	barrier := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_postgres_run_read_barrier.go", "HoldPostgresRunTableReadBarrierForTest")
	for _, cut := range []string{"owner.backend.RunTransaction(ctx,", "runlifecycle.HoldPostgresRunTableReadBarrierTx(sqlctx, tx)", "close(ready)", "if err != rollback", "close(barrier.done)", "barrier.Close()"} {
		if !strings.Contains(barrier, cut) {
			t.Fatalf("run barrier lost joined original-owner cut %s", cut)
		}
	}
}
