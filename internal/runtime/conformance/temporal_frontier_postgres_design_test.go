package conformance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testpostgres"
	_ "github.com/lib/pq"
)

func TestTemporalFrontierPostgresDesignConformance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	connection, err := testpostgres.ConnectionFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	admin, err := connection.Open()
	if err != nil {
		t.Fatalf("open PostgreSQL conformance connection: %v", err)
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL conformance connection: %v", err)
	}

	var canCreateRoles bool
	if err := admin.QueryRowContext(ctx, `
		SELECT rolsuper OR rolcreaterole
		FROM pg_roles
		WHERE rolname = current_user
	`).Scan(&canCreateRoles); err != nil {
		t.Fatalf("inspect PostgreSQL conformance role: %v", err)
	}
	if !canCreateRoles {
		t.Skip("temporal frontier privilege conformance requires CREATEROLE; use the dedicated cmd/swarm-test-postgres runner")
	}

	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	ownerRole := "tf_owner_" + suffix
	runtimeRole := "tf_runtime_" + suffix
	upgradeSchema := "tf_upgrade_" + suffix
	freshSchema := "tf_fresh_" + suffix
	runtimePassword := "tf-runtime-" + suffix

	mustExecTemporal(t, ctx, admin, `CREATE ROLE `+quoteTemporalIdent(ownerRole)+` LOGIN PASSWORD 'owner-not-used' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS`)
	mustExecTemporal(t, ctx, admin, `CREATE ROLE `+quoteTemporalIdent(runtimeRole)+` LOGIN PASSWORD `+quoteTemporalLiteral(runtimePassword)+` NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS`)
	defer func() {
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+quoteTemporalIdent(freshSchema)+` CASCADE`)
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+quoteTemporalIdent(upgradeSchema)+` CASCADE`)
		_, _ = admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS `+quoteTemporalIdent(runtimeRole))
		_, _ = admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS `+quoteTemporalIdent(ownerRole))
	}()

	mustExecTemporal(t, ctx, admin, `CREATE SCHEMA `+quoteTemporalIdent(freshSchema)+` AUTHORIZATION `+quoteTemporalIdent(ownerRole))
	if err := applyTemporalPrototype(ctx, admin, freshSchema, ownerRole, runtimeRole, true, false); err != nil {
		t.Fatalf("fresh temporal schema apply: %v", err)
	}
	assertTemporalTargetMetadata(t, ctx, admin, freshSchema, ownerRole, runtimeRole)
	for _, table := range []string{"run_temporal_transactions", "run_temporal_frontiers", "run_temporal_revisions", "runtime_store_migrations", "run_deletion_tombstones", "event_delivery_history", "event_receipt_history"} {
		assertTemporalTableExists(t, ctx, admin, freshSchema, table, true)
	}

	mustExecTemporal(t, ctx, admin, `CREATE SCHEMA `+quoteTemporalIdent(upgradeSchema)+` AUTHORIZATION `+quoteTemporalIdent(ownerRole))
	createTemporalLegacySchema(t, ctx, admin, upgradeSchema, ownerRole)
	legacyRun := "00000000-0000-0000-0000-000000000001"
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`INSERT INTO %s.runs(run_id,status) VALUES ($1,'running')`, quoteTemporalIdent(upgradeSchema)), legacyRun)
	legacyEvent := "00000000-0000-0000-0000-000000000011"
	legacyDelivery := "00000000-0000-0000-0000-000000000012"
	legacyReceipt := "00000000-0000-0000-0000-000000000013"
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`INSERT INTO %s.events(event_id,run_id) VALUES ($1,$2)`, quoteTemporalIdent(upgradeSchema)), legacyEvent, legacyRun)
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`INSERT INTO %s.event_deliveries(delivery_id,run_id,event_id,status) VALUES ($1,$2,$3,'pending')`, quoteTemporalIdent(upgradeSchema)), legacyDelivery, legacyRun, legacyEvent)
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`INSERT INTO %s.event_receipts(receipt_id,event_id,outcome) VALUES ($1,$2,'success')`, quoteTemporalIdent(upgradeSchema)), legacyReceipt, legacyEvent)

	err = applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, false, false)
	if err == nil || !strings.Contains(err.Error(), legacyRun) {
		t.Fatalf("active legacy migration error = %v, want exact active run", err)
	}
	assertTemporalTableExists(t, ctx, admin, upgradeSchema, "run_temporal_frontiers", false)
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`UPDATE %s.runs SET status='completed' WHERE run_id=$1`, quoteTemporalIdent(upgradeSchema)), legacyRun)

	err = applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, false, true)
	if err == nil || !strings.Contains(err.Error(), "forced temporal migration rollback") {
		t.Fatalf("forced migration error = %v", err)
	}
	assertTemporalTableExists(t, ctx, admin, upgradeSchema, "run_temporal_frontiers", false)
	var legacyVersion string
	if err := admin.QueryRowContext(ctx, fmt.Sprintf(`SELECT platform_version FROM %s.runtime_store_metadata WHERE id=1`, quoteTemporalIdent(upgradeSchema))).Scan(&legacyVersion); err != nil {
		t.Fatalf("read rolled-back metadata: %v", err)
	}
	if legacyVersion != "0.7.0" {
		t.Fatalf("rolled-back platform version = %q, want 0.7.0", legacyVersion)
	}

	if err := applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, false, false); err != nil {
		t.Fatalf("recognized temporal upgrade: %v", err)
	}
	assertTemporalTargetMetadata(t, ctx, admin, upgradeSchema, ownerRole, runtimeRole)
	if err := applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, false, false); err != nil {
		t.Fatalf("idempotent temporal reapply: %v", err)
	}
	var migrationCount int
	if err := admin.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.runtime_store_migrations WHERE migration_id='temporal-frontier-v1'`, quoteTemporalIdent(upgradeSchema))).Scan(&migrationCount); err != nil {
		t.Fatalf("count temporal migrations: %v", err)
	}
	if migrationCount != 1 {
		t.Fatalf("temporal migration rows = %d, want 1", migrationCount)
	}

	runtimeDSN := temporalRoleDSN(connection.Parameters(), runtimeRole, runtimePassword)
	runtimeDB, err := sql.Open("postgres", runtimeDSN)
	if err != nil {
		t.Fatalf("open restricted runtime connection: %v", err)
	}
	defer runtimeDB.Close()
	if err := runtimeDB.PingContext(ctx); err != nil {
		t.Fatalf("ping restricted runtime connection: %v", err)
	}

	lockKey := temporalSchemaLockKey(upgradeSchema)
	runtimeLockConn, err := runtimeDB.Conn(ctx)
	if err != nil {
		t.Fatalf("open runtime lock connection: %v", err)
	}
	mustExecTemporal(t, ctx, runtimeLockConn, `SELECT pg_advisory_lock_shared($1)`, lockKey)
	var migrationLockAvailable bool
	if err := admin.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey).Scan(&migrationLockAvailable); err != nil {
		t.Fatalf("try migration lock: %v", err)
	}
	if migrationLockAvailable {
		_, _ = admin.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, lockKey)
		t.Fatal("migration exclusive lock succeeded while runtime held shared lock")
	}
	mustExecTemporal(t, ctx, runtimeLockConn, `SELECT pg_advisory_unlock_shared($1)`, lockKey)
	runtimeLockConn.Close()

	assertTemporalRuntimeAdmission(t, ctx, runtimeDB, upgradeSchema, ownerRole, runtimeRole)
	secondRuntimeDB, err := sql.Open("postgres", runtimeDSN)
	if err != nil {
		t.Fatalf("open second runtime connection: %v", err)
	}
	assertTemporalRuntimeAdmission(t, ctx, secondRuntimeDB, upgradeSchema, ownerRole, runtimeRole)
	secondRuntimeDB.Close()

	assertTemporalPrivilegeDenials(t, ctx, runtimeDB, upgradeSchema, ownerRole)

	runA := "10000000-0000-0000-0000-000000000001"
	runB := "20000000-0000-0000-0000-000000000002"
	runC := "30000000-0000-0000-0000-000000000003"
	seedTemporalVersionedRuns(t, ctx, admin, upgradeSchema, ownerRole, runA, runB, runC)

	eventID := "a0000000-0000-0000-0000-000000000001"
	deliveryID := "d0000000-0000-0000-0000-000000000001"
	receiptID := "e0000000-0000-0000-0000-000000000001"
	assertTemporalUndeclaredDMLRejected(t, ctx, runtimeDB, upgradeSchema, runA, eventID)
	writeTemporalEventDeliveryReceipt(t, ctx, runtimeDB, upgradeSchema, runA, eventID, deliveryID, receiptID)
	assertTemporalSharedRevision(t, ctx, runtimeDB, upgradeSchema, eventID, deliveryID, receiptID)
	assertTemporalDirectEventMutationRejected(t, ctx, runtimeDB, upgradeSchema, eventID)
	assertTemporalEventTriggerDefendsGrantDrift(t, ctx, admin, upgradeSchema, ownerRole, runA, eventID)
	assertTemporalUndeclaredDeliveryMutationRejected(t, ctx, runtimeDB, upgradeSchema, deliveryID)
	assertTemporalOwnershipMove(t, ctx, runtimeDB, upgradeSchema, deliveryID, runA, runB)
	assertTemporalDeclaredDeliveryDelete(t, ctx, runtimeDB, upgradeSchema, deliveryID, runB)
	assertTemporalRollbackPublishesNothing(t, ctx, runtimeDB, upgradeSchema, runC)
	assertTemporalRunlessLineage(t, ctx, runtimeDB, upgradeSchema)
	assertTemporalUnversionedDestruction(t, ctx, runtimeDB, upgradeSchema, legacyRun, legacyEvent, legacyDelivery, legacyReceipt)
	assertTemporalReverseClaimsSerialize(t, ctx, admin, runtimeDB, upgradeSchema, runA, runB)
}

func createTemporalLegacySchema(t *testing.T, ctx context.Context, db *sql.DB, schema, owner string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin legacy schema: %v", err)
	}
	defer tx.Rollback()
	mustExecTemporal(t, ctx, tx, `SET LOCAL ROLE `+quoteTemporalIdent(owner))
	mustExecTemporal(t, ctx, tx, temporalLegacyDDL(quoteTemporalIdent(schema)))
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit legacy schema: %v", err)
	}
}

func applyTemporalPrototype(ctx context.Context, db *sql.DB, schema, owner, runtimeRole string, fresh, forceRollback bool) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin temporal schema apply: %w", err)
	}
	defer tx.Rollback()

	lockKey := temporalSchemaLockKey(schema)
	var locked bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock($1)`, lockKey).Scan(&locked); err != nil {
		return fmt.Errorf("acquire temporal migration lock: %w", err)
	}
	if !locked {
		return fmt.Errorf("temporal schema migration blocked by an active runtime")
	}

	qSchema := quoteTemporalIdent(schema)
	var metadataExists bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, schema+`.runtime_store_metadata`).Scan(&metadataExists); err != nil {
		return fmt.Errorf("inspect temporal metadata table: %w", err)
	}
	if fresh {
		if metadataExists {
			return fmt.Errorf("fresh temporal apply requires an empty schema")
		}
		if _, err := tx.ExecContext(ctx, `SET LOCAL ROLE `+quoteTemporalIdent(owner)); err != nil {
			return fmt.Errorf("assume temporal schema owner: %w", err)
		}
		if _, err := tx.ExecContext(ctx, temporalLegacyDDL(qSchema)); err != nil {
			return fmt.Errorf("create fresh platform base: %w", err)
		}
		metadataExists = true
	}
	if !metadataExists {
		return fmt.Errorf("recognized runtime_store_metadata is required")
	}

	var targetColumns int
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_schema=$1 AND table_name='runtime_store_metadata'
		  AND column_name IN ('schema_generation','schema_ddl_sha256','schema_owner_role','runtime_role')
	`, schema).Scan(&targetColumns); err != nil {
		return fmt.Errorf("inspect temporal metadata generation: %w", err)
	}
	checksum := temporalPrototypeChecksum()
	if targetColumns == 4 {
		var generation, storedChecksum, storedOwner, storedRuntime string
		if err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT schema_generation,schema_ddl_sha256,schema_owner_role,runtime_role FROM %s.runtime_store_metadata WHERE id=1`, qSchema)).Scan(&generation, &storedChecksum, &storedOwner, &storedRuntime); err != nil {
			return fmt.Errorf("read temporal target metadata: %w", err)
		}
		if generation != "temporal-frontier-v1" || storedChecksum != checksum || storedOwner != owner || storedRuntime != runtimeRole {
			return fmt.Errorf("temporal target metadata drift")
		}
		var ledgerCount int
		if err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.runtime_store_migrations WHERE migration_id='temporal-frontier-v1' AND ddl_sha256=$1`, qSchema), checksum).Scan(&ledgerCount); err != nil || ledgerCount != 1 {
			return fmt.Errorf("temporal migration ledger drift: count=%d err=%v", ledgerCount, err)
		}
		return tx.Commit()
	}
	if targetColumns != 0 {
		return fmt.Errorf("partial temporal metadata shape is unsupported")
	}

	if _, err := tx.ExecContext(ctx, `SET LOCAL ROLE `+quoteTemporalIdent(owner)); err != nil {
		return fmt.Errorf("assume temporal schema owner: %w", err)
	}
	for _, table := range []string{"event_deliveries", "event_receipts", "events", "runs", "runtime_store_metadata"} {
		if _, err := tx.ExecContext(ctx, `LOCK TABLE `+qSchema+`.`+quoteTemporalIdent(table)+` IN ACCESS EXCLUSIVE MODE`); err != nil {
			return fmt.Errorf("lock legacy table %s: %w", table, err)
		}
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT run_id::text FROM %s.runs WHERE status IN ('running','paused') ORDER BY run_id::text`, qSchema))
	if err != nil {
		return fmt.Errorf("query active legacy runs: %w", err)
	}
	var active []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			rows.Close()
			return fmt.Errorf("scan active legacy run: %w", err)
		}
		active = append(active, runID)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close active legacy runs: %w", err)
	}
	if len(active) > 0 {
		return fmt.Errorf("active legacy runs block temporal migration: %s", strings.Join(active, ","))
	}

	var platformVersion string
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT platform_version FROM %s.runtime_store_metadata WHERE id=1`, qSchema)).Scan(&platformVersion); err != nil {
		return fmt.Errorf("read legacy platform version: %w", err)
	}
	if platformVersion != "0.7.0" {
		return fmt.Errorf("unrecognized legacy platform version %q", platformVersion)
	}
	if _, err := tx.ExecContext(ctx, temporalTargetDDL(qSchema)); err != nil {
		return fmt.Errorf("create temporal target schema: %w", err)
	}
	if _, err := tx.ExecContext(ctx, temporalFunctionsDDL(qSchema)); err != nil {
		return fmt.Errorf("create temporal functions and triggers: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %[1]s.run_temporal_frontiers(run_id,model_version,current_revision,history_complete)
		SELECT run_id,0,0,false FROM %[1]s.runs;
	`, qSchema)); err != nil {
		return fmt.Errorf("mark retained legacy runs unversioned: %w", err)
	}
	if _, err := tx.ExecContext(ctx, temporalGrantDDL(qSchema, quoteTemporalIdent(runtimeRole))); err != nil {
		return fmt.Errorf("apply temporal grants: %w", err)
	}
	if forceRollback {
		return fmt.Errorf("forced temporal migration rollback")
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s.runtime_store_migrations(migration_id,from_generation,to_generation,ddl_sha256,schema_owner_role,runtime_role)
		VALUES ('temporal-frontier-v1','pre-temporal-v0','temporal-frontier-v1',$1,$2,$3)
	`, qSchema), checksum, owner, runtimeRole); err != nil {
		return fmt.Errorf("write temporal migration ledger: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s.runtime_store_metadata
		SET platform_version='0.7.0', schema_generation='temporal-frontier-v1', schema_ddl_sha256=$1,
		    schema_owner_role=$2, runtime_role=$3
		WHERE id=1
	`, qSchema), checksum, owner, runtimeRole); err != nil {
		return fmt.Errorf("commit temporal metadata authority: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit temporal schema apply: %w", err)
	}
	return nil
}

func assertTemporalTargetMetadata(t *testing.T, ctx context.Context, db *sql.DB, schema, owner, runtimeRole string) {
	t.Helper()
	var generation, checksum, gotOwner, gotRuntime string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT schema_generation,schema_ddl_sha256,schema_owner_role,runtime_role FROM %s.runtime_store_metadata WHERE id=1`, quoteTemporalIdent(schema))).Scan(&generation, &checksum, &gotOwner, &gotRuntime); err != nil {
		t.Fatalf("read temporal target metadata: %v", err)
	}
	if generation != "temporal-frontier-v1" || checksum != temporalPrototypeChecksum() || gotOwner != owner || gotRuntime != runtimeRole {
		t.Fatalf("temporal metadata = generation:%q checksum:%q owner:%q runtime:%q", generation, checksum, gotOwner, gotRuntime)
	}
}

func assertTemporalTableExists(t *testing.T, ctx context.Context, db *sql.DB, schema, table string, want bool) {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, schema+`.`+table).Scan(&exists); err != nil {
		t.Fatalf("inspect temporal table %s.%s: %v", schema, table, err)
	}
	if exists != want {
		t.Fatalf("temporal table %s.%s exists = %v, want %v", schema, table, exists, want)
	}
}

func assertTemporalRuntimeAdmission(t *testing.T, ctx context.Context, db *sql.DB, schema, owner, runtimeRole string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	var currentUser, generation, metadataRuntime string
	var ownerMember, superuser, bypassRLS, createRole, schemaCreate bool
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT current_user,
		       m.schema_generation,
		       m.runtime_role,
		       pg_has_role(current_user,$1,'MEMBER'),
		       r.rolsuper,
		       r.rolbypassrls,
		       r.rolcreaterole,
		       has_schema_privilege(current_user,$2,'CREATE')
		FROM %[1]s.runtime_store_metadata m
		JOIN pg_roles r ON r.rolname=current_user
		WHERE m.id=1
	`, qSchema), owner, schema).Scan(&currentUser, &generation, &metadataRuntime, &ownerMember, &superuser, &bypassRLS, &createRole, &schemaCreate); err != nil {
		t.Fatalf("read temporal runtime admission: %v", err)
	}
	if currentUser != runtimeRole || metadataRuntime != runtimeRole || generation != "temporal-frontier-v1" || ownerMember || superuser || bypassRLS || createRole || schemaCreate {
		t.Fatalf("runtime admission drift: user=%q metadata=%q generation=%q owner_member=%v super=%v bypass=%v createrole=%v schema_create=%v", currentUser, metadataRuntime, generation, ownerMember, superuser, bypassRLS, createRole, schemaCreate)
	}

	var triggerCount int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_trigger t
		JOIN pg_class c ON c.oid=t.tgrelid
		JOIN pg_namespace n ON n.oid=c.relnamespace
		JOIN pg_proc p ON p.oid=t.tgfoid
		JOIN pg_roles owner_role ON owner_role.oid=p.proowner
		WHERE n.nspname=$1
		  AND c.relname IN ('events','event_deliveries','event_receipts')
		  AND NOT t.tgisinternal
		  AND t.tgenabled='A'
		  AND p.prosecdef
		  AND owner_role.rolname=$2
		  AND EXISTS (SELECT 1 FROM unnest(p.proconfig) cfg WHERE cfg LIKE 'search_path=pg_catalog,%')
	`, schema, owner).Scan(&triggerCount); err != nil {
		t.Fatalf("inspect temporal triggers: %v", err)
	}
	if triggerCount != 3 {
		t.Fatalf("admitted temporal trigger count = %d, want 3", triggerCount)
	}

	var eventInsert, eventUpdate, eventDelete, historyDML, helperExecute bool
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT has_table_privilege(current_user,'%[1]s.events','INSERT'),
		       has_table_privilege(current_user,'%[1]s.events','UPDATE'),
		       has_table_privilege(current_user,'%[1]s.events','DELETE'),
		       has_table_privilege(current_user,'%[1]s.event_delivery_history','INSERT,UPDATE,DELETE'),
		       has_function_privilege(current_user,'%[1]s.swarm_next_temporal_ordinal(uuid)','EXECUTE')
	`, qSchema)).Scan(&eventInsert, &eventUpdate, &eventDelete, &historyDML, &helperExecute); err != nil {
		t.Fatalf("inspect temporal grants: %v", err)
	}
	if !eventInsert || eventUpdate || eventDelete || historyDML || helperExecute {
		t.Fatalf("runtime grant drift: event_insert=%v event_update=%v event_delete=%v history_dml=%v helper_execute=%v", eventInsert, eventUpdate, eventDelete, historyDML, helperExecute)
	}
	functionGrants := []struct {
		signature string
		want      bool
	}{
		{signature: "swarm_claim_temporal_runs(uuid[])", want: true},
		{signature: "swarm_destroy_run(uuid,text)", want: true},
		{signature: "swarm_declare_temporal_runs(uuid[],text)", want: false},
		{signature: "swarm_next_temporal_ordinal(uuid)", want: false},
		{signature: "swarm_guard_events()", want: false},
		{signature: "swarm_guard_event_deliveries()", want: false},
		{signature: "swarm_guard_event_receipts()", want: false},
	}
	for _, grant := range functionGrants {
		var got bool
		if err := db.QueryRowContext(ctx, `SELECT has_function_privilege(current_user,$1,'EXECUTE')`, schema+`.`+grant.signature).Scan(&got); err != nil {
			t.Fatalf("inspect temporal function grant %s: %v", grant.signature, err)
		}
		if got != grant.want {
			t.Fatalf("runtime EXECUTE on %s = %v, want %v", grant.signature, got, grant.want)
		}
	}
}

func assertTemporalPrivilegeDenials(t *testing.T, ctx context.Context, db *sql.DB, schema, owner string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	assertTemporalExecFails(t, ctx, db, `SET ROLE `+quoteTemporalIdent(owner), "assume schema owner")
	assertTemporalExecFails(t, ctx, db, `ALTER TABLE `+qSchema+`.events DISABLE TRIGGER ALL`, "disable temporal trigger")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`INSERT INTO %s.event_delivery_history(run_id,revision,ordinal,operation,fact_id) VALUES ('00000000-0000-0000-0000-000000000000',1,1,'insert','forged')`, qSchema), "write temporal history")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`INSERT INTO %s.run_temporal_transactions(transaction_id,run_ids,mode) VALUES (pg_current_xact_id(),ARRAY['00000000-0000-0000-0000-000000000000'::uuid],'normal')`, qSchema), "write temporal transaction authority")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`UPDATE %s.run_temporal_frontiers SET current_revision=current_revision+1`, qSchema), "write temporal frontier authority")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`DELETE FROM %s.run_temporal_revisions`, qSchema), "write temporal revision authority")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`DELETE FROM %s.runtime_store_migrations`, qSchema), "write temporal migration ledger")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`UPDATE %s.runtime_store_metadata SET schema_generation='forged'`, qSchema), "write temporal metadata")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`DELETE FROM %s.run_deletion_tombstones`, qSchema), "write temporal deletion evidence")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`SELECT * FROM %s.swarm_next_temporal_ordinal('00000000-0000-0000-0000-000000000000'::uuid)`, qSchema), "invoke ordinal bypass")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`SELECT %s.swarm_declare_temporal_runs(ARRAY['00000000-0000-0000-0000-000000000000'::uuid],'destructive')`, qSchema), "invoke destructive declaration bypass")
}

func seedTemporalVersionedRuns(t *testing.T, ctx context.Context, db *sql.DB, schema, owner string, runIDs ...string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin temporal run seed: %v", err)
	}
	defer tx.Rollback()
	mustExecTemporal(t, ctx, tx, `SET LOCAL ROLE `+quoteTemporalIdent(owner))
	qSchema := quoteTemporalIdent(schema)
	for _, runID := range runIDs {
		mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.runs(run_id,status) VALUES ($1,'running')`, qSchema), runID)
		mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.run_temporal_frontiers(run_id,model_version,current_revision,history_complete) VALUES ($1,1,0,true)`, qSchema), runID)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit temporal run seed: %v", err)
	}
}

func assertTemporalUndeclaredDMLRejected(t *testing.T, ctx context.Context, db *sql.DB, schema, runID, eventID string) {
	t.Helper()
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`INSERT INTO %s.events(event_id,run_id,temporal_revision,temporal_ordinal) VALUES ($1,$2,999,999)`, quoteTemporalIdent(schema)), "undeclared event insert", eventID, runID)
}

func writeTemporalEventDeliveryReceipt(t *testing.T, ctx context.Context, db *sql.DB, schema, runID, eventID, deliveryID, receiptID string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin event/delivery/receipt transaction: %v", err)
	}
	defer tx.Rollback()
	qSchema := quoteTemporalIdent(schema)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.events(event_id,run_id,temporal_revision,temporal_ordinal) VALUES ($1,$2,999,999)`, qSchema), eventID, runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.event_deliveries(delivery_id,run_id,event_id,status,temporal_revision,temporal_ordinal) VALUES ($1,$2,$3,'pending',999,999)`, qSchema), deliveryID, runID, eventID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.event_receipts(receipt_id,event_id,outcome,temporal_revision,temporal_ordinal) VALUES ($1,$2,'success',999,999)`, qSchema), receiptID, eventID)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit event/delivery/receipt transaction: %v", err)
	}
}

func assertTemporalSharedRevision(t *testing.T, ctx context.Context, db *sql.DB, schema, eventID, deliveryID, receiptID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	var eventRev, deliveryRev, receiptRev int64
	var eventOrdinal, deliveryOrdinal, receiptOrdinal int
	var receiptRun string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT e.temporal_revision,e.temporal_ordinal,
		       d.temporal_revision,d.temporal_ordinal,
		       r.temporal_revision,r.temporal_ordinal,r.temporal_run_id::text
		FROM %[1]s.events e
		JOIN %[1]s.event_deliveries d ON d.delivery_id=$2
		JOIN %[1]s.event_receipts r ON r.receipt_id=$3
		WHERE e.event_id=$1
	`, qSchema), eventID, deliveryID, receiptID).Scan(&eventRev, &eventOrdinal, &deliveryRev, &deliveryOrdinal, &receiptRev, &receiptOrdinal, &receiptRun); err != nil {
		t.Fatalf("read event/delivery/receipt temporal stamps: %v", err)
	}
	if eventRev != deliveryRev || eventRev != receiptRev || eventOrdinal != 1 || deliveryOrdinal != 2 || receiptOrdinal != 3 || receiptRun == "" {
		t.Fatalf("shared revision = %d/%d/%d ordinals=%d/%d/%d receipt_run=%q", eventRev, deliveryRev, receiptRev, eventOrdinal, deliveryOrdinal, receiptOrdinal, receiptRun)
	}
	var deliveryHistory, receiptHistory int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT (SELECT count(*) FROM %[1]s.event_delivery_history),(SELECT count(*) FROM %[1]s.event_receipt_history)`, qSchema)).Scan(&deliveryHistory, &receiptHistory); err != nil {
		t.Fatalf("count typed temporal history: %v", err)
	}
	if deliveryHistory != 1 || receiptHistory != 1 {
		t.Fatalf("typed temporal history counts = delivery:%d receipt:%d, want 1/1", deliveryHistory, receiptHistory)
	}
}

func assertTemporalDirectEventMutationRejected(t *testing.T, ctx context.Context, db *sql.DB, schema, eventID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`UPDATE %s.events SET created_at=clock_timestamp() WHERE event_id=$1`, qSchema), "runtime event update", eventID)
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`DELETE FROM %s.events WHERE event_id=$1`, qSchema), "runtime event delete", eventID)
}

func assertTemporalEventTriggerDefendsGrantDrift(t *testing.T, ctx context.Context, db *sql.DB, schema, owner, runID, eventID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	for _, mutation := range []struct {
		name string
		sql  string
	}{
		{name: "update", sql: fmt.Sprintf(`UPDATE %s.events SET created_at=clock_timestamp() WHERE event_id=$1`, qSchema)},
		{name: "normal delete", sql: fmt.Sprintf(`DELETE FROM %s.events WHERE event_id=$1`, qSchema)},
	} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin owner event %s proof: %v", mutation.name, err)
		}
		mustExecTemporal(t, ctx, tx, `SET LOCAL ROLE `+quoteTemporalIdent(owner))
		mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runID)
		if _, err := tx.ExecContext(ctx, mutation.sql, eventID); err == nil {
			_ = tx.Rollback()
			t.Fatalf("owner event %s bypassed ENABLE ALWAYS trigger", mutation.name)
		}
		_ = tx.Rollback()
	}
}

func assertTemporalUndeclaredDeliveryMutationRejected(t *testing.T, ctx context.Context, db *sql.DB, schema, deliveryID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`UPDATE %s.event_deliveries SET status='delivered' WHERE delivery_id=$1`, qSchema), "undeclared delivery update", deliveryID)
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`DELETE FROM %s.event_deliveries WHERE delivery_id=$1`, qSchema), "undeclared delivery delete", deliveryID)
}

func assertTemporalOwnershipMove(t *testing.T, ctx context.Context, db *sql.DB, schema, deliveryID, runA, runB string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin incomplete ownership move: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runB)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s.event_deliveries SET run_id=$1 WHERE delivery_id=$2`, qSchema), runB, deliveryID); err == nil {
		t.Fatal("ownership move with NEW run only unexpectedly succeeded")
	}
	_ = tx.Rollback()

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin complete ownership move: %v", err)
	}
	defer tx.Rollback()
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid,$2::uuid])`, qSchema), runB, runA)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`UPDATE %s.event_deliveries SET run_id=$1 WHERE delivery_id=$2`, qSchema), runB, deliveryID)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit complete ownership move: %v", err)
	}
	var gotRun string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT run_id::text FROM %s.event_deliveries WHERE delivery_id=$1`, qSchema), deliveryID).Scan(&gotRun); err != nil {
		t.Fatalf("read moved delivery: %v", err)
	}
	if gotRun != runB {
		t.Fatalf("moved delivery run = %s, want %s", gotRun, runB)
	}
}

func assertTemporalDeclaredDeliveryDelete(t *testing.T, ctx context.Context, db *sql.DB, schema, deliveryID, runID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	var before int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.event_delivery_history WHERE operation='delete' AND fact_id=$1`, qSchema), deliveryID).Scan(&before); err != nil {
		t.Fatalf("count delivery delete history before mutation: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin declared delivery delete: %v", err)
	}
	defer tx.Rollback()
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`DELETE FROM %s.event_deliveries WHERE delivery_id=$1`, qSchema), deliveryID)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit declared delivery delete: %v", err)
	}
	var after int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.event_delivery_history WHERE operation='delete' AND fact_id=$1`, qSchema), deliveryID).Scan(&after); err != nil {
		t.Fatalf("count delivery delete history after mutation: %v", err)
	}
	if after != before+1 {
		t.Fatalf("declared delivery delete history count = %d, want %d", after, before+1)
	}
}

func assertTemporalRollbackPublishesNothing(t *testing.T, ctx context.Context, db *sql.DB, schema, runID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin temporal rollback proof: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.events(event_id,run_id) VALUES ('c0000000-0000-0000-0000-000000000001',$1)`, qSchema), runID)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback temporal transaction: %v", err)
	}
	var frontier, revisionCount, eventCount int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT f.current_revision,
		       (SELECT count(*) FROM %[1]s.run_temporal_revisions r WHERE r.run_id=f.run_id),
		       (SELECT count(*) FROM %[1]s.events e WHERE e.run_id=f.run_id)
		FROM %[1]s.run_temporal_frontiers f WHERE f.run_id=$1
	`, qSchema), runID).Scan(&frontier, &revisionCount, &eventCount); err != nil {
		t.Fatalf("read rollback temporal authority: %v", err)
	}
	if frontier != 0 || revisionCount != 0 || eventCount != 0 {
		t.Fatalf("rollback published frontier=%d revisions=%d events=%d", frontier, revisionCount, eventCount)
	}
}

func assertTemporalRunlessLineage(t *testing.T, ctx context.Context, db *sql.DB, schema string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	eventID := "f0000000-0000-0000-0000-000000000001"
	receiptID := "f0000000-0000-0000-0000-000000000002"
	mustExecTemporal(t, ctx, db, fmt.Sprintf(`INSERT INTO %s.events(event_id,run_id) VALUES ($1,NULL)`, qSchema), eventID)
	mustExecTemporal(t, ctx, db, fmt.Sprintf(`INSERT INTO %s.event_receipts(receipt_id,event_id,outcome) VALUES ($1,$2,'success')`, qSchema), receiptID, eventID)
	var eventRevision, receiptRevision sql.NullInt64
	var receiptRun sql.NullString
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT e.temporal_revision,r.temporal_run_id::text,r.temporal_revision
		FROM %[1]s.events e JOIN %[1]s.event_receipts r ON r.event_id=e.event_id
		WHERE e.event_id=$1
	`, qSchema), eventID).Scan(&eventRevision, &receiptRun, &receiptRevision); err == nil {
	} else {
		t.Fatalf("read runless temporal lineage: %v", err)
	}
	if eventRevision.Valid || receiptRun.Valid || receiptRevision.Valid {
		t.Fatalf("runless temporal stamps = event:%v receipt_run:%v receipt:%v, want NULL", eventRevision, receiptRun, receiptRevision)
	}
}

func assertTemporalUnversionedDestruction(t *testing.T, ctx context.Context, db *sql.DB, schema, runID, eventID, deliveryID, receiptID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin unversioned normal claim: %v", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runID); err == nil {
		t.Fatal("normal claim for unversioned run unexpectedly succeeded")
	}
	_ = tx.Rollback()

	mustExecTemporal(t, ctx, db, fmt.Sprintf(`SELECT %s.swarm_destroy_run($1,'conformance')`, qSchema), runID)
	var tombstoneVersion int
	var lastRevision sql.NullInt64
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT source_model_version,last_revision FROM %s.run_deletion_tombstones WHERE run_id=$1`, qSchema), runID).Scan(&tombstoneVersion, &lastRevision); err != nil {
		t.Fatalf("read unversioned deletion tombstone: %v", err)
	}
	if tombstoneVersion != 0 || lastRevision.Valid {
		t.Fatalf("unversioned tombstone = version:%d revision:%v, want 0/NULL", tombstoneVersion, lastRevision)
	}
	var revisionCount int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.run_temporal_revisions WHERE run_id=$1`, qSchema), runID).Scan(&revisionCount); err != nil {
		t.Fatalf("count unversioned destructive revisions: %v", err)
	}
	if revisionCount != 0 {
		t.Fatalf("unversioned destructive cleanup created %d ordinary revisions", revisionCount)
	}
	var remainingFacts int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT (SELECT count(*) FROM %[1]s.events WHERE event_id=$1)
		     + (SELECT count(*) FROM %[1]s.event_deliveries WHERE delivery_id=$2)
		     + (SELECT count(*) FROM %[1]s.event_receipts WHERE receipt_id=$3)
	`, qSchema), eventID, deliveryID, receiptID).Scan(&remainingFacts); err != nil {
		t.Fatalf("count destructively cascaded legacy facts: %v", err)
	}
	if remainingFacts != 0 {
		t.Fatalf("destructive legacy cascade left %d facts", remainingFacts)
	}
}

func assertTemporalReverseClaimsSerialize(t *testing.T, ctx context.Context, admin, runtimeDB *sql.DB, schema, runA, runB string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	tx1, err := runtimeDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin first reverse claim: %v", err)
	}
	defer tx1.Rollback()
	mustExecTemporal(t, ctx, tx1, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid,$2::uuid])`, qSchema), runB, runA)

	conn2, err := runtimeDB.Conn(ctx)
	if err != nil {
		t.Fatalf("open second reverse claim connection: %v", err)
	}
	defer conn2.Close()
	tx2, err := conn2.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin second reverse claim: %v", err)
	}
	defer tx2.Rollback()
	var pid int
	if err := tx2.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatalf("read second reverse claim pid: %v", err)
	}

	result := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := tx2.ExecContext(ctx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid,$2::uuid])`, qSchema), runA, runB)
		result <- err
	}()

	deadline := time.Now().Add(3 * time.Second)
	waiting := false
	for time.Now().Before(deadline) {
		if err := admin.QueryRowContext(ctx, `SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&waiting); err == nil && waiting {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("reverse-order temporal claim did not block on sorted frontier lock")
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("commit first reverse claim: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("second reverse claim after serialization: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit second reverse claim: %v", err)
	}
	wg.Wait()
}

func assertTemporalExecFails(t *testing.T, ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, query, label string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err == nil {
		t.Fatalf("%s unexpectedly succeeded", label)
	}
}

func mustExecTemporal(t *testing.T, ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("execute temporal conformance SQL: %v\n%s", err, query)
	}
}

func temporalRoleDSN(params testpostgres.Parameters, user, password string) string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		quoteTemporalDSNValue(params.Host), params.Port, quoteTemporalDSNValue(params.Database),
		quoteTemporalDSNValue(user), quoteTemporalDSNValue(password), quoteTemporalDSNValue(params.SSLMode))
}

func quoteTemporalDSNValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	return `'` + value + `'`
}

func quoteTemporalIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteTemporalLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}

func temporalSchemaLockKey(schema string) int64 {
	digest := sha256.Sum256([]byte("swarm:temporal-schema:" + schema))
	var key int64
	for _, b := range digest[:8] {
		key = key<<8 | int64(b)
	}
	return key
}

func temporalPrototypeChecksum() string {
	digest := sha256.Sum256([]byte(temporalPrototypeSchemaTemplate + "\x00" + temporalPrototypeFunctionsTemplate + "\x00" + temporalPrototypeGrantTemplate))
	return hex.EncodeToString(digest[:])
}

func temporalLegacyDDL(schema string) string {
	return fmt.Sprintf(temporalLegacySchemaTemplate, schema)
}

func temporalTargetDDL(schema string) string {
	return fmt.Sprintf(temporalPrototypeSchemaTemplate, schema)
}

func temporalFunctionsDDL(schema string) string {
	return fmt.Sprintf(temporalPrototypeFunctionsTemplate, schema)
}

func temporalGrantDDL(schema, runtimeRole string) string {
	return fmt.Sprintf(temporalPrototypeGrantTemplate, schema, runtimeRole)
}

const temporalLegacySchemaTemplate = `
	CREATE TABLE %[1]s.runtime_store_metadata (
		id INTEGER PRIMARY KEY CHECK (id=1),
		swarm_version TEXT NOT NULL,
		platform_version TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
	);
	INSERT INTO %[1]s.runtime_store_metadata(id,swarm_version,platform_version) VALUES (1,'conformance','0.7.0');
	CREATE TABLE %[1]s.runs (
		run_id UUID PRIMARY KEY,
		status TEXT NOT NULL CHECK (status IN ('running','paused','completed','failed','cancelled','forked'))
	);
	CREATE TABLE %[1]s.events (
		event_id UUID PRIMARY KEY,
		run_id UUID REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
	);
	CREATE TABLE %[1]s.event_deliveries (
		delivery_id UUID PRIMARY KEY,
		run_id UUID REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		event_id UUID NOT NULL REFERENCES %[1]s.events(event_id) ON DELETE CASCADE,
		status TEXT NOT NULL CHECK (status IN ('pending','in_progress','delivered','failed','dead_letter'))
	);
	CREATE TABLE %[1]s.event_receipts (
		receipt_id UUID PRIMARY KEY,
		event_id UUID NOT NULL REFERENCES %[1]s.events(event_id) ON DELETE CASCADE,
		outcome TEXT NOT NULL
	);
`

const temporalPrototypeSchemaTemplate = `
	ALTER TABLE %[1]s.runtime_store_metadata
		ADD COLUMN schema_generation TEXT,
		ADD COLUMN schema_ddl_sha256 TEXT,
		ADD COLUMN schema_owner_role TEXT,
		ADD COLUMN runtime_role TEXT;
	ALTER TABLE %[1]s.events
		ADD COLUMN temporal_revision BIGINT,
		ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.event_deliveries
		ADD COLUMN temporal_revision BIGINT,
		ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.event_receipts
		ADD COLUMN temporal_run_id UUID REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		ADD COLUMN temporal_revision BIGINT,
		ADD COLUMN temporal_ordinal INTEGER;
	UPDATE %[1]s.event_receipts AS receipt
	SET temporal_run_id=event.run_id
	FROM %[1]s.events AS event
	WHERE event.event_id=receipt.event_id;

	CREATE TABLE %[1]s.run_temporal_transactions (
		transaction_id XID8 PRIMARY KEY,
		run_ids UUID[] NOT NULL,
		mode TEXT NOT NULL CHECK (mode IN ('normal','destructive')),
		declared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
		CHECK (cardinality(run_ids) > 0)
	);
	CREATE TABLE %[1]s.run_temporal_frontiers (
		run_id UUID PRIMARY KEY REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		model_version INTEGER NOT NULL CHECK (model_version IN (0,1)),
		current_revision BIGINT NOT NULL DEFAULT 0 CHECK (current_revision >= 0),
		history_complete BOOLEAN NOT NULL DEFAULT FALSE,
		CHECK ((model_version=0 AND history_complete=FALSE AND current_revision=0)
		    OR (model_version=1 AND history_complete=TRUE))
	);
	CREATE TABLE %[1]s.run_temporal_revisions (
		run_id UUID NOT NULL REFERENCES %[1]s.run_temporal_frontiers(run_id) ON DELETE CASCADE,
		revision BIGINT NOT NULL CHECK (revision > 0),
		transaction_id XID8 NOT NULL REFERENCES %[1]s.run_temporal_transactions(transaction_id),
		next_ordinal INTEGER NOT NULL DEFAULT 0 CHECK (next_ordinal >= 0),
		recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
		PRIMARY KEY (run_id,revision),
		UNIQUE (run_id,transaction_id)
	);
	CREATE TABLE %[1]s.runtime_store_migrations (
		migration_id TEXT PRIMARY KEY,
		from_generation TEXT NOT NULL,
		to_generation TEXT NOT NULL,
		ddl_sha256 TEXT NOT NULL CHECK (ddl_sha256 ~ '^[0-9a-f]{64}$'),
		schema_owner_role TEXT NOT NULL,
		runtime_role TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
		CHECK (schema_owner_role <> runtime_role)
	);
	CREATE TABLE %[1]s.run_deletion_tombstones (
		deletion_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		run_id UUID NOT NULL UNIQUE,
		source_model_version INTEGER NOT NULL CHECK (source_model_version IN (0,1)),
		last_revision BIGINT CHECK (last_revision IS NULL OR last_revision >= 0),
		transaction_id XID8 NOT NULL REFERENCES %[1]s.run_temporal_transactions(transaction_id),
		deleted_by TEXT NOT NULL,
		deleted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
		CHECK ((source_model_version=0 AND last_revision IS NULL)
		    OR (source_model_version=1 AND last_revision IS NOT NULL))
	);
	CREATE TABLE %[1]s.run_lifecycle_history (
		history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		run_id UUID NOT NULL,
		revision BIGINT NOT NULL,
		ordinal INTEGER NOT NULL CHECK (ordinal > 0),
		operation TEXT NOT NULL CHECK (operation IN ('insert','update','delete')),
		fact_id TEXT NOT NULL,
		before_state JSONB,
		after_state JSONB,
		FOREIGN KEY (run_id,revision) REFERENCES %[1]s.run_temporal_revisions(run_id,revision) ON DELETE CASCADE,
		UNIQUE (run_id,revision,ordinal)
	);
	CREATE TABLE %[1]s.event_delivery_history (LIKE %[1]s.run_lifecycle_history INCLUDING ALL);
	CREATE TABLE %[1]s.event_receipt_history (LIKE %[1]s.run_lifecycle_history INCLUDING ALL);
	CREATE TABLE %[1]s.timer_history (LIKE %[1]s.run_lifecycle_history INCLUDING ALL);
	CREATE TABLE %[1]s.entity_state_history (LIKE %[1]s.run_lifecycle_history INCLUDING ALL);
	CREATE TABLE %[1]s.agent_session_history (LIKE %[1]s.run_lifecycle_history INCLUDING ALL);
	CREATE TABLE %[1]s.conversation_audit_history (LIKE %[1]s.run_lifecycle_history INCLUDING ALL);
	CREATE TABLE %[1]s.reply_context_history (LIKE %[1]s.run_lifecycle_history INCLUDING ALL);
	CREATE TABLE %[1]s.activity_attempt_history (LIKE %[1]s.run_lifecycle_history INCLUDING ALL);
	ALTER TABLE %[1]s.event_delivery_history ADD FOREIGN KEY (run_id,revision) REFERENCES %[1]s.run_temporal_revisions(run_id,revision) ON DELETE CASCADE;
	ALTER TABLE %[1]s.event_receipt_history ADD FOREIGN KEY (run_id,revision) REFERENCES %[1]s.run_temporal_revisions(run_id,revision) ON DELETE CASCADE;
	ALTER TABLE %[1]s.timer_history ADD FOREIGN KEY (run_id,revision) REFERENCES %[1]s.run_temporal_revisions(run_id,revision) ON DELETE CASCADE;
	ALTER TABLE %[1]s.entity_state_history ADD FOREIGN KEY (run_id,revision) REFERENCES %[1]s.run_temporal_revisions(run_id,revision) ON DELETE CASCADE;
	ALTER TABLE %[1]s.agent_session_history ADD FOREIGN KEY (run_id,revision) REFERENCES %[1]s.run_temporal_revisions(run_id,revision) ON DELETE CASCADE;
	ALTER TABLE %[1]s.conversation_audit_history ADD FOREIGN KEY (run_id,revision) REFERENCES %[1]s.run_temporal_revisions(run_id,revision) ON DELETE CASCADE;
	ALTER TABLE %[1]s.reply_context_history ADD FOREIGN KEY (run_id,revision) REFERENCES %[1]s.run_temporal_revisions(run_id,revision) ON DELETE CASCADE;
	ALTER TABLE %[1]s.activity_attempt_history ADD FOREIGN KEY (run_id,revision) REFERENCES %[1]s.run_temporal_revisions(run_id,revision) ON DELETE CASCADE;
`

const temporalPrototypeFunctionsTemplate = `
	CREATE FUNCTION %[1]s.swarm_declare_temporal_runs(requested_run_ids UUID[], requested_mode TEXT)
	RETURNS VOID
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		current_xid XID8 := pg_current_xact_id();
		normalized UUID[];
		existing_run_ids UUID[];
		existing_mode TEXT;
		target_run UUID;
		target_model INTEGER;
		target_history_complete BOOLEAN;
		next_revision BIGINT;
	BEGIN
		IF requested_mode NOT IN ('normal','destructive') THEN
			RAISE EXCEPTION 'unsupported temporal declaration mode %%', requested_mode;
		END IF;
		SELECT array_agg(run_id ORDER BY run_id)
		INTO normalized
		FROM (SELECT DISTINCT unnest(requested_run_ids) AS run_id) declared
		WHERE run_id IS NOT NULL;
		IF normalized IS NULL OR cardinality(normalized)=0 THEN
			RAISE EXCEPTION 'temporal declaration requires at least one run';
		END IF;
		SELECT run_ids,mode INTO existing_run_ids,existing_mode
		FROM %[1]s.run_temporal_transactions
		WHERE transaction_id=current_xid;
		IF FOUND THEN
			IF existing_run_ids<>normalized OR existing_mode<>requested_mode THEN
				RAISE EXCEPTION 'temporal declaration is sealed for this transaction';
			END IF;
			RETURN;
		END IF;
		INSERT INTO %[1]s.run_temporal_transactions(transaction_id,run_ids,mode)
		VALUES (current_xid,normalized,requested_mode);
		FOREACH target_run IN ARRAY normalized LOOP
			SELECT model_version,history_complete
			INTO target_model,target_history_complete
			FROM %[1]s.run_temporal_frontiers
			WHERE run_id=target_run
			FOR UPDATE;
			IF NOT FOUND THEN
				RAISE EXCEPTION 'run %% has no temporal frontier', target_run;
			END IF;
			IF requested_mode='normal' THEN
				IF target_model<>1 OR NOT target_history_complete THEN
					RAISE EXCEPTION 'run %% temporal history is unproven', target_run;
				END IF;
				UPDATE %[1]s.run_temporal_frontiers
				SET current_revision=current_revision+1
				WHERE run_id=target_run
				RETURNING current_revision INTO next_revision;
				INSERT INTO %[1]s.run_temporal_revisions(run_id,revision,transaction_id)
				VALUES (target_run,next_revision,current_xid);
			END IF;
		END LOOP;
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_claim_temporal_runs(requested_run_ids UUID[])
	RETURNS VOID
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	BEGIN
		PERFORM %[1]s.swarm_declare_temporal_runs(requested_run_ids,'normal');
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_next_temporal_ordinal(target_run UUID)
	RETURNS TABLE(temporal_revision BIGINT, temporal_ordinal INTEGER)
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		current_xid XID8 := pg_current_xact_id();
		declared_mode TEXT;
	BEGIN
		SELECT mode INTO declared_mode
		FROM %[1]s.run_temporal_transactions
		WHERE transaction_id=current_xid AND target_run=ANY(run_ids);
		IF NOT FOUND OR declared_mode<>'normal' THEN
			RAISE EXCEPTION 'run %% is not in the sealed normal temporal declaration', target_run;
		END IF;
		UPDATE %[1]s.run_temporal_revisions
		SET next_ordinal=next_ordinal+1
		WHERE run_id=target_run AND transaction_id=current_xid
		RETURNING revision,next_ordinal INTO temporal_revision,temporal_ordinal;
		IF NOT FOUND THEN
			RAISE EXCEPTION 'run %% has no revision for the current transaction', target_run;
		END IF;
		RETURN NEXT;
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_guard_events()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		stamp RECORD;
		allowed BOOLEAN;
	BEGIN
		IF TG_OP='UPDATE' THEN
			RAISE EXCEPTION 'events are immutable';
		ELSIF TG_OP='DELETE' THEN
			IF OLD.run_id IS NULL THEN
				RAISE EXCEPTION 'runless event deletion has no destructive authority';
			END IF;
			SELECT EXISTS (
				SELECT 1 FROM %[1]s.run_temporal_transactions
				WHERE transaction_id=pg_current_xact_id() AND mode='destructive' AND OLD.run_id=ANY(run_ids)
			) INTO allowed;
			IF NOT allowed THEN
				RAISE EXCEPTION 'event deletion requires sealed destructive run authority';
			END IF;
			RETURN OLD;
		END IF;
		IF NEW.run_id IS NULL THEN
			NEW.temporal_revision=NULL;
			NEW.temporal_ordinal=NULL;
			RETURN NEW;
		END IF;
		SELECT * INTO stamp FROM %[1]s.swarm_next_temporal_ordinal(NEW.run_id);
		NEW.temporal_revision=stamp.temporal_revision;
		NEW.temporal_ordinal=stamp.temporal_ordinal;
		RETURN NEW;
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_guard_event_deliveries()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		old_stamp RECORD;
		new_stamp RECORD;
		destructive BOOLEAN;
	BEGIN
		IF TG_OP='DELETE' THEN
			IF OLD.run_id IS NULL THEN RETURN OLD; END IF;
			SELECT EXISTS (
				SELECT 1 FROM %[1]s.run_temporal_transactions
				WHERE transaction_id=pg_current_xact_id() AND mode='destructive' AND OLD.run_id=ANY(run_ids)
			) INTO destructive;
			IF destructive THEN RETURN OLD; END IF;
			SELECT * INTO old_stamp FROM %[1]s.swarm_next_temporal_ordinal(OLD.run_id);
			INSERT INTO %[1]s.event_delivery_history(run_id,revision,ordinal,operation,fact_id,before_state)
			VALUES (OLD.run_id,old_stamp.temporal_revision,old_stamp.temporal_ordinal,'delete',OLD.delivery_id::text,to_jsonb(OLD));
			RETURN OLD;
		ELSIF TG_OP='INSERT' THEN
			IF NEW.run_id IS NULL THEN
				NEW.temporal_revision=NULL; NEW.temporal_ordinal=NULL; RETURN NEW;
			END IF;
			SELECT * INTO new_stamp FROM %[1]s.swarm_next_temporal_ordinal(NEW.run_id);
			NEW.temporal_revision=new_stamp.temporal_revision;
			NEW.temporal_ordinal=new_stamp.temporal_ordinal;
			INSERT INTO %[1]s.event_delivery_history(run_id,revision,ordinal,operation,fact_id,after_state)
			VALUES (NEW.run_id,new_stamp.temporal_revision,new_stamp.temporal_ordinal,'insert',NEW.delivery_id::text,to_jsonb(NEW));
			RETURN NEW;
		END IF;
		IF OLD.run_id IS NOT DISTINCT FROM NEW.run_id THEN
			IF NEW.run_id IS NULL THEN
				NEW.temporal_revision=NULL; NEW.temporal_ordinal=NULL; RETURN NEW;
			END IF;
			SELECT * INTO new_stamp FROM %[1]s.swarm_next_temporal_ordinal(NEW.run_id);
			NEW.temporal_revision=new_stamp.temporal_revision;
			NEW.temporal_ordinal=new_stamp.temporal_ordinal;
			INSERT INTO %[1]s.event_delivery_history(run_id,revision,ordinal,operation,fact_id,before_state,after_state)
			VALUES (NEW.run_id,new_stamp.temporal_revision,new_stamp.temporal_ordinal,'update',NEW.delivery_id::text,to_jsonb(OLD),to_jsonb(NEW));
			RETURN NEW;
		END IF;
		IF OLD.run_id IS NOT NULL THEN
			SELECT * INTO old_stamp FROM %[1]s.swarm_next_temporal_ordinal(OLD.run_id);
			INSERT INTO %[1]s.event_delivery_history(run_id,revision,ordinal,operation,fact_id,before_state,after_state)
			VALUES (OLD.run_id,old_stamp.temporal_revision,old_stamp.temporal_ordinal,'update',OLD.delivery_id::text,to_jsonb(OLD),to_jsonb(NEW));
		END IF;
		IF NEW.run_id IS NOT NULL THEN
			SELECT * INTO new_stamp FROM %[1]s.swarm_next_temporal_ordinal(NEW.run_id);
			NEW.temporal_revision=new_stamp.temporal_revision;
			NEW.temporal_ordinal=new_stamp.temporal_ordinal;
			INSERT INTO %[1]s.event_delivery_history(run_id,revision,ordinal,operation,fact_id,before_state,after_state)
			VALUES (NEW.run_id,new_stamp.temporal_revision,new_stamp.temporal_ordinal,'update',NEW.delivery_id::text,to_jsonb(OLD),to_jsonb(NEW));
		ELSE
			NEW.temporal_revision=NULL; NEW.temporal_ordinal=NULL;
		END IF;
		RETURN NEW;
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_guard_event_receipts()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		old_run UUID;
		new_run UUID;
		old_stamp RECORD;
		new_stamp RECORD;
		destructive BOOLEAN;
	BEGIN
		IF TG_OP<>'INSERT' THEN old_run=OLD.temporal_run_id; END IF;
		IF TG_OP<>'DELETE' THEN
			SELECT run_id INTO STRICT new_run FROM %[1]s.events WHERE event_id=NEW.event_id;
			NEW.temporal_run_id=new_run;
		END IF;
		IF TG_OP='DELETE' THEN
			IF old_run IS NULL THEN RETURN OLD; END IF;
			SELECT EXISTS (
				SELECT 1 FROM %[1]s.run_temporal_transactions
				WHERE transaction_id=pg_current_xact_id() AND mode='destructive' AND old_run=ANY(run_ids)
			) INTO destructive;
			IF destructive THEN RETURN OLD; END IF;
			SELECT * INTO old_stamp FROM %[1]s.swarm_next_temporal_ordinal(old_run);
			INSERT INTO %[1]s.event_receipt_history(run_id,revision,ordinal,operation,fact_id,before_state)
			VALUES (old_run,old_stamp.temporal_revision,old_stamp.temporal_ordinal,'delete',OLD.receipt_id::text,to_jsonb(OLD));
			RETURN OLD;
		ELSIF TG_OP='INSERT' THEN
			IF new_run IS NULL THEN
				NEW.temporal_revision=NULL; NEW.temporal_ordinal=NULL; RETURN NEW;
			END IF;
			SELECT * INTO new_stamp FROM %[1]s.swarm_next_temporal_ordinal(new_run);
			NEW.temporal_revision=new_stamp.temporal_revision;
			NEW.temporal_ordinal=new_stamp.temporal_ordinal;
			INSERT INTO %[1]s.event_receipt_history(run_id,revision,ordinal,operation,fact_id,after_state)
			VALUES (new_run,new_stamp.temporal_revision,new_stamp.temporal_ordinal,'insert',NEW.receipt_id::text,to_jsonb(NEW));
			RETURN NEW;
		END IF;
		IF old_run IS NOT DISTINCT FROM new_run THEN
			IF new_run IS NULL THEN
				NEW.temporal_revision=NULL; NEW.temporal_ordinal=NULL; RETURN NEW;
			END IF;
			SELECT * INTO new_stamp FROM %[1]s.swarm_next_temporal_ordinal(new_run);
			NEW.temporal_revision=new_stamp.temporal_revision;
			NEW.temporal_ordinal=new_stamp.temporal_ordinal;
			INSERT INTO %[1]s.event_receipt_history(run_id,revision,ordinal,operation,fact_id,before_state,after_state)
			VALUES (new_run,new_stamp.temporal_revision,new_stamp.temporal_ordinal,'update',NEW.receipt_id::text,to_jsonb(OLD),to_jsonb(NEW));
			RETURN NEW;
		END IF;
		IF old_run IS NOT NULL THEN
			SELECT * INTO old_stamp FROM %[1]s.swarm_next_temporal_ordinal(old_run);
			INSERT INTO %[1]s.event_receipt_history(run_id,revision,ordinal,operation,fact_id,before_state,after_state)
			VALUES (old_run,old_stamp.temporal_revision,old_stamp.temporal_ordinal,'update',OLD.receipt_id::text,to_jsonb(OLD),to_jsonb(NEW));
		END IF;
		IF new_run IS NOT NULL THEN
			SELECT * INTO new_stamp FROM %[1]s.swarm_next_temporal_ordinal(new_run);
			NEW.temporal_revision=new_stamp.temporal_revision;
			NEW.temporal_ordinal=new_stamp.temporal_ordinal;
			INSERT INTO %[1]s.event_receipt_history(run_id,revision,ordinal,operation,fact_id,before_state,after_state)
			VALUES (new_run,new_stamp.temporal_revision,new_stamp.temporal_ordinal,'update',NEW.receipt_id::text,to_jsonb(OLD),to_jsonb(NEW));
		ELSE
			NEW.temporal_revision=NULL; NEW.temporal_ordinal=NULL;
		END IF;
		RETURN NEW;
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_destroy_run(target_run UUID, deletion_actor TEXT)
	RETURNS VOID
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		model INTEGER;
		frontier BIGINT;
	BEGIN
		PERFORM %[1]s.swarm_declare_temporal_runs(ARRAY[target_run],'destructive');
		SELECT model_version,current_revision INTO STRICT model,frontier
		FROM %[1]s.run_temporal_frontiers WHERE run_id=target_run;
		INSERT INTO %[1]s.run_deletion_tombstones(run_id,source_model_version,last_revision,transaction_id,deleted_by)
		VALUES (target_run,model,CASE WHEN model=0 THEN NULL ELSE frontier END,pg_current_xact_id(),deletion_actor);
		DELETE FROM %[1]s.runs WHERE run_id=target_run;
		IF NOT FOUND THEN RAISE EXCEPTION 'run %% does not exist', target_run; END IF;
	END
	$function$;

	CREATE TRIGGER temporal_events_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.events
	FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_events();
	CREATE TRIGGER temporal_event_deliveries_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.event_deliveries
	FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_event_deliveries();
	CREATE TRIGGER temporal_event_receipts_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.event_receipts
	FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_event_receipts();
	ALTER TABLE %[1]s.events ENABLE ALWAYS TRIGGER temporal_events_guard;
	ALTER TABLE %[1]s.event_deliveries ENABLE ALWAYS TRIGGER temporal_event_deliveries_guard;
	ALTER TABLE %[1]s.event_receipts ENABLE ALWAYS TRIGGER temporal_event_receipts_guard;
`

const temporalPrototypeGrantTemplate = `
	REVOKE ALL ON SCHEMA %[1]s FROM PUBLIC;
	REVOKE ALL ON ALL TABLES IN SCHEMA %[1]s FROM PUBLIC;
	REVOKE ALL ON ALL SEQUENCES IN SCHEMA %[1]s FROM PUBLIC;
	REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %[1]s FROM PUBLIC;
	GRANT USAGE ON SCHEMA %[1]s TO %[2]s;
	GRANT SELECT ON
		%[1]s.runtime_store_metadata,
		%[1]s.runtime_store_migrations,
		%[1]s.runs,
		%[1]s.events,
		%[1]s.event_deliveries,
		%[1]s.event_receipts,
		%[1]s.run_temporal_transactions,
		%[1]s.run_temporal_frontiers,
		%[1]s.run_temporal_revisions,
		%[1]s.run_deletion_tombstones,
		%[1]s.run_lifecycle_history,
		%[1]s.event_delivery_history,
		%[1]s.event_receipt_history,
		%[1]s.timer_history,
		%[1]s.entity_state_history,
		%[1]s.agent_session_history,
		%[1]s.conversation_audit_history,
		%[1]s.reply_context_history,
		%[1]s.activity_attempt_history
	TO %[2]s;
	GRANT INSERT ON %[1]s.events TO %[2]s;
	GRANT INSERT,UPDATE,DELETE ON %[1]s.event_deliveries,%[1]s.event_receipts TO %[2]s;
	GRANT EXECUTE ON FUNCTION %[1]s.swarm_claim_temporal_runs(UUID[]) TO %[2]s;
	GRANT EXECUTE ON FUNCTION %[1]s.swarm_destroy_run(UUID,TEXT) TO %[2]s;
`
