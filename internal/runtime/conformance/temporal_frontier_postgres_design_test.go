package conformance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/lib/pq"
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
	cleanupAuthorizerRole := "tf_cleanup_authorizer_" + suffix
	upgradeSchema := "tf_upgrade_" + suffix
	freshSchema := "tf_fresh_" + suffix
	runtimePassword := "tf-runtime-" + suffix
	cleanupAuthorizerPassword := "tf-cleanup-authorizer-" + suffix

	mustExecTemporal(t, ctx, admin, `CREATE ROLE `+quoteTemporalIdent(ownerRole)+` LOGIN PASSWORD 'owner-not-used' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS`)
	mustExecTemporal(t, ctx, admin, `CREATE ROLE `+quoteTemporalIdent(runtimeRole)+` LOGIN PASSWORD `+quoteTemporalLiteral(runtimePassword)+` NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS`)
	mustExecTemporal(t, ctx, admin, `CREATE ROLE `+quoteTemporalIdent(cleanupAuthorizerRole)+` LOGIN PASSWORD `+quoteTemporalLiteral(cleanupAuthorizerPassword)+` NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS`)
	defer func() {
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+quoteTemporalIdent(freshSchema)+` CASCADE`)
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+quoteTemporalIdent(upgradeSchema)+` CASCADE`)
		_, _ = admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS `+quoteTemporalIdent(runtimeRole))
		_, _ = admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS `+quoteTemporalIdent(cleanupAuthorizerRole))
		_, _ = admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS `+quoteTemporalIdent(ownerRole))
	}()

	mustExecTemporal(t, ctx, admin, `CREATE SCHEMA `+quoteTemporalIdent(freshSchema)+` AUTHORIZATION `+quoteTemporalIdent(ownerRole))
	if err := applyTemporalPrototype(ctx, admin, freshSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, true, false); err != nil {
		t.Fatalf("fresh temporal schema apply: %v", err)
	}
	assertTemporalTargetMetadata(t, ctx, admin, freshSchema, ownerRole, runtimeRole, cleanupAuthorizerRole)
	for _, table := range []string{"run_temporal_transactions", "run_temporal_transaction_runs", "run_temporal_frontiers", "run_temporal_revisions", "runtime_store_migrations", "run_cleanup_authorizations", "run_deletion_tombstones", "event_delivery_history", "event_receipt_history"} {
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

	err = applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, false, false)
	if err == nil || !strings.Contains(err.Error(), legacyRun) {
		t.Fatalf("active legacy migration error = %v, want exact active run", err)
	}
	assertTemporalTableExists(t, ctx, admin, upgradeSchema, "run_temporal_frontiers", false)
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`UPDATE %s.runs SET status='completed' WHERE run_id=$1`, quoteTemporalIdent(upgradeSchema)), legacyRun)

	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`ALTER TABLE %s.events ADD COLUMN unregistered_drift TEXT`, quoteTemporalIdent(upgradeSchema)))
	err = applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, false, false)
	if err == nil || !strings.Contains(err.Error(), "legacy catalog checksum mismatch") {
		t.Fatalf("drifted legacy migration error = %v, want catalog checksum rejection", err)
	}
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`ALTER TABLE %s.events DROP COLUMN unregistered_drift`, quoteTemporalIdent(upgradeSchema)))

	legacyMismatchRun := "00000000-0000-0000-0000-000000000002"
	legacyMismatchDelivery := "00000000-0000-0000-0000-000000000014"
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`INSERT INTO %s.runs(run_id,status) VALUES ($1,'completed')`, quoteTemporalIdent(upgradeSchema)), legacyMismatchRun)
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`INSERT INTO %s.event_deliveries(delivery_id,run_id,event_id,status) VALUES ($1,$2,$3,'pending')`, quoteTemporalIdent(upgradeSchema)), legacyMismatchDelivery, legacyMismatchRun, legacyEvent)
	err = applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, false, false)
	if err == nil || !strings.Contains(err.Error(), legacyMismatchDelivery) || !strings.Contains(err.Error(), legacyMismatchRun) || !strings.Contains(err.Error(), legacyRun) {
		t.Fatalf("legacy lineage migration error = %v, want delivery/stored/event run diagnostic", err)
	}
	assertTemporalTableExists(t, ctx, admin, upgradeSchema, "run_temporal_frontiers", false)
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`DELETE FROM %s.event_deliveries WHERE delivery_id=$1`, quoteTemporalIdent(upgradeSchema)), legacyMismatchDelivery)
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`DELETE FROM %s.runs WHERE run_id=$1`, quoteTemporalIdent(upgradeSchema)), legacyMismatchRun)

	err = applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, false, true)
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

	if err := applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, false, false); err != nil {
		t.Fatalf("recognized temporal upgrade: %v", err)
	}
	assertTemporalTargetMetadata(t, ctx, admin, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole)
	if err := applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, false, false); err != nil {
		t.Fatalf("idempotent temporal reapply: %v", err)
	}
	var migrationCount int
	if err := admin.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.runtime_store_migrations WHERE migration_id='temporal-frontier-v1'`, quoteTemporalIdent(upgradeSchema))).Scan(&migrationCount); err != nil {
		t.Fatalf("count temporal migrations: %v", err)
	}
	if migrationCount != 1 {
		t.Fatalf("temporal migration rows = %d, want 1", migrationCount)
	}
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`ALTER TABLE %s.event_deliveries DISABLE TRIGGER temporal_event_deliveries_guard`, quoteTemporalIdent(upgradeSchema)))
	err = applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, false, false)
	if err == nil || !strings.Contains(err.Error(), "target catalog checksum mismatch") {
		t.Fatalf("drifted target reapply error = %v, want full catalog rejection", err)
	}
	mustExecTemporal(t, ctx, admin, fmt.Sprintf(`ALTER TABLE %s.event_deliveries ENABLE ALWAYS TRIGGER temporal_event_deliveries_guard`, quoteTemporalIdent(upgradeSchema)))
	if err := applyTemporalPrototype(ctx, admin, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, false, false); err != nil {
		t.Fatalf("reapply after restoring target catalog: %v", err)
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
	cleanupAuthorizerDSN := temporalRoleDSN(connection.Parameters(), cleanupAuthorizerRole, cleanupAuthorizerPassword)
	cleanupAuthorizerDB, err := sql.Open("postgres", cleanupAuthorizerDSN)
	if err != nil {
		t.Fatalf("open cleanup authorizer connection: %v", err)
	}
	defer cleanupAuthorizerDB.Close()
	if err := cleanupAuthorizerDB.PingContext(ctx); err != nil {
		t.Fatalf("ping cleanup authorizer connection: %v", err)
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

	assertTemporalReadOnlyAdmission(t, ctx, runtimeDB, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, temporalAdmissionRuntime)
	secondRuntimeDB, err := sql.Open("postgres", runtimeDSN)
	if err != nil {
		t.Fatalf("open second runtime connection: %v", err)
	}
	assertTemporalReadOnlyAdmission(t, ctx, secondRuntimeDB, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, temporalAdmissionRuntime)
	secondRuntimeDB.Close()

	assertTemporalPrivilegeDenials(t, ctx, runtimeDB, upgradeSchema, ownerRole)
	assertTemporalReadOnlyAdmission(t, ctx, cleanupAuthorizerDB, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole, temporalAdmissionCleanupAuthorizer)
	assertTemporalAdmissionRejectsCommittedDrift(t, ctx, admin, runtimeDB, cleanupAuthorizerDB, upgradeSchema, ownerRole, runtimeRole, cleanupAuthorizerRole)

	runA := "10000000-0000-0000-0000-000000000001"
	runB := "20000000-0000-0000-0000-000000000002"
	runC := "30000000-0000-0000-0000-000000000003"
	for _, runID := range []string{runA, runB, runC} {
		mustExecTemporal(t, ctx, runtimeDB, fmt.Sprintf(`SELECT %s.swarm_create_run($1,'running')`, quoteTemporalIdent(upgradeSchema)), runID)
	}
	assertTemporalCreatedRuns(t, ctx, runtimeDB, upgradeSchema, runA, runB, runC)

	eventID := "a0000000-0000-0000-0000-000000000001"
	deliveryID := "d0000000-0000-0000-0000-000000000001"
	receiptID := "e0000000-0000-0000-0000-000000000001"
	assertTemporalUndeclaredDMLRejected(t, ctx, runtimeDB, upgradeSchema, runA, eventID)
	writeTemporalEventDeliveryReceipt(t, ctx, runtimeDB, upgradeSchema, runA, eventID, deliveryID, receiptID)
	assertTemporalSharedRevision(t, ctx, runtimeDB, upgradeSchema, eventID, deliveryID, receiptID)
	assertTemporalDirectEventMutationRejected(t, ctx, runtimeDB, upgradeSchema, eventID)
	assertTemporalAllGuardedFamilies(t, ctx, runtimeDB, upgradeSchema, runA, eventID)
	assertTemporalAppendFamilyImmutability(t, ctx, admin, upgradeSchema, ownerRole, runA)
	assertTemporalUndeclaredDeliveryMutationRejected(t, ctx, runtimeDB, upgradeSchema, deliveryID)
	assertTemporalEventDerivedMutableOperations(t, ctx, runtimeDB, upgradeSchema, runA, deliveryID, receiptID)
	assertTemporalDeliveryLineageMismatchRejected(t, ctx, runtimeDB, upgradeSchema, deliveryID, runA, runB)
	assertTemporalDeclaredDeliveryDelete(t, ctx, runtimeDB, upgradeSchema, deliveryID, runA)
	assertTemporalRollbackPublishesNothing(t, ctx, runtimeDB, upgradeSchema, runC)
	assertTemporalRunlessLineage(t, ctx, runtimeDB, upgradeSchema)
	assertTemporalAuthorizedMixedCleanup(t, ctx, cleanupAuthorizerDB, runtimeDB, upgradeSchema, runA, runC)
	assertTemporalUnversionedDestruction(t, ctx, cleanupAuthorizerDB, runtimeDB, upgradeSchema, legacyRun, legacyEvent, legacyDelivery, legacyReceipt)
	assertTemporalEveryFamilyDestructiveCascade(t, ctx, cleanupAuthorizerDB, runtimeDB, upgradeSchema)
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

func applyTemporalPrototype(ctx context.Context, db *sql.DB, schema, owner, runtimeRole, cleanupAuthorizerRole string, fresh, forceRollback bool) error {
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
		  AND column_name IN ('schema_generation','schema_ddl_sha256','schema_catalog_sha256','schema_owner_role','runtime_role','cleanup_authorizer_role')
	`, schema).Scan(&targetColumns); err != nil {
		return fmt.Errorf("inspect temporal metadata generation: %w", err)
	}
	checksum := temporalPrototypeChecksum()
	if targetColumns == 6 {
		var generation, storedChecksum, storedCatalogChecksum, storedOwner, storedRuntime, storedCleanupAuthorizer string
		if err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT schema_generation,schema_ddl_sha256,schema_catalog_sha256,schema_owner_role,runtime_role,cleanup_authorizer_role FROM %s.runtime_store_metadata WHERE id=1`, qSchema)).Scan(&generation, &storedChecksum, &storedCatalogChecksum, &storedOwner, &storedRuntime, &storedCleanupAuthorizer); err != nil {
			return fmt.Errorf("read temporal target metadata: %w", err)
		}
		if generation != "temporal-frontier-v1" || storedChecksum != checksum || storedOwner != owner || storedRuntime != runtimeRole || storedCleanupAuthorizer != cleanupAuthorizerRole {
			return fmt.Errorf("temporal target metadata drift")
		}
		var ledgerCount int
		if err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.runtime_store_migrations WHERE migration_id='temporal-frontier-v1' AND ddl_sha256=$1 AND catalog_sha256=$2 AND cleanup_authorizer_role=$3`, qSchema), checksum, storedCatalogChecksum, cleanupAuthorizerRole).Scan(&ledgerCount); err != nil || ledgerCount != 1 {
			return fmt.Errorf("temporal migration ledger drift: count=%d err=%v", ledgerCount, err)
		}
		actualCatalogChecksum, err := temporalCatalogChecksum(ctx, tx, schema, owner, runtimeRole)
		if err != nil {
			return fmt.Errorf("revalidate temporal target catalog: %w", err)
		}
		if actualCatalogChecksum != storedCatalogChecksum {
			return fmt.Errorf("target catalog checksum mismatch: got %s want %s", actualCatalogChecksum, storedCatalogChecksum)
		}
		return tx.Commit()
	}
	if targetColumns != 0 {
		return fmt.Errorf("partial temporal metadata shape is unsupported")
	}

	if _, err := tx.ExecContext(ctx, `SET LOCAL ROLE `+quoteTemporalIdent(owner)); err != nil {
		return fmt.Errorf("assume temporal schema owner: %w", err)
	}
	for _, table := range []string{"activity_attempts", "agent_conversation_audits", "agent_sessions", "agent_turns", "dead_letters", "entity_mutations", "entity_state", "event_deliveries", "event_receipts", "events", "reply_contexts", "runs", "runtime_store_metadata", "selected_fork_lineage", "timers"} {
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
	legacyCatalogChecksum, err := temporalCatalogChecksum(ctx, tx, schema, owner, runtimeRole)
	if err != nil {
		return fmt.Errorf("inspect legacy catalog checksum: %w", err)
	}
	if legacyCatalogChecksum != temporalRegisteredLegacyCatalogChecksum {
		return fmt.Errorf("legacy catalog checksum mismatch: got %s want %s", legacyCatalogChecksum, temporalRegisteredLegacyCatalogChecksum)
	}
	rows, err = tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT delivery.delivery_id::text,delivery.run_id::text,event.run_id::text
		FROM %s.event_deliveries delivery
		LEFT JOIN %s.events event ON event.event_id=delivery.event_id
		WHERE event.event_id IS NULL OR delivery.run_id IS DISTINCT FROM event.run_id
		ORDER BY delivery.delivery_id::text
	`, qSchema, qSchema))
	if err != nil {
		return fmt.Errorf("inspect legacy delivery lineage: %w", err)
	}
	var lineageMismatches []string
	for rows.Next() {
		var deliveryID, storedRun string
		var eventRun sql.NullString
		if err := rows.Scan(&deliveryID, &storedRun, &eventRun); err != nil {
			rows.Close()
			return fmt.Errorf("scan legacy delivery lineage: %w", err)
		}
		resolvedEventRun := "<missing-event>"
		if eventRun.Valid {
			resolvedEventRun = eventRun.String
		}
		lineageMismatches = append(lineageMismatches, fmt.Sprintf("delivery=%s stored_run=%s event_run=%s", deliveryID, storedRun, resolvedEventRun))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate legacy delivery lineage: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close legacy delivery lineage: %w", err)
	}
	if len(lineageMismatches) > 0 {
		return fmt.Errorf("legacy delivery/event lineage mismatch: %s", strings.Join(lineageMismatches, ";"))
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
	if _, err := tx.ExecContext(ctx, temporalGrantDDL(qSchema, quoteTemporalIdent(runtimeRole), quoteTemporalIdent(cleanupAuthorizerRole))); err != nil {
		return fmt.Errorf("apply temporal grants: %w", err)
	}
	if forceRollback {
		return fmt.Errorf("forced temporal migration rollback")
	}
	targetCatalogChecksum, err := temporalCatalogChecksum(ctx, tx, schema, owner, runtimeRole)
	if err != nil {
		return fmt.Errorf("compute installed temporal catalog checksum: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s.runtime_store_migrations(migration_id,from_generation,to_generation,ddl_sha256,catalog_sha256,schema_owner_role,runtime_role,cleanup_authorizer_role)
		VALUES ('temporal-frontier-v1','pre-temporal-v0','temporal-frontier-v1',$1,$2,$3,$4,$5)
	`, qSchema), checksum, targetCatalogChecksum, owner, runtimeRole, cleanupAuthorizerRole); err != nil {
		return fmt.Errorf("write temporal migration ledger: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s.runtime_store_metadata
		SET platform_version='0.7.0', schema_generation='temporal-frontier-v1', schema_ddl_sha256=$1,
		    schema_catalog_sha256=$2, schema_owner_role=$3, runtime_role=$4, cleanup_authorizer_role=$5
		WHERE id=1
	`, qSchema), checksum, targetCatalogChecksum, owner, runtimeRole, cleanupAuthorizerRole); err != nil {
		return fmt.Errorf("commit temporal metadata authority: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit temporal schema apply: %w", err)
	}
	return nil
}

func assertTemporalTargetMetadata(t *testing.T, ctx context.Context, db *sql.DB, schema, owner, runtimeRole, cleanupAuthorizerRole string) {
	t.Helper()
	var generation, checksum, catalogChecksum, gotOwner, gotRuntime, gotCleanupAuthorizer string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT schema_generation,schema_ddl_sha256,schema_catalog_sha256,schema_owner_role,runtime_role,cleanup_authorizer_role FROM %s.runtime_store_metadata WHERE id=1`, quoteTemporalIdent(schema))).Scan(&generation, &checksum, &catalogChecksum, &gotOwner, &gotRuntime, &gotCleanupAuthorizer); err != nil {
		t.Fatalf("read temporal target metadata: %v", err)
	}
	if generation != "temporal-frontier-v1" || checksum != temporalPrototypeChecksum() || len(catalogChecksum) != 64 || gotOwner != owner || gotRuntime != runtimeRole || gotCleanupAuthorizer != cleanupAuthorizerRole {
		t.Fatalf("temporal metadata = generation:%q checksum:%q catalog:%q owner:%q runtime:%q cleanup_authorizer:%q", generation, checksum, catalogChecksum, gotOwner, gotRuntime, gotCleanupAuthorizer)
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

type temporalAdmissionIdentity string

const (
	temporalAdmissionRuntime           temporalAdmissionIdentity = "runtime"
	temporalAdmissionCleanupAuthorizer temporalAdmissionIdentity = "cleanup_authorizer"
)

func assertTemporalReadOnlyAdmission(t *testing.T, ctx context.Context, db *sql.DB, schema, owner, runtimeRole, cleanupAuthorizerRole string, identity temporalAdmissionIdentity) {
	t.Helper()
	if err := temporalReadOnlyAdmissionError(ctx, db, schema, owner, runtimeRole, cleanupAuthorizerRole, identity); err != nil {
		t.Fatalf("%s temporal admission rejected: %v", identity, err)
	}
}

func temporalReadOnlyAdmissionError(ctx context.Context, db *sql.DB, schema, owner, runtimeRole, cleanupAuthorizerRole string, identity temporalAdmissionIdentity) error {
	expectedCurrentUser := runtimeRole
	if identity == temporalAdmissionCleanupAuthorizer {
		expectedCurrentUser = cleanupAuthorizerRole
	} else if identity != temporalAdmissionRuntime {
		return fmt.Errorf("unknown temporal admission identity %q", identity)
	}
	if owner == runtimeRole || owner == cleanupAuthorizerRole || runtimeRole == cleanupAuthorizerRole {
		return fmt.Errorf("temporal database identities are not distinct")
	}

	qSchema := quoteTemporalIdent(schema)
	var currentUser, platformVersion, generation, ddlChecksum, catalogChecksum, metadataOwner, metadataRuntime, metadataCleanupAuthorizer string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT current_user,platform_version,schema_generation,schema_ddl_sha256,schema_catalog_sha256,
		       schema_owner_role,runtime_role,cleanup_authorizer_role
		FROM %s.runtime_store_metadata WHERE id=1
	`, qSchema)).Scan(&currentUser, &platformVersion, &generation, &ddlChecksum, &catalogChecksum, &metadataOwner, &metadataRuntime, &metadataCleanupAuthorizer); err != nil {
		return fmt.Errorf("read temporal metadata: %w", err)
	}
	if currentUser != expectedCurrentUser || platformVersion != "0.7.0" || generation != "temporal-frontier-v1" || ddlChecksum != temporalPrototypeChecksum() || metadataOwner != owner || metadataRuntime != runtimeRole || metadataCleanupAuthorizer != cleanupAuthorizerRole {
		return fmt.Errorf("metadata/identity drift: current=%q platform=%q generation=%q ddl=%q owner=%q runtime=%q cleanup=%q", currentUser, platformVersion, generation, ddlChecksum, metadataOwner, metadataRuntime, metadataCleanupAuthorizer)
	}
	actualCatalogChecksum, err := temporalCatalogChecksum(ctx, db, schema, owner, runtimeRole)
	if err != nil {
		return fmt.Errorf("recompute installed catalog: %w", err)
	}
	if actualCatalogChecksum != catalogChecksum {
		return fmt.Errorf("installed catalog checksum drift: got %s want %s", actualCatalogChecksum, catalogChecksum)
	}

	var ledgerCount int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT count(*) FROM %s.runtime_store_migrations
		WHERE migration_id='temporal-frontier-v1'
		  AND from_generation='pre-temporal-v0'
		  AND to_generation='temporal-frontier-v1'
		  AND ddl_sha256=$1 AND catalog_sha256=$2
		  AND schema_owner_role=$3 AND runtime_role=$4 AND cleanup_authorizer_role=$5
	`, qSchema), ddlChecksum, catalogChecksum, owner, runtimeRole, cleanupAuthorizerRole).Scan(&ledgerCount); err != nil {
		return fmt.Errorf("read temporal migration ledger: %w", err)
	}
	if ledgerCount != 1 {
		return fmt.Errorf("temporal migration ledger exact rows = %d, want 1", ledgerCount)
	}

	var schemaOwner string
	if err := db.QueryRowContext(ctx, `
		SELECT role.rolname FROM pg_namespace namespace
		JOIN pg_roles role ON role.oid=namespace.nspowner
		WHERE namespace.nspname=$1
	`, schema).Scan(&schemaOwner); err != nil {
		return fmt.Errorf("read temporal schema owner: %w", err)
	}
	if schemaOwner != owner {
		return fmt.Errorf("temporal schema owner = %q, want %q", schemaOwner, owner)
	}
	for _, roleName := range []string{owner, runtimeRole, cleanupAuthorizerRole} {
		var superuser, inherit, createRole, createDB, canLogin, replication, bypassRLS bool
		var connectionLimit int
		if err := db.QueryRowContext(ctx, `
			SELECT rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls,rolconnlimit
			FROM pg_roles WHERE rolname=$1
		`, roleName).Scan(&superuser, &inherit, &createRole, &createDB, &canLogin, &replication, &bypassRLS, &connectionLimit); err != nil {
			return fmt.Errorf("read temporal role %s: %w", roleName, err)
		}
		if superuser || inherit || createRole || createDB || !canLogin || replication || bypassRLS || connectionLimit != -1 {
			return fmt.Errorf("temporal role %s attributes drifted", roleName)
		}
	}
	var membershipCount int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_auth_members membership
		JOIN pg_roles granted_role ON granted_role.oid=membership.roleid
		JOIN pg_roles member_role ON member_role.oid=membership.member
		WHERE granted_role.rolname=ANY($1::text[]) OR member_role.rolname=ANY($1::text[])
	`, pq.Array([]string{owner, runtimeRole, cleanupAuthorizerRole})).Scan(&membershipCount); err != nil {
		return fmt.Errorf("read temporal role memberships: %w", err)
	}
	if membershipCount != 0 {
		return fmt.Errorf("temporal role membership rows = %d, want 0", membershipCount)
	}

	var schemaUsage, schemaCreate bool
	if err := db.QueryRowContext(ctx, `SELECT has_schema_privilege(current_user,$1,'USAGE'),has_schema_privilege(current_user,$1,'CREATE')`, schema).Scan(&schemaUsage, &schemaCreate); err != nil {
		return fmt.Errorf("read temporal schema grants: %w", err)
	}
	if !schemaUsage || schemaCreate {
		return fmt.Errorf("%s schema grants drifted: usage=%v create=%v", identity, schemaUsage, schemaCreate)
	}

	actualTableGrants, err := temporalAdmissionGrantRows(ctx, db, `
		SELECT table_name,privilege_type
		FROM information_schema.role_table_grants
		WHERE table_schema=$1 AND grantee=current_user
		ORDER BY table_name,privilege_type
	`, schema)
	if err != nil {
		return fmt.Errorf("read temporal table grants: %w", err)
	}
	if err := requireTemporalExactRows("table grants", actualTableGrants, temporalExpectedTableGrants(identity)); err != nil {
		return err
	}

	actualFunctionGrants, err := temporalAdmissionGrantRows(ctx, db, `
		SELECT routine_name,privilege_type
		FROM information_schema.role_routine_grants
		WHERE specific_schema=$1 AND grantee=current_user
		ORDER BY routine_name,privilege_type
	`, schema)
	if err != nil {
		return fmt.Errorf("read temporal function grants: %w", err)
	}
	expectedFunctionGrants := []string{"swarm_create_run|EXECUTE", "swarm_claim_temporal_runs|EXECUTE", "swarm_claim_authorized_run_cleanup|EXECUTE", "swarm_delete_authorized_runs|EXECUTE"}
	if identity == temporalAdmissionCleanupAuthorizer {
		expectedFunctionGrants = []string{"swarm_authorize_run_cleanup|EXECUTE"}
	}
	if err := requireTemporalExactRows("function grants", actualFunctionGrants, expectedFunctionGrants); err != nil {
		return err
	}

	functionRows, err := db.QueryContext(ctx, `
		SELECT function.proname,oidvectortypes(function.proargtypes),function.prosecdef,owner_role.rolname,
		       COALESCE(array_to_string(function.proconfig,','),'')
		FROM pg_proc function
		JOIN pg_namespace namespace ON namespace.oid=function.pronamespace
		JOIN pg_roles owner_role ON owner_role.oid=function.proowner
		WHERE namespace.nspname=$1
		ORDER BY function.proname,oidvectortypes(function.proargtypes)
	`, schema)
	if err != nil {
		return fmt.Errorf("read temporal function mapping: %w", err)
	}
	var actualFunctions []string
	for functionRows.Next() {
		var name, arguments, functionOwner, config string
		var securityDefiner bool
		if err := functionRows.Scan(&name, &arguments, &securityDefiner, &functionOwner, &config); err != nil {
			functionRows.Close()
			return fmt.Errorf("scan temporal function mapping: %w", err)
		}
		actualFunctions = append(actualFunctions, fmt.Sprintf("%s|%s|%t|%s|%s", name, arguments, securityDefiner, functionOwner, config))
	}
	if err := functionRows.Err(); err != nil {
		functionRows.Close()
		return fmt.Errorf("iterate temporal function mapping: %w", err)
	}
	if err := functionRows.Close(); err != nil {
		return fmt.Errorf("close temporal function mapping: %w", err)
	}
	if err := requireTemporalExactRows("function mapping", actualFunctions, temporalExpectedFunctions(owner, schema)); err != nil {
		return err
	}

	triggerRows, err := db.QueryContext(ctx, `
		SELECT relation.relname,trigger.tgname,function.proname,trigger.tgenabled::text
		FROM pg_trigger trigger
		JOIN pg_class relation ON relation.oid=trigger.tgrelid
		JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
		JOIN pg_proc function ON function.oid=trigger.tgfoid
		WHERE namespace.nspname=$1 AND NOT trigger.tgisinternal
		ORDER BY relation.relname,trigger.tgname
	`, schema)
	if err != nil {
		return fmt.Errorf("read temporal trigger mapping: %w", err)
	}
	var actualTriggers []string
	for triggerRows.Next() {
		var table, trigger, function, enabled string
		if err := triggerRows.Scan(&table, &trigger, &function, &enabled); err != nil {
			triggerRows.Close()
			return fmt.Errorf("scan temporal trigger mapping: %w", err)
		}
		actualTriggers = append(actualTriggers, table+"|"+trigger+"|"+function+"|"+enabled)
	}
	if err := triggerRows.Err(); err != nil {
		triggerRows.Close()
		return fmt.Errorf("iterate temporal trigger mapping: %w", err)
	}
	if err := triggerRows.Close(); err != nil {
		return fmt.Errorf("close temporal trigger mapping: %w", err)
	}
	return requireTemporalExactRows("trigger mapping", actualTriggers, temporalExpectedTriggers())
}

func temporalAdmissionGrantRows(ctx context.Context, db *sql.DB, query, schema string) ([]string, error) {
	rows, err := db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []string
	for rows.Next() {
		var object, privilege string
		if err := rows.Scan(&object, &privilege); err != nil {
			return nil, err
		}
		grants = append(grants, object+"|"+privilege)
	}
	return grants, rows.Err()
}

func temporalExpectedTableGrants(identity temporalAdmissionIdentity) []string {
	readTables := []string{"runtime_store_metadata", "runtime_store_migrations"}
	if identity == temporalAdmissionCleanupAuthorizer {
		return []string{"runtime_store_metadata|SELECT", "runtime_store_migrations|SELECT"}
	}
	readTables = append(readTables,
		"runs", "events", "event_deliveries", "event_receipts", "dead_letters", "entity_mutations", "entity_state", "timers",
		"agent_sessions", "agent_turns", "agent_conversation_audits", "reply_contexts", "activity_attempts", "selected_fork_lineage",
		"run_temporal_transactions", "run_temporal_transaction_runs", "run_temporal_frontiers", "run_temporal_revisions", "run_deletion_tombstones",
		"run_lifecycle_history", "event_delivery_history", "event_receipt_history", "timer_history", "entity_state_history", "agent_session_history",
		"conversation_audit_history", "reply_context_history", "activity_attempt_history",
	)
	var grants []string
	for _, table := range readTables {
		grants = append(grants, table+"|SELECT")
	}
	for _, table := range []string{"events", "dead_letters", "entity_mutations", "agent_turns", "selected_fork_lineage"} {
		grants = append(grants, table+"|INSERT")
	}
	grants = append(grants, "runs|UPDATE")
	for _, table := range []string{"event_deliveries", "event_receipts", "entity_state", "timers", "agent_sessions", "agent_conversation_audits", "reply_contexts", "activity_attempts"} {
		for _, privilege := range []string{"INSERT", "UPDATE", "DELETE"} {
			grants = append(grants, table+"|"+privilege)
		}
	}
	return grants
}

func temporalExpectedFunctions(owner, schema string) []string {
	config := "search_path=pg_catalog, " + schema
	rows := []struct {
		name      string
		arguments string
	}{
		{name: "swarm_declare_temporal_runs", arguments: "uuid[], uuid[]"},
		{name: "swarm_create_run", arguments: "uuid, text"},
		{name: "swarm_claim_temporal_runs", arguments: "uuid[]"},
		{name: "swarm_authorize_run_cleanup", arguments: "uuid, text, text, uuid[], uuid[]"},
		{name: "swarm_claim_authorized_run_cleanup", arguments: "uuid"},
		{name: "swarm_delete_authorized_runs", arguments: "uuid"},
		{name: "swarm_next_temporal_ordinal", arguments: "uuid"},
		{name: "swarm_resolve_temporal_run", arguments: "jsonb, text, text"},
		{name: "swarm_guard_append_fact", arguments: ""},
		{name: "swarm_guard_mutable_fact", arguments: ""},
	}
	result := make([]string, 0, len(rows))
	for _, row := range rows {
		result = append(result, fmt.Sprintf("%s|%s|true|%s|%s", row.name, row.arguments, owner, config))
	}
	return result
}

func temporalExpectedTriggers() []string {
	return []string{
		"runs|temporal_runs_guard|swarm_guard_mutable_fact|A",
		"events|temporal_events_guard|swarm_guard_append_fact|A",
		"event_deliveries|temporal_event_deliveries_guard|swarm_guard_mutable_fact|A",
		"event_receipts|temporal_event_receipts_guard|swarm_guard_mutable_fact|A",
		"dead_letters|temporal_dead_letters_guard|swarm_guard_append_fact|A",
		"entity_mutations|temporal_entity_mutations_guard|swarm_guard_append_fact|A",
		"entity_state|temporal_entity_state_guard|swarm_guard_mutable_fact|A",
		"timers|temporal_timers_guard|swarm_guard_mutable_fact|A",
		"agent_sessions|temporal_agent_sessions_guard|swarm_guard_mutable_fact|A",
		"agent_turns|temporal_agent_turns_guard|swarm_guard_append_fact|A",
		"agent_conversation_audits|temporal_agent_conversation_audits_guard|swarm_guard_mutable_fact|A",
		"reply_contexts|temporal_reply_contexts_guard|swarm_guard_mutable_fact|A",
		"activity_attempts|temporal_activity_attempts_guard|swarm_guard_mutable_fact|A",
		"selected_fork_lineage|temporal_selected_fork_lineage_guard|swarm_guard_append_fact|A",
	}
}

func requireTemporalExactRows(label string, got, want []string) error {
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		return fmt.Errorf("temporal %s drift: got %v want %v", label, got, want)
	}
	return nil
}

func assertTemporalAdmissionRejectsCommittedDrift(t *testing.T, ctx context.Context, admin, runtimeDB, cleanupAuthorizerDB *sql.DB, schema, owner, runtimeRole, cleanupAuthorizerRole string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	mustExecTemporal(t, ctx, admin, `GRANT UPDATE ON `+qSchema+`.events TO `+quoteTemporalIdent(runtimeRole))
	for _, probe := range []struct {
		name     string
		db       *sql.DB
		identity temporalAdmissionIdentity
	}{
		{name: "runtime", db: runtimeDB, identity: temporalAdmissionRuntime},
		{name: "cleanup authorizer", db: cleanupAuthorizerDB, identity: temporalAdmissionCleanupAuthorizer},
	} {
		if err := temporalReadOnlyAdmissionError(ctx, probe.db, schema, owner, runtimeRole, cleanupAuthorizerRole, probe.identity); err == nil || !strings.Contains(err.Error(), "catalog checksum drift") {
			t.Fatalf("%s boot admission after committed grant drift = %v, want catalog rejection", probe.name, err)
		}
	}
	mustExecTemporal(t, ctx, admin, `REVOKE UPDATE ON `+qSchema+`.events FROM `+quoteTemporalIdent(runtimeRole))
	assertTemporalReadOnlyAdmission(t, ctx, runtimeDB, schema, owner, runtimeRole, cleanupAuthorizerRole, temporalAdmissionRuntime)
	assertTemporalReadOnlyAdmission(t, ctx, cleanupAuthorizerDB, schema, owner, runtimeRole, cleanupAuthorizerRole, temporalAdmissionCleanupAuthorizer)
}

func assertTemporalPrivilegeDenials(t *testing.T, ctx context.Context, db *sql.DB, schema, owner string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	assertTemporalExecFails(t, ctx, db, `SET ROLE `+quoteTemporalIdent(owner), "assume schema owner")
	assertTemporalExecFails(t, ctx, db, `ALTER TABLE `+qSchema+`.events DISABLE TRIGGER ALL`, "disable temporal trigger")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`INSERT INTO %s.event_delivery_history(run_id,revision,ordinal,operation,fact_id) VALUES ('00000000-0000-0000-0000-000000000000',1,1,'insert','forged')`, qSchema), "write temporal history")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`INSERT INTO %s.run_temporal_transactions(transaction_id) VALUES (pg_current_xact_id())`, qSchema), "write temporal transaction authority")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`INSERT INTO %s.run_temporal_transaction_runs(transaction_id,run_id,disposition) VALUES (pg_current_xact_id(),'00000000-0000-0000-0000-000000000000','normal')`, qSchema), "write temporal transaction run authority")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`UPDATE %s.run_temporal_frontiers SET current_revision=current_revision+1`, qSchema), "write temporal frontier authority")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`DELETE FROM %s.run_temporal_revisions`, qSchema), "write temporal revision authority")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`DELETE FROM %s.runtime_store_migrations`, qSchema), "write temporal migration ledger")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`UPDATE %s.runtime_store_metadata SET schema_generation='forged'`, qSchema), "write temporal metadata")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`DELETE FROM %s.run_deletion_tombstones`, qSchema), "write temporal deletion evidence")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`INSERT INTO %s.run_cleanup_authorizations(authorization_id,operation_id,actor_evidence,destructive_run_ids) VALUES (gen_random_uuid(),'forged','forged',ARRAY['00000000-0000-0000-0000-000000000000'::uuid])`, qSchema), "write cleanup authorization")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`INSERT INTO %s.runs(run_id,status) VALUES ('00000000-0000-0000-0000-000000000000','running')`, qSchema), "insert run outside creation owner")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`SELECT * FROM %s.swarm_next_temporal_ordinal('00000000-0000-0000-0000-000000000000'::uuid)`, qSchema), "invoke ordinal bypass")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`SELECT %s.swarm_declare_temporal_runs('{}'::uuid[],ARRAY['00000000-0000-0000-0000-000000000000'::uuid])`, qSchema), "invoke destructive declaration bypass")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`SELECT %s.swarm_authorize_run_cleanup(gen_random_uuid(),'forged','forged','{}'::uuid[],ARRAY['00000000-0000-0000-0000-000000000000'::uuid])`, qSchema), "mint cleanup authorization from runtime")
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`SELECT %s.swarm_claim_authorized_run_cleanup('00000000-0000-0000-0000-000000000000')`, qSchema), "claim unknown cleanup authorization")
}

func assertTemporalCreatedRuns(t *testing.T, ctx context.Context, db *sql.DB, schema string, runIDs ...string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	for _, runID := range runIDs {
		var model int
		var revision int64
		var historyCount int
		if err := db.QueryRowContext(ctx, fmt.Sprintf(`
			SELECT f.model_version,f.current_revision,
			       (SELECT count(*) FROM %[1]s.run_lifecycle_history h WHERE h.run_id=f.run_id AND h.operation='insert')
			FROM %[1]s.run_temporal_frontiers f WHERE f.run_id=$1
		`, qSchema), runID).Scan(&model, &revision, &historyCount); err != nil {
			t.Fatalf("read restricted-runtime-created run %s: %v", runID, err)
		}
		if model != 1 || revision != 1 || historyCount != 1 {
			t.Fatalf("created run %s = model:%d revision:%d history:%d, want 1/1/1", runID, model, revision, historyCount)
		}
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

func assertTemporalAppendFamilyImmutability(t *testing.T, ctx context.Context, db *sql.DB, schema, owner, runID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	appendFamilies := []struct {
		name      string
		table     string
		idColumn  string
		id        string
		updateSQL string
	}{
		{name: "events", table: "events", idColumn: "event_id", id: "a0000000-0000-0000-0000-000000000001", updateSQL: "created_at=clock_timestamp()"},
		{name: "entity_mutations", table: "entity_mutations", idColumn: "mutation_id", id: "41000000-0000-0000-0000-000000000001", updateSQL: "value='forged'"},
		{name: "dead_letters", table: "dead_letters", idColumn: "dead_letter_id", id: "42000000-0000-0000-0000-000000000001", updateSQL: "value='forged'"},
		{name: "agent_turns", table: "agent_turns", idColumn: "turn_id", id: "43000000-0000-0000-0000-000000000001", updateSQL: "value='forged'"},
		{name: "selected_fork_lineage", table: "selected_fork_lineage", idColumn: "lineage_id", id: "44000000-0000-0000-0000-000000000001", updateSQL: "value='forged'"},
	}
	for _, family := range appendFamilies {
		for _, mutation := range []struct {
			name string
			sql  string
		}{
			{name: "update", sql: fmt.Sprintf(`UPDATE %s.%s SET %s WHERE %s=$1`, qSchema, quoteTemporalIdent(family.table), family.updateSQL, quoteTemporalIdent(family.idColumn))},
			{name: "delete", sql: fmt.Sprintf(`DELETE FROM %s.%s WHERE %s=$1`, qSchema, quoteTemporalIdent(family.table), quoteTemporalIdent(family.idColumn))},
		} {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin owner %s %s proof: %v", family.name, mutation.name, err)
			}
			mustExecTemporal(t, ctx, tx, `SET LOCAL ROLE `+quoteTemporalIdent(owner))
			mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runID)
			if _, err := tx.ExecContext(ctx, mutation.sql, family.id); err == nil {
				_ = tx.Rollback()
				t.Fatalf("owner %s %s bypassed ENABLE ALWAYS append guard", family.name, mutation.name)
			}
			_ = tx.Rollback()
		}
	}
}

func assertTemporalUndeclaredDeliveryMutationRejected(t *testing.T, ctx context.Context, db *sql.DB, schema, deliveryID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`UPDATE %s.event_deliveries SET status='delivered' WHERE delivery_id=$1`, qSchema), "undeclared delivery update", deliveryID)
	assertTemporalExecFails(t, ctx, db, fmt.Sprintf(`DELETE FROM %s.event_deliveries WHERE delivery_id=$1`, qSchema), "undeclared delivery delete", deliveryID)
}

func assertTemporalEventDerivedMutableOperations(t *testing.T, ctx context.Context, db *sql.DB, schema, runID, deliveryID, receiptID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin delivery/receipt operation matrix: %v", err)
	}
	defer tx.Rollback()
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`UPDATE %s.event_deliveries SET status='delivered' WHERE delivery_id=$1`, qSchema), deliveryID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`UPDATE %s.event_receipts SET outcome='retry' WHERE receipt_id=$1`, qSchema), receiptID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`DELETE FROM %s.event_receipts WHERE receipt_id=$1`, qSchema), receiptID)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit delivery/receipt operation matrix: %v", err)
	}
	var deliveryUpdates, receiptUpdates, receiptDeletes, receiptRows int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT (SELECT count(*) FROM %[1]s.event_delivery_history WHERE fact_id=$1::text AND operation='update'),
		       (SELECT count(*) FROM %[1]s.event_receipt_history WHERE fact_id=$2::text AND operation='update'),
		       (SELECT count(*) FROM %[1]s.event_receipt_history WHERE fact_id=$2::text AND operation='delete'),
		       (SELECT count(*) FROM %[1]s.event_receipts WHERE receipt_id=$2::uuid)
	`, qSchema), deliveryID, receiptID).Scan(&deliveryUpdates, &receiptUpdates, &receiptDeletes, &receiptRows); err != nil {
		t.Fatalf("read delivery/receipt operation matrix: %v", err)
	}
	if deliveryUpdates != 1 || receiptUpdates != 1 || receiptDeletes != 1 || receiptRows != 0 {
		t.Fatalf("delivery/receipt operations = delivery_update:%d receipt_update:%d receipt_delete:%d receipt_rows:%d", deliveryUpdates, receiptUpdates, receiptDeletes, receiptRows)
	}
}

func assertTemporalDeliveryLineageMismatchRejected(t *testing.T, ctx context.Context, db *sql.DB, schema, deliveryID, runA, runB string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	eventB := "b0000000-0000-0000-0000-000000000002"
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin second-run event insert: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runB)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.events(event_id,run_id) VALUES ($1,$2)`, qSchema), eventB, runB)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit second-run event insert: %v", err)
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin delivery lineage mismatch insert: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runA)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s.event_deliveries(delivery_id,run_id,event_id,status) VALUES ('d0000000-0000-0000-0000-000000000099',$1,$2,'pending')`, qSchema), runA, eventB); err == nil {
		t.Fatal("delivery insert with mismatched event lineage unexpectedly succeeded")
	}
	_ = tx.Rollback()

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin delivery lineage mismatch update: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid,$2::uuid])`, qSchema), runA, runB)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s.event_deliveries SET run_id=$1 WHERE delivery_id=$2`, qSchema), runB, deliveryID); err == nil {
		t.Fatal("delivery moved away from authoritative event lineage")
	}
	_ = tx.Rollback()

	entityID := "b0000000-0000-0000-0000-000000000003"
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin direct mutable fixture: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runA)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.entity_state(entity_id,run_id,value) VALUES ($1,$2,'owned-a')`, qSchema), entityID, runA)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit direct mutable fixture: %v", err)
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin incomplete direct ownership move: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runB)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s.entity_state SET run_id=$1 WHERE entity_id=$2`, qSchema), runB, entityID); err == nil {
		t.Fatal("direct ownership move with only NEW run unexpectedly succeeded")
	}
	_ = tx.Rollback()

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin complete direct ownership move: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid,$2::uuid])`, qSchema), runB, runA)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`UPDATE %s.entity_state SET run_id=$1,value='owned-b' WHERE entity_id=$2`, qSchema), runB, entityID)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit complete direct ownership move: %v", err)
	}
	var movedRun string
	var oldRunHistory, newRunHistory int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT e.run_id::text,
		       (SELECT count(*) FROM %[1]s.entity_state_history WHERE fact_id=$1 AND run_id=$2::uuid),
		       (SELECT count(*) FROM %[1]s.entity_state_history WHERE fact_id=$1 AND run_id=$3::uuid)
		FROM %[1]s.entity_state e WHERE entity_id=$1::uuid
	`, qSchema), entityID, runA, runB).Scan(&movedRun, &oldRunHistory, &newRunHistory); err != nil {
		t.Fatalf("read complete direct ownership move: %v", err)
	}
	if movedRun != runB || oldRunHistory == 0 || newRunHistory == 0 {
		t.Fatalf("direct ownership move = run:%s old_history:%d new_history:%d", movedRun, oldRunHistory, newRunHistory)
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

func assertTemporalAllGuardedFamilies(t *testing.T, ctx context.Context, db *sql.DB, schema, runID, eventID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin all-family restricted runtime proof: %v", err)
	}
	defer tx.Rollback()
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runID)

	appendStatements := []struct {
		query string
		arg   string
	}{
		{query: fmt.Sprintf(`INSERT INTO %s.entity_mutations(mutation_id,run_id,value) VALUES ('41000000-0000-0000-0000-000000000001',$1,'inserted')`, qSchema), arg: runID},
		{query: fmt.Sprintf(`INSERT INTO %s.dead_letters(dead_letter_id,original_event_id,value) VALUES ('42000000-0000-0000-0000-000000000001',$1,'inserted')`, qSchema), arg: eventID},
		{query: fmt.Sprintf(`INSERT INTO %s.agent_turns(turn_id,run_id,value) VALUES ('43000000-0000-0000-0000-000000000001',$1,'inserted')`, qSchema), arg: runID},
		{query: fmt.Sprintf(`INSERT INTO %s.selected_fork_lineage(lineage_id,run_id,value) VALUES ('44000000-0000-0000-0000-000000000001',$1,'inserted')`, qSchema), arg: runID},
	}
	for _, stmt := range appendStatements {
		mustExecTemporal(t, ctx, tx, stmt.query, stmt.arg)
	}

	mutableFamilies := []struct {
		table        string
		idColumn     string
		id           string
		historyTable string
	}{
		{table: "timers", idColumn: "timer_id", id: "51000000-0000-0000-0000-000000000001", historyTable: "timer_history"},
		{table: "entity_state", idColumn: "entity_id", id: "52000000-0000-0000-0000-000000000001", historyTable: "entity_state_history"},
		{table: "agent_sessions", idColumn: "session_id", id: "53000000-0000-0000-0000-000000000001", historyTable: "agent_session_history"},
		{table: "agent_conversation_audits", idColumn: "audit_id", id: "54000000-0000-0000-0000-000000000001", historyTable: "conversation_audit_history"},
		{table: "reply_contexts", idColumn: "reply_context_id", id: "55000000-0000-0000-0000-000000000001", historyTable: "reply_context_history"},
		{table: "activity_attempts", idColumn: "attempt_id", id: "56000000-0000-0000-0000-000000000001", historyTable: "activity_attempt_history"},
	}
	for _, family := range mutableFamilies {
		mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.%s(%s,run_id,value) VALUES ($1,$2,'inserted')`, qSchema, quoteTemporalIdent(family.table), quoteTemporalIdent(family.idColumn)), family.id, runID)
		mustExecTemporal(t, ctx, tx, fmt.Sprintf(`UPDATE %s.%s SET value='updated' WHERE %s=$1`, qSchema, quoteTemporalIdent(family.table), quoteTemporalIdent(family.idColumn)), family.id)
		mustExecTemporal(t, ctx, tx, fmt.Sprintf(`DELETE FROM %s.%s WHERE %s=$1`, qSchema, quoteTemporalIdent(family.table), quoteTemporalIdent(family.idColumn)), family.id)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`UPDATE %s.runs SET status='paused' WHERE run_id=$1`, qSchema), runID)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit all-family restricted runtime proof: %v", err)
	}

	for _, family := range mutableFamilies {
		var count int
		if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.%s WHERE fact_id=$1 AND operation IN ('insert','update','delete')`, qSchema, quoteTemporalIdent(family.historyTable)), family.id).Scan(&count); err != nil {
			t.Fatalf("count %s typed history: %v", family.table, err)
		}
		if count != 3 {
			t.Fatalf("%s typed history count = %d, want 3", family.table, count)
		}
	}
	for _, family := range []struct {
		table string
		idCol string
		id    string
	}{
		{table: "entity_mutations", idCol: "mutation_id", id: "41000000-0000-0000-0000-000000000001"},
		{table: "dead_letters", idCol: "dead_letter_id", id: "42000000-0000-0000-0000-000000000001"},
		{table: "agent_turns", idCol: "turn_id", id: "43000000-0000-0000-0000-000000000001"},
		{table: "selected_fork_lineage", idCol: "lineage_id", id: "44000000-0000-0000-0000-000000000001"},
	} {
		var revision int64
		if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT temporal_revision FROM %s.%s WHERE %s=$1`, qSchema, quoteTemporalIdent(family.table), quoteTemporalIdent(family.idCol)), family.id).Scan(&revision); err != nil {
			t.Fatalf("read %s temporal stamp: %v", family.table, err)
		}
		if revision <= 0 {
			t.Fatalf("%s temporal revision = %d, want positive", family.table, revision)
		}
	}
}

func authorizeTemporalCleanup(t *testing.T, ctx context.Context, db *sql.DB, schema, authorizationID, operationID, actor string, normalRuns, destructiveRuns []string) {
	t.Helper()
	if normalRuns == nil {
		normalRuns = []string{}
	}
	mustExecTemporal(t, ctx, db, fmt.Sprintf(`SELECT %s.swarm_authorize_run_cleanup($1,$2,$3,$4::uuid[],$5::uuid[])`, quoteTemporalIdent(schema)), authorizationID, operationID, actor, pq.Array(normalRuns), pq.Array(destructiveRuns))
}

func assertTemporalAuthorizedMixedCleanup(t *testing.T, ctx context.Context, cleanupAuthorizerDB, runtimeDB *sql.DB, schema, survivorRun, deletedRun string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	authorizationID := "61000000-0000-0000-0000-000000000001"
	timerID := "61000000-0000-0000-0000-000000000002"
	cleanupTimerID := "61000000-0000-0000-0000-000000000004"
	tx, err := runtimeDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin mixed-cleanup fixture: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid,$2::uuid])`, qSchema), survivorRun, deletedRun)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.timers(timer_id,run_id,value) VALUES ($1,$2,'before-cleanup')`, qSchema), timerID, survivorRun)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.events(event_id,run_id) VALUES ('61000000-0000-0000-0000-000000000003',$1)`, qSchema), deletedRun)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.timers(timer_id,run_id,value) VALUES ($1,$2,'must-delete-with-run')`, qSchema), cleanupTimerID, deletedRun)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit mixed-cleanup fixture: %v", err)
	}
	authorizeTemporalCleanup(t, ctx, cleanupAuthorizerDB, schema, authorizationID, "runtime.nuke/operation-1", "operator-token:7", []string{survivorRun}, []string{deletedRun})

	tx, err = runtimeDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin partial destructive cleanup rejection: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_authorized_run_cleanup($1)`, qSchema), authorizationID)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s.timers WHERE timer_id=$1`, qSchema), cleanupTimerID); err == nil || !strings.Contains(err.Error(), "current-xid whole-run tombstone") {
		_ = tx.Rollback()
		t.Fatalf("partial destructive fact deletion error = %v, want tombstone refusal", err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("claim plus partial destructive fact deletion unexpectedly committed")
	}
	var cleanupTimerCount, prematureTombstones int
	if err := runtimeDB.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT (SELECT count(*) FROM %[1]s.timers WHERE timer_id=$1),
		       (SELECT count(*) FROM %[1]s.run_deletion_tombstones WHERE run_id=$2)
	`, qSchema), cleanupTimerID, deletedRun).Scan(&cleanupTimerCount, &prematureTombstones); err != nil {
		t.Fatalf("read rolled-back partial cleanup proof: %v", err)
	}
	if cleanupTimerCount != 1 || prematureTombstones != 0 {
		t.Fatalf("partial cleanup rollback = timer:%d tombstones:%d", cleanupTimerCount, prematureTombstones)
	}

	tx, err = runtimeDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin authorized mixed cleanup: %v", err)
	}
	defer tx.Rollback()
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_authorized_run_cleanup($1)`, qSchema), authorizationID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`UPDATE %s.timers SET value='reference-severed' WHERE timer_id=$1`, qSchema), timerID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_delete_authorized_runs($1)`, qSchema), authorizationID)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit authorized mixed cleanup: %v", err)
	}

	var deletedCount, survivorHistory, cleanupFactCount int
	var actor string
	if err := runtimeDB.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT (SELECT count(*) FROM %[1]s.runs WHERE run_id=$1),
		       (SELECT count(*) FROM %[1]s.timer_history WHERE fact_id=$2 AND operation='update'),
		       (SELECT count(*) FROM %[1]s.timers WHERE timer_id=$3),
		       (SELECT deleted_by FROM %[1]s.run_deletion_tombstones WHERE run_id=$1)
	`, qSchema), deletedRun, timerID, cleanupTimerID).Scan(&deletedCount, &survivorHistory, &cleanupFactCount, &actor); err != nil {
		t.Fatalf("read authorized mixed cleanup proof: %v", err)
	}
	var authorizer string
	if err := cleanupAuthorizerDB.QueryRowContext(ctx, `SELECT current_user`).Scan(&authorizer); err != nil {
		t.Fatalf("read cleanup authorizer identity: %v", err)
	}
	if deletedCount != 0 || survivorHistory != 1 || cleanupFactCount != 0 || actor != authorizer+":operator-token:7" {
		t.Fatalf("mixed cleanup = deleted:%d survivor_history:%d cleanup_facts:%d actor:%q", deletedCount, survivorHistory, cleanupFactCount, actor)
	}
}

func assertTemporalRollbackPublishesNothing(t *testing.T, ctx context.Context, db *sql.DB, schema, runID string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	var beforeFrontier, beforeRevisions int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT current_revision,(SELECT count(*) FROM %[1]s.run_temporal_revisions WHERE run_id=$1)
		FROM %[1]s.run_temporal_frontiers WHERE run_id=$1
	`, qSchema), runID).Scan(&beforeFrontier, &beforeRevisions); err != nil {
		t.Fatalf("read pre-rollback temporal authority: %v", err)
	}
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
	if frontier != beforeFrontier || revisionCount != beforeRevisions || eventCount != 0 {
		t.Fatalf("rollback published frontier=%d/%d revisions=%d/%d events=%d", frontier, beforeFrontier, revisionCount, beforeRevisions, eventCount)
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

func assertTemporalUnversionedDestruction(t *testing.T, ctx context.Context, cleanupAuthorizerDB, db *sql.DB, schema, runID, eventID, deliveryID, receiptID string) {
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

	authorizationID := "62000000-0000-0000-0000-000000000001"
	authorizeTemporalCleanup(t, ctx, cleanupAuthorizerDB, schema, authorizationID, "runtime.nuke/legacy", "operator-token:legacy", nil, []string{runID})
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin authorized unversioned cleanup: %v", err)
	}
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_authorized_run_cleanup($1)`, qSchema), authorizationID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_delete_authorized_runs($1)`, qSchema), authorizationID)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit authorized unversioned cleanup: %v", err)
	}
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

func assertTemporalEveryFamilyDestructiveCascade(t *testing.T, ctx context.Context, cleanupAuthorizerDB, runtimeDB *sql.DB, schema string) {
	t.Helper()
	qSchema := quoteTemporalIdent(schema)
	runID := "70000000-0000-0000-0000-000000000001"
	eventID := "70000000-0000-0000-0000-000000000002"
	deliveryID := "70000000-0000-0000-0000-000000000003"
	receiptID := "70000000-0000-0000-0000-000000000004"

	mustExecTemporal(t, ctx, runtimeDB, fmt.Sprintf(`SELECT %s.swarm_create_run($1,'running')`, qSchema), runID)
	tx, err := runtimeDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin every-family destructive fixture: %v", err)
	}
	defer tx.Rollback()
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_temporal_runs(ARRAY[$1::uuid])`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.events(event_id,run_id) VALUES ($1,$2)`, qSchema), eventID, runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.event_deliveries(delivery_id,run_id,event_id,status) VALUES ($1,$2,$3,'pending')`, qSchema), deliveryID, runID, eventID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.event_receipts(receipt_id,event_id,outcome) VALUES ($1,$2,'success')`, qSchema), receiptID, eventID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.dead_letters(dead_letter_id,original_event_id,value) VALUES ('70000000-0000-0000-0000-000000000005',$1,'fixture')`, qSchema), eventID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.entity_mutations(mutation_id,run_id,value) VALUES ('70000000-0000-0000-0000-000000000006',$1,'fixture')`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.entity_state(entity_id,run_id,value) VALUES ('70000000-0000-0000-0000-000000000007',$1,'fixture')`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.timers(timer_id,run_id,value) VALUES ('70000000-0000-0000-0000-000000000008',$1,'fixture')`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.agent_sessions(session_id,run_id,value) VALUES ('70000000-0000-0000-0000-000000000009',$1,'fixture')`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.agent_turns(turn_id,run_id,value) VALUES ('70000000-0000-0000-0000-00000000000a',$1,'fixture')`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.agent_conversation_audits(audit_id,run_id,value) VALUES ('70000000-0000-0000-0000-00000000000b',$1,'fixture')`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.reply_contexts(reply_context_id,run_id,value) VALUES ('70000000-0000-0000-0000-00000000000c',$1,'fixture')`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.activity_attempts(attempt_id,run_id,value) VALUES ('70000000-0000-0000-0000-00000000000d',$1,'fixture')`, qSchema), runID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`INSERT INTO %s.selected_fork_lineage(lineage_id,run_id,value) VALUES ('70000000-0000-0000-0000-00000000000e',$1,'fixture')`, qSchema), runID)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit every-family destructive fixture: %v", err)
	}

	authorizationID := "70000000-0000-0000-0000-00000000000f"
	authorizeTemporalCleanup(t, ctx, cleanupAuthorizerDB, schema, authorizationID, "runtime.nuke/every-family", "operator-token:every-family", nil, []string{runID})
	tx, err = runtimeDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin every-family destructive cleanup: %v", err)
	}
	defer tx.Rollback()
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_claim_authorized_run_cleanup($1)`, qSchema), authorizationID)
	mustExecTemporal(t, ctx, tx, fmt.Sprintf(`SELECT %s.swarm_delete_authorized_runs($1)`, qSchema), authorizationID)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit every-family destructive cleanup: %v", err)
	}

	checks := []struct {
		table    string
		idColumn string
		id       string
	}{
		{table: "runs", idColumn: "run_id", id: runID},
		{table: "events", idColumn: "event_id", id: eventID},
		{table: "event_deliveries", idColumn: "delivery_id", id: deliveryID},
		{table: "event_receipts", idColumn: "receipt_id", id: receiptID},
		{table: "dead_letters", idColumn: "dead_letter_id", id: "70000000-0000-0000-0000-000000000005"},
		{table: "entity_mutations", idColumn: "mutation_id", id: "70000000-0000-0000-0000-000000000006"},
		{table: "entity_state", idColumn: "entity_id", id: "70000000-0000-0000-0000-000000000007"},
		{table: "timers", idColumn: "timer_id", id: "70000000-0000-0000-0000-000000000008"},
		{table: "agent_sessions", idColumn: "session_id", id: "70000000-0000-0000-0000-000000000009"},
		{table: "agent_turns", idColumn: "turn_id", id: "70000000-0000-0000-0000-00000000000a"},
		{table: "agent_conversation_audits", idColumn: "audit_id", id: "70000000-0000-0000-0000-00000000000b"},
		{table: "reply_contexts", idColumn: "reply_context_id", id: "70000000-0000-0000-0000-00000000000c"},
		{table: "activity_attempts", idColumn: "attempt_id", id: "70000000-0000-0000-0000-00000000000d"},
		{table: "selected_fork_lineage", idColumn: "lineage_id", id: "70000000-0000-0000-0000-00000000000e"},
	}
	for _, check := range checks {
		var count int
		if err := runtimeDB.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.%s WHERE %s=$1`, qSchema, quoteTemporalIdent(check.table), quoteTemporalIdent(check.idColumn)), check.id).Scan(&count); err != nil {
			t.Fatalf("count destructively cascaded %s: %v", check.table, err)
		}
		if count != 0 {
			t.Fatalf("authorized destructive cascade left %s row %s", check.table, check.id)
		}
	}
	var tombstones int
	if err := runtimeDB.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.run_deletion_tombstones WHERE run_id=$1 AND transaction_id IS NOT NULL`, qSchema), runID).Scan(&tombstones); err != nil {
		t.Fatalf("read every-family deletion tombstone: %v", err)
	}
	if tombstones != 1 {
		t.Fatalf("every-family destructive tombstones = %d, want 1", tombstones)
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

const temporalRegisteredLegacyCatalogChecksum = "12323dcc1333b52314fe166dbf5db811560e287ca4b247beac2bab766b46e01e"

func temporalCatalogChecksum(ctx context.Context, db interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, schema, owner, runtimeRole string) (string, error) {
	queries := []string{
		`SELECT 'relation',c.relkind::text,c.relname,r.rolname,COALESCE(c.relacl::text,'')
		 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles r ON r.oid=c.relowner
		 WHERE n.nspname=$1 AND c.relkind IN ('r','S','i')`,
		`SELECT 'column',c.relname,a.attnum::text,a.attname,
		        format_type(a.atttypid,a.atttypmod),a.attnotnull::text,
		        COALESCE(pg_get_expr(d.adbin,d.adrelid),'')
		 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
		 LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
		 WHERE n.nspname=$1 AND c.relkind IN ('r','S')`,
		`SELECT 'constraint',c.relname,con.contype::text,con.conname,pg_get_constraintdef(con.oid,true)
		 FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname=$1`,
		`SELECT 'index',tablename,indexname,indexdef
		 FROM pg_indexes WHERE schemaname=$1`,
		`SELECT 'trigger',c.relname,t.tgname,t.tgenabled::text,p.proname,pg_get_triggerdef(t.oid,true)
		 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
		 JOIN pg_proc p ON p.oid=t.tgfoid WHERE n.nspname=$1 AND NOT t.tgisinternal`,
		`SELECT 'function',p.proname,pg_get_function_identity_arguments(p.oid),p.prosecdef::text,r.rolname,
		        COALESCE(array_to_string(p.proconfig,','),''),COALESCE(p.proacl::text,''),pg_get_functiondef(p.oid)
		 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_roles r ON r.oid=p.proowner
		 WHERE n.nspname=$1`,
		`SELECT 'schema',n.nspname,r.rolname,COALESCE(n.nspacl::text,'')
		 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname=$1`,
	}
	var entries []string
	for _, query := range queries {
		rows, err := db.QueryContext(ctx, query, schema)
		if err != nil {
			return "", err
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			return "", err
		}
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				return "", err
			}
			parts := make([]string, len(values))
			for i, value := range values {
				if value.Valid {
					parts[i] = value.String
				}
			}
			entry := strings.Join(parts, "\x1f")
			entry = strings.ReplaceAll(entry, quoteTemporalIdent(schema), "<schema>")
			entry = strings.ReplaceAll(entry, schema+".", "<schema>.")
			entry = strings.ReplaceAll(entry, schema, "<schema>")
			entry = strings.ReplaceAll(entry, owner, "<owner>")
			entry = strings.ReplaceAll(entry, runtimeRole, "<runtime>")
			entries = append(entries, entry)
		}
		if err := rows.Close(); err != nil {
			return "", err
		}
		if err := rows.Err(); err != nil {
			return "", err
		}
	}
	sort.Strings(entries)
	digest := sha256.Sum256([]byte(strings.Join(entries, "\n")))
	return hex.EncodeToString(digest[:]), nil
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

func temporalGrantDDL(schema, runtimeRole, cleanupAuthorizerRole string) string {
	return fmt.Sprintf(temporalPrototypeGrantTemplate, schema, runtimeRole, cleanupAuthorizerRole)
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
		status TEXT NOT NULL CHECK (status IN ('running','paused','completed','failed','cancelled','forked')),
		value TEXT NOT NULL DEFAULT ''
	);
	CREATE TABLE %[1]s.events (
		event_id UUID PRIMARY KEY,
		run_id UUID REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
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
	CREATE TABLE %[1]s.dead_letters (
		dead_letter_id UUID PRIMARY KEY,
		original_event_id UUID NOT NULL REFERENCES %[1]s.events(event_id) ON DELETE CASCADE,
		value TEXT NOT NULL DEFAULT ''
	);
	CREATE TABLE %[1]s.entity_mutations (
		mutation_id UUID PRIMARY KEY,
		run_id UUID NOT NULL REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		value TEXT NOT NULL DEFAULT ''
	);
	CREATE TABLE %[1]s.entity_state (
		entity_id UUID PRIMARY KEY,
		run_id UUID NOT NULL REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		value TEXT NOT NULL DEFAULT ''
	);
	CREATE TABLE %[1]s.timers (
		timer_id UUID PRIMARY KEY,
		run_id UUID REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		value TEXT NOT NULL DEFAULT ''
	);
	CREATE TABLE %[1]s.agent_sessions (
		session_id UUID PRIMARY KEY,
		run_id UUID NOT NULL REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		value TEXT NOT NULL DEFAULT ''
	);
	CREATE TABLE %[1]s.agent_turns (
		turn_id UUID PRIMARY KEY,
		run_id UUID NOT NULL REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		value TEXT NOT NULL DEFAULT ''
	);
	CREATE TABLE %[1]s.agent_conversation_audits (
		audit_id UUID PRIMARY KEY,
		run_id UUID NOT NULL REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		value TEXT NOT NULL DEFAULT ''
	);
	CREATE TABLE %[1]s.reply_contexts (
		reply_context_id UUID PRIMARY KEY,
		run_id UUID REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		value TEXT NOT NULL DEFAULT ''
	);
	CREATE TABLE %[1]s.activity_attempts (
		attempt_id UUID PRIMARY KEY,
		run_id UUID NOT NULL REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		value TEXT NOT NULL DEFAULT ''
	);
	CREATE TABLE %[1]s.selected_fork_lineage (
		lineage_id UUID PRIMARY KEY,
		run_id UUID NOT NULL REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		value TEXT NOT NULL DEFAULT ''
	);
`

const temporalPrototypeSchemaTemplate = `
	ALTER TABLE %[1]s.runtime_store_metadata
		ADD COLUMN schema_generation TEXT,
		ADD COLUMN schema_ddl_sha256 TEXT,
		ADD COLUMN schema_catalog_sha256 TEXT,
		ADD COLUMN schema_owner_role TEXT,
		ADD COLUMN runtime_role TEXT,
		ADD COLUMN cleanup_authorizer_role TEXT;
	ALTER TABLE %[1]s.runs ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.events ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.event_deliveries ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.event_receipts
		ADD COLUMN temporal_run_id UUID REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE,
		ADD COLUMN temporal_revision BIGINT,
		ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.dead_letters ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.entity_mutations ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.entity_state ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.timers ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.agent_sessions ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.agent_turns ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.agent_conversation_audits ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.reply_contexts ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.activity_attempts ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	ALTER TABLE %[1]s.selected_fork_lineage ADD COLUMN temporal_revision BIGINT, ADD COLUMN temporal_ordinal INTEGER;
	UPDATE %[1]s.event_receipts AS receipt
	SET temporal_run_id=event.run_id
	FROM %[1]s.events AS event
	WHERE event.event_id=receipt.event_id;

	CREATE TABLE %[1]s.run_temporal_transactions (
		transaction_id XID8 PRIMARY KEY,
		declared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
	);
	CREATE TABLE %[1]s.run_temporal_transaction_runs (
		transaction_id XID8 NOT NULL REFERENCES %[1]s.run_temporal_transactions(transaction_id) ON DELETE CASCADE,
		run_id UUID NOT NULL,
		disposition TEXT NOT NULL CHECK (disposition IN ('normal','destructive')),
		PRIMARY KEY (transaction_id,run_id)
	);
	CREATE TABLE %[1]s.run_temporal_frontiers (
		run_id UUID PRIMARY KEY REFERENCES %[1]s.runs(run_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
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
		catalog_sha256 TEXT NOT NULL CHECK (catalog_sha256 ~ '^[0-9a-f]{64}$'),
		schema_owner_role TEXT NOT NULL,
		runtime_role TEXT NOT NULL,
		cleanup_authorizer_role TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
		CHECK (schema_owner_role<>runtime_role AND schema_owner_role<>cleanup_authorizer_role AND runtime_role<>cleanup_authorizer_role)
	);
	CREATE TABLE %[1]s.run_cleanup_authorizations (
		authorization_id UUID PRIMARY KEY,
		operation_id TEXT NOT NULL UNIQUE,
		actor_evidence TEXT NOT NULL CHECK (btrim(actor_evidence)<>''),
		authorized_by TEXT NOT NULL CHECK (btrim(authorized_by)<>''),
		normal_run_ids UUID[] NOT NULL DEFAULT '{}',
		destructive_run_ids UUID[] NOT NULL,
		authorized_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
		claimed_transaction_id XID8 UNIQUE,
		CHECK (cardinality(destructive_run_ids)>0)
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
	CREATE FUNCTION %[1]s.swarm_declare_temporal_runs(normal_run_ids UUID[], destructive_run_ids UUID[])
	RETURNS VOID
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		current_xid XID8 := pg_current_xact_id();
		normalized_normal UUID[];
		normalized_destructive UUID[];
		existing_normal UUID[];
		existing_destructive UUID[];
		target_run UUID;
		target_disposition TEXT;
		target_model INTEGER;
		target_history_complete BOOLEAN;
		next_revision BIGINT;
	BEGIN
		SELECT array_agg(run_id ORDER BY run_id)
		INTO normalized_normal
		FROM (SELECT DISTINCT unnest(COALESCE(normal_run_ids,'{}'::uuid[])) AS run_id) declared
		WHERE run_id IS NOT NULL;
		SELECT array_agg(run_id ORDER BY run_id)
		INTO normalized_destructive
		FROM (SELECT DISTINCT unnest(COALESCE(destructive_run_ids,'{}'::uuid[])) AS run_id) declared
		WHERE run_id IS NOT NULL;
		normalized_normal=COALESCE(normalized_normal,'{}'::uuid[]);
		normalized_destructive=COALESCE(normalized_destructive,'{}'::uuid[]);
		IF cardinality(normalized_normal)+cardinality(normalized_destructive)=0 THEN
			RAISE EXCEPTION 'temporal declaration requires at least one run';
		END IF;
		IF EXISTS (SELECT 1 FROM unnest(normalized_normal) n JOIN unnest(normalized_destructive) d ON n=d) THEN
			RAISE EXCEPTION 'temporal declaration dispositions overlap';
		END IF;
		SELECT COALESCE(array_agg(run_id ORDER BY run_id) FILTER (WHERE disposition='normal'),'{}'::uuid[]),
		       COALESCE(array_agg(run_id ORDER BY run_id) FILTER (WHERE disposition='destructive'),'{}'::uuid[])
		INTO existing_normal,existing_destructive
		FROM %[1]s.run_temporal_transaction_runs
		WHERE transaction_id=current_xid;
		PERFORM 1 FROM %[1]s.run_temporal_transactions WHERE transaction_id=current_xid;
		IF FOUND THEN
			IF existing_normal<>normalized_normal OR existing_destructive<>normalized_destructive THEN
				RAISE EXCEPTION 'temporal declaration is sealed for this transaction';
			END IF;
			RETURN;
		END IF;
		INSERT INTO %[1]s.run_temporal_transactions(transaction_id) VALUES (current_xid);
		INSERT INTO %[1]s.run_temporal_transaction_runs(transaction_id,run_id,disposition)
		SELECT current_xid,run_id,'normal' FROM unnest(normalized_normal) AS normal_runs(run_id)
		UNION ALL
		SELECT current_xid,run_id,'destructive' FROM unnest(normalized_destructive) AS destructive_runs(run_id);
		FOR target_run,target_disposition IN
			SELECT run_id,disposition FROM %[1]s.run_temporal_transaction_runs
			WHERE transaction_id=current_xid ORDER BY run_id
		LOOP
			SELECT model_version,history_complete
			INTO target_model,target_history_complete
			FROM %[1]s.run_temporal_frontiers
			WHERE run_id=target_run
			FOR UPDATE;
			IF NOT FOUND THEN
				RAISE EXCEPTION 'run %% has no temporal frontier', target_run;
			END IF;
			IF target_disposition='normal' THEN
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

	CREATE FUNCTION %[1]s.swarm_create_run(target_run UUID, initial_status TEXT)
	RETURNS VOID
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		stamp RECORD;
		created_row JSONB;
	BEGIN
		IF initial_status NOT IN ('running','paused') THEN
			RAISE EXCEPTION 'unsupported initial run status %%', initial_status;
		END IF;
		INSERT INTO %[1]s.run_temporal_frontiers(run_id,model_version,current_revision,history_complete)
		VALUES (target_run,1,0,true);
		PERFORM %[1]s.swarm_declare_temporal_runs(ARRAY[target_run],'{}'::uuid[]);
		SELECT * INTO stamp FROM %[1]s.swarm_next_temporal_ordinal(target_run);
		INSERT INTO %[1]s.runs(run_id,status,temporal_revision,temporal_ordinal)
		VALUES (target_run,initial_status,stamp.temporal_revision,stamp.temporal_ordinal);
		SELECT to_jsonb(run_row) INTO STRICT created_row
		FROM %[1]s.runs AS run_row WHERE run_id=target_run;
		INSERT INTO %[1]s.run_lifecycle_history(run_id,revision,ordinal,operation,fact_id,after_state)
		VALUES (target_run,stamp.temporal_revision,stamp.temporal_ordinal,'insert',target_run::text,created_row);
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_claim_temporal_runs(requested_run_ids UUID[])
	RETURNS VOID
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	BEGIN
		PERFORM %[1]s.swarm_declare_temporal_runs(requested_run_ids,'{}'::uuid[]);
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_authorize_run_cleanup(target_authorization UUID, target_operation TEXT, target_actor TEXT, requested_normal UUID[], requested_destructive UUID[])
	RETURNS VOID
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		normalized_normal UUID[];
		normalized_destructive UUID[];
	BEGIN
		IF btrim(COALESCE(target_operation,''))='' OR btrim(COALESCE(target_actor,''))='' THEN
			RAISE EXCEPTION 'cleanup authorization requires operation and actor evidence';
		END IF;
		SELECT COALESCE(array_agg(run_id ORDER BY run_id),'{}'::uuid[])
		INTO normalized_normal
		FROM (SELECT DISTINCT unnest(COALESCE(requested_normal,'{}'::uuid[])) AS run_id) declared
		WHERE run_id IS NOT NULL;
		SELECT COALESCE(array_agg(run_id ORDER BY run_id),'{}'::uuid[])
		INTO normalized_destructive
		FROM (SELECT DISTINCT unnest(COALESCE(requested_destructive,'{}'::uuid[])) AS run_id) declared
		WHERE run_id IS NOT NULL;
		IF cardinality(normalized_destructive)=0
		   OR EXISTS (SELECT 1 FROM unnest(normalized_normal) n JOIN unnest(normalized_destructive) d ON n=d) THEN
			RAISE EXCEPTION 'cleanup authorization requires disjoint sets and at least one destructive run';
		END IF;
		INSERT INTO %[1]s.run_cleanup_authorizations(
			authorization_id,operation_id,actor_evidence,authorized_by,normal_run_ids,destructive_run_ids
		) VALUES (
			target_authorization,target_operation,target_actor,session_user,normalized_normal,normalized_destructive
		);
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_claim_authorized_run_cleanup(target_authorization UUID)
	RETURNS VOID
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		normalized_normal UUID[];
		normalized_destructive UUID[];
		claimed XID8;
		current_xid XID8 := pg_current_xact_id();
	BEGIN
		SELECT normal_run_ids,destructive_run_ids,claimed_transaction_id
		INTO STRICT normalized_normal,normalized_destructive,claimed
		FROM %[1]s.run_cleanup_authorizations
		WHERE authorization_id=target_authorization
		FOR UPDATE;
		IF normalized_normal<>ARRAY(SELECT DISTINCT x FROM unnest(normalized_normal) x ORDER BY x)
		   OR normalized_destructive<>ARRAY(SELECT DISTINCT x FROM unnest(normalized_destructive) x ORDER BY x)
		   OR EXISTS (SELECT 1 FROM unnest(normalized_normal) n JOIN unnest(normalized_destructive) d ON n=d) THEN
			RAISE EXCEPTION 'cleanup authorization run sets are not sealed canonical sets';
		END IF;
		IF claimed IS NOT NULL AND claimed<>current_xid THEN
			RAISE EXCEPTION 'cleanup authorization already consumed';
		END IF;
		UPDATE %[1]s.run_cleanup_authorizations
		SET claimed_transaction_id=current_xid
		WHERE authorization_id=target_authorization;
		PERFORM %[1]s.swarm_declare_temporal_runs(normalized_normal,normalized_destructive);
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_delete_authorized_runs(target_authorization UUID)
	RETURNS VOID
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		cleanup_runs UUID[];
		actor TEXT;
		authorizer TEXT;
		target_run UUID;
		model INTEGER;
		frontier BIGINT;
	BEGIN
		SELECT destructive_run_ids,actor_evidence,authorized_by
		INTO STRICT cleanup_runs,actor,authorizer
		FROM %[1]s.run_cleanup_authorizations
		WHERE authorization_id=target_authorization
		  AND claimed_transaction_id=pg_current_xact_id();
		FOREACH target_run IN ARRAY cleanup_runs LOOP
			SELECT model_version,current_revision INTO STRICT model,frontier
			FROM %[1]s.run_temporal_frontiers WHERE run_id=target_run;
			INSERT INTO %[1]s.run_deletion_tombstones(run_id,source_model_version,last_revision,transaction_id,deleted_by)
			VALUES (target_run,model,CASE WHEN model=0 THEN NULL ELSE frontier END,pg_current_xact_id(),authorizer||':'||actor);
			DELETE FROM %[1]s.event_receipts receipt
			USING %[1]s.events event
			WHERE receipt.event_id=event.event_id AND event.run_id=target_run;
			DELETE FROM %[1]s.dead_letters dead_letter
			USING %[1]s.events event
			WHERE dead_letter.original_event_id=event.event_id AND event.run_id=target_run;
			DELETE FROM %[1]s.event_deliveries WHERE run_id=target_run;
			DELETE FROM %[1]s.runs WHERE run_id=target_run;
			IF NOT FOUND THEN RAISE EXCEPTION 'authorized cleanup run %% does not exist', target_run; END IF;
		END LOOP;
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
		declared_disposition TEXT;
	BEGIN
		SELECT disposition INTO declared_disposition
		FROM %[1]s.run_temporal_transaction_runs
		WHERE transaction_id=current_xid AND run_id=target_run;
		IF NOT FOUND OR declared_disposition<>'normal' THEN
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

	CREATE FUNCTION %[1]s.swarm_resolve_temporal_run(row_data JSONB, lineage_kind TEXT, lineage_field TEXT)
	RETURNS UUID
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		resolved UUID;
	BEGIN
		IF lineage_kind='direct' THEN
			RETURN NULLIF(row_data->>lineage_field,'')::uuid;
		ELSIF lineage_kind='event' THEN
			SELECT run_id INTO STRICT resolved FROM %[1]s.events
			WHERE event_id=NULLIF(row_data->>lineage_field,'')::uuid;
			RETURN resolved;
		END IF;
		RAISE EXCEPTION 'unsupported temporal lineage kind %%', lineage_kind;
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_guard_append_fact()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		stamp RECORD;
		fact_run UUID;
		destructive BOOLEAN;
	BEGIN
		IF TG_OP='UPDATE' THEN
			RAISE EXCEPTION 'append-only temporal fact %% is immutable', TG_TABLE_NAME;
		END IF;
		IF TG_OP='DELETE' THEN
			fact_run=%[1]s.swarm_resolve_temporal_run(to_jsonb(OLD),TG_ARGV[1],TG_ARGV[2]);
			IF fact_run IS NULL THEN RAISE EXCEPTION 'runless append-only fact cannot be deleted'; END IF;
			SELECT EXISTS (
				SELECT 1
				FROM %[1]s.run_temporal_transaction_runs declared
				JOIN %[1]s.run_deletion_tombstones tombstone
				  ON tombstone.run_id=declared.run_id
				 AND tombstone.transaction_id=declared.transaction_id
				WHERE declared.transaction_id=pg_current_xact_id()
				  AND declared.run_id=fact_run
				  AND declared.disposition='destructive'
			) INTO destructive;
			IF NOT destructive THEN RAISE EXCEPTION 'append-only fact deletion requires current-xid whole-run tombstone'; END IF;
			RETURN OLD;
		END IF;
		fact_run=%[1]s.swarm_resolve_temporal_run(to_jsonb(NEW),TG_ARGV[1],TG_ARGV[2]);
		IF fact_run IS NULL THEN
			NEW=jsonb_populate_record(NEW,jsonb_build_object('temporal_revision',NULL,'temporal_ordinal',NULL));
			RETURN NEW;
		END IF;
		SELECT * INTO stamp FROM %[1]s.swarm_next_temporal_ordinal(fact_run);
		NEW=jsonb_populate_record(NEW,jsonb_build_object('temporal_revision',stamp.temporal_revision,'temporal_ordinal',stamp.temporal_ordinal));
		RETURN NEW;
	END
	$function$;

	CREATE FUNCTION %[1]s.swarm_guard_mutable_fact()
	RETURNS TRIGGER
	LANGUAGE plpgsql
	SECURITY DEFINER
	SET search_path = pg_catalog, %[1]s
	AS $function$
	DECLARE
		old_run UUID;
		new_run UUID;
		asserted_run UUID;
		old_stamp RECORD;
		new_stamp RECORD;
		destructive BOOLEAN;
		declared_destructive BOOLEAN;
		fact_id TEXT;
		history_table TEXT := TG_ARGV[3];
	BEGIN
		IF TG_OP<>'INSERT' THEN
			fact_id=COALESCE(to_jsonb(OLD)->>TG_ARGV[0],'');
			IF TG_ARGV[1]='event' THEN
				old_run=NULLIF(to_jsonb(OLD)->>'temporal_run_id','')::uuid;
			ELSIF TG_ARGV[1]='asserted_event' THEN
				asserted_run=NULLIF(to_jsonb(OLD)->>'run_id','')::uuid;
				IF TG_OP='DELETE' THEN
					old_run=asserted_run;
				ELSE
					old_run=%[1]s.swarm_resolve_temporal_run(to_jsonb(OLD),'event',TG_ARGV[2]);
					IF old_run IS DISTINCT FROM asserted_run THEN RAISE EXCEPTION 'stored run identity disagrees with event lineage'; END IF;
				END IF;
			ELSE
				old_run=%[1]s.swarm_resolve_temporal_run(to_jsonb(OLD),'direct',TG_ARGV[2]);
			END IF;
		END IF;
		IF TG_OP<>'DELETE' THEN
			fact_id=COALESCE(to_jsonb(NEW)->>TG_ARGV[0],'');
			IF TG_ARGV[1]='event' THEN
				new_run=%[1]s.swarm_resolve_temporal_run(to_jsonb(NEW),'event',TG_ARGV[2]);
				NEW=jsonb_populate_record(NEW,jsonb_build_object('temporal_run_id',new_run));
			ELSIF TG_ARGV[1]='asserted_event' THEN
				new_run=%[1]s.swarm_resolve_temporal_run(to_jsonb(NEW),'event',TG_ARGV[2]);
				asserted_run=NULLIF(to_jsonb(NEW)->>'run_id','')::uuid;
				IF new_run IS DISTINCT FROM asserted_run THEN RAISE EXCEPTION 'stored run identity disagrees with event lineage'; END IF;
			ELSE
				new_run=%[1]s.swarm_resolve_temporal_run(to_jsonb(NEW),'direct',TG_ARGV[2]);
			END IF;
		END IF;
		IF TG_OP='DELETE' THEN
			IF old_run IS NULL THEN RETURN OLD; END IF;
			SELECT EXISTS (
				SELECT 1 FROM %[1]s.run_temporal_transaction_runs
				WHERE transaction_id=pg_current_xact_id() AND run_id=old_run AND disposition='destructive'
			) INTO declared_destructive;
			SELECT EXISTS (
				SELECT 1
				FROM %[1]s.run_temporal_transaction_runs declared
				JOIN %[1]s.run_deletion_tombstones tombstone
				  ON tombstone.run_id=declared.run_id
				 AND tombstone.transaction_id=declared.transaction_id
				WHERE declared.transaction_id=pg_current_xact_id()
				  AND declared.run_id=old_run
				  AND declared.disposition='destructive'
			) INTO destructive;
			IF declared_destructive THEN
				IF NOT destructive THEN RAISE EXCEPTION 'mutable fact destructive deletion requires current-xid whole-run tombstone'; END IF;
				RETURN OLD;
			END IF;
			SELECT * INTO old_stamp FROM %[1]s.swarm_next_temporal_ordinal(old_run);
			EXECUTE format('INSERT INTO %%I.%%I(run_id,revision,ordinal,operation,fact_id,before_state) VALUES ($1,$2,$3,''delete'',$4,$5)',TG_TABLE_SCHEMA,history_table)
			USING old_run,old_stamp.temporal_revision,old_stamp.temporal_ordinal,fact_id,to_jsonb(OLD);
			RETURN OLD;
		ELSIF TG_OP='INSERT' THEN
			IF new_run IS NULL THEN
				NEW=jsonb_populate_record(NEW,jsonb_build_object('temporal_revision',NULL,'temporal_ordinal',NULL)); RETURN NEW;
			END IF;
			SELECT * INTO new_stamp FROM %[1]s.swarm_next_temporal_ordinal(new_run);
			NEW=jsonb_populate_record(NEW,jsonb_build_object('temporal_revision',new_stamp.temporal_revision,'temporal_ordinal',new_stamp.temporal_ordinal));
			EXECUTE format('INSERT INTO %%I.%%I(run_id,revision,ordinal,operation,fact_id,after_state) VALUES ($1,$2,$3,''insert'',$4,$5)',TG_TABLE_SCHEMA,history_table)
			USING new_run,new_stamp.temporal_revision,new_stamp.temporal_ordinal,fact_id,to_jsonb(NEW);
			RETURN NEW;
		END IF;
		IF old_run IS NOT DISTINCT FROM new_run THEN
			IF new_run IS NULL THEN
				NEW=jsonb_populate_record(NEW,jsonb_build_object('temporal_revision',NULL,'temporal_ordinal',NULL)); RETURN NEW;
			END IF;
			SELECT * INTO new_stamp FROM %[1]s.swarm_next_temporal_ordinal(new_run);
			NEW=jsonb_populate_record(NEW,jsonb_build_object('temporal_revision',new_stamp.temporal_revision,'temporal_ordinal',new_stamp.temporal_ordinal));
			EXECUTE format('INSERT INTO %%I.%%I(run_id,revision,ordinal,operation,fact_id,before_state,after_state) VALUES ($1,$2,$3,''update'',$4,$5,$6)',TG_TABLE_SCHEMA,history_table)
			USING new_run,new_stamp.temporal_revision,new_stamp.temporal_ordinal,fact_id,to_jsonb(OLD),to_jsonb(NEW);
			RETURN NEW;
		END IF;
		IF old_run IS NOT NULL THEN
			SELECT * INTO old_stamp FROM %[1]s.swarm_next_temporal_ordinal(old_run);
			EXECUTE format('INSERT INTO %%I.%%I(run_id,revision,ordinal,operation,fact_id,before_state,after_state) VALUES ($1,$2,$3,''update'',$4,$5,$6)',TG_TABLE_SCHEMA,history_table)
			USING old_run,old_stamp.temporal_revision,old_stamp.temporal_ordinal,fact_id,to_jsonb(OLD),to_jsonb(NEW);
		END IF;
		IF new_run IS NOT NULL THEN
			SELECT * INTO new_stamp FROM %[1]s.swarm_next_temporal_ordinal(new_run);
			NEW=jsonb_populate_record(NEW,jsonb_build_object('temporal_revision',new_stamp.temporal_revision,'temporal_ordinal',new_stamp.temporal_ordinal));
			EXECUTE format('INSERT INTO %%I.%%I(run_id,revision,ordinal,operation,fact_id,before_state,after_state) VALUES ($1,$2,$3,''update'',$4,$5,$6)',TG_TABLE_SCHEMA,history_table)
			USING new_run,new_stamp.temporal_revision,new_stamp.temporal_ordinal,fact_id,to_jsonb(OLD),to_jsonb(NEW);
		ELSE
			NEW=jsonb_populate_record(NEW,jsonb_build_object('temporal_revision',NULL,'temporal_ordinal',NULL));
		END IF;
		RETURN NEW;
	END
	$function$;

	CREATE TRIGGER temporal_runs_guard BEFORE UPDATE OR DELETE ON %[1]s.runs FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_mutable_fact('run_id','direct','run_id','run_lifecycle_history');
	CREATE TRIGGER temporal_events_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.events FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_append_fact('event_id','direct','run_id');
	CREATE TRIGGER temporal_event_deliveries_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.event_deliveries FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_mutable_fact('delivery_id','asserted_event','event_id','event_delivery_history');
	CREATE TRIGGER temporal_event_receipts_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.event_receipts FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_mutable_fact('receipt_id','event','event_id','event_receipt_history');
	CREATE TRIGGER temporal_dead_letters_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.dead_letters FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_append_fact('dead_letter_id','event','original_event_id');
	CREATE TRIGGER temporal_entity_mutations_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.entity_mutations FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_append_fact('mutation_id','direct','run_id');
	CREATE TRIGGER temporal_entity_state_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.entity_state FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_mutable_fact('entity_id','direct','run_id','entity_state_history');
	CREATE TRIGGER temporal_timers_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.timers FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_mutable_fact('timer_id','direct','run_id','timer_history');
	CREATE TRIGGER temporal_agent_sessions_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.agent_sessions FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_mutable_fact('session_id','direct','run_id','agent_session_history');
	CREATE TRIGGER temporal_agent_turns_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.agent_turns FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_append_fact('turn_id','direct','run_id');
	CREATE TRIGGER temporal_agent_conversation_audits_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.agent_conversation_audits FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_mutable_fact('audit_id','direct','run_id','conversation_audit_history');
	CREATE TRIGGER temporal_reply_contexts_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.reply_contexts FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_mutable_fact('reply_context_id','direct','run_id','reply_context_history');
	CREATE TRIGGER temporal_activity_attempts_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.activity_attempts FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_mutable_fact('attempt_id','direct','run_id','activity_attempt_history');
	CREATE TRIGGER temporal_selected_fork_lineage_guard BEFORE INSERT OR UPDATE OR DELETE ON %[1]s.selected_fork_lineage FOR EACH ROW EXECUTE FUNCTION %[1]s.swarm_guard_append_fact('lineage_id','direct','run_id');
	ALTER TABLE %[1]s.runs ENABLE ALWAYS TRIGGER temporal_runs_guard;
	ALTER TABLE %[1]s.events ENABLE ALWAYS TRIGGER temporal_events_guard;
	ALTER TABLE %[1]s.event_deliveries ENABLE ALWAYS TRIGGER temporal_event_deliveries_guard;
	ALTER TABLE %[1]s.event_receipts ENABLE ALWAYS TRIGGER temporal_event_receipts_guard;
	ALTER TABLE %[1]s.dead_letters ENABLE ALWAYS TRIGGER temporal_dead_letters_guard;
	ALTER TABLE %[1]s.entity_mutations ENABLE ALWAYS TRIGGER temporal_entity_mutations_guard;
	ALTER TABLE %[1]s.entity_state ENABLE ALWAYS TRIGGER temporal_entity_state_guard;
	ALTER TABLE %[1]s.timers ENABLE ALWAYS TRIGGER temporal_timers_guard;
	ALTER TABLE %[1]s.agent_sessions ENABLE ALWAYS TRIGGER temporal_agent_sessions_guard;
	ALTER TABLE %[1]s.agent_turns ENABLE ALWAYS TRIGGER temporal_agent_turns_guard;
	ALTER TABLE %[1]s.agent_conversation_audits ENABLE ALWAYS TRIGGER temporal_agent_conversation_audits_guard;
	ALTER TABLE %[1]s.reply_contexts ENABLE ALWAYS TRIGGER temporal_reply_contexts_guard;
	ALTER TABLE %[1]s.activity_attempts ENABLE ALWAYS TRIGGER temporal_activity_attempts_guard;
	ALTER TABLE %[1]s.selected_fork_lineage ENABLE ALWAYS TRIGGER temporal_selected_fork_lineage_guard;
`

const temporalPrototypeGrantTemplate = `
	REVOKE ALL ON SCHEMA %[1]s FROM PUBLIC;
	REVOKE ALL ON ALL TABLES IN SCHEMA %[1]s FROM PUBLIC;
	REVOKE ALL ON ALL SEQUENCES IN SCHEMA %[1]s FROM PUBLIC;
	REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %[1]s FROM PUBLIC;
	GRANT USAGE ON SCHEMA %[1]s TO %[2]s;
	GRANT USAGE ON SCHEMA %[1]s TO %[3]s;
	GRANT SELECT ON %[1]s.runtime_store_metadata,%[1]s.runtime_store_migrations TO %[3]s;
	GRANT SELECT ON
		%[1]s.runtime_store_metadata,
		%[1]s.runtime_store_migrations,
		%[1]s.runs,
		%[1]s.events,
		%[1]s.event_deliveries,
		%[1]s.event_receipts,
		%[1]s.dead_letters,
		%[1]s.entity_mutations,
		%[1]s.entity_state,
		%[1]s.timers,
		%[1]s.agent_sessions,
		%[1]s.agent_turns,
		%[1]s.agent_conversation_audits,
		%[1]s.reply_contexts,
		%[1]s.activity_attempts,
		%[1]s.selected_fork_lineage,
		%[1]s.run_temporal_transactions,
		%[1]s.run_temporal_transaction_runs,
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
	GRANT INSERT ON %[1]s.events,%[1]s.dead_letters,%[1]s.entity_mutations,%[1]s.agent_turns,%[1]s.selected_fork_lineage TO %[2]s;
	GRANT UPDATE ON %[1]s.runs TO %[2]s;
	GRANT INSERT,UPDATE,DELETE ON %[1]s.event_deliveries,%[1]s.event_receipts,%[1]s.entity_state,%[1]s.timers,%[1]s.agent_sessions,%[1]s.agent_conversation_audits,%[1]s.reply_contexts,%[1]s.activity_attempts TO %[2]s;
	GRANT EXECUTE ON FUNCTION %[1]s.swarm_create_run(UUID,TEXT) TO %[2]s;
	GRANT EXECUTE ON FUNCTION %[1]s.swarm_claim_temporal_runs(UUID[]) TO %[2]s;
	GRANT EXECUTE ON FUNCTION %[1]s.swarm_claim_authorized_run_cleanup(UUID) TO %[2]s;
	GRANT EXECUTE ON FUNCTION %[1]s.swarm_delete_authorized_runs(UUID) TO %[2]s;
	GRANT EXECUTE ON FUNCTION %[1]s.swarm_authorize_run_cleanup(UUID,TEXT,TEXT,UUID[],UUID[]) TO %[3]s;
`
