package schemastore

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestForkPointSchemaClosedArmsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := bootstrappedForkPointSchemaDB(t, backend)
			ctx := context.Background()
			sourceRunID := uuid.NewString()
			bundleHash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id,bundle_hash,origin_kind) VALUES ($1,$2,'scenario_setup')`, sourceRunID, bundleHash); err != nil {
				t.Fatalf("create source run: %v", err)
			}
			eventID := uuid.NewString()
			if _, err := db.ExecContext(ctx, `INSERT INTO events
				(event_class,event_id,run_id,event_name,scope,payload,payload_bytes,payload_schema_bundle_hash,
				 payload_schema_event_key,payload_schema_digest,payload_schema_class,execution_mode,chain_depth,
				 produced_by,produced_by_type,created_at,routing_source_kind,routing_source_authority,
				 source_route,target_route,target_set,route_settlement)
				VALUES ('root_ingress',$1,$2,'test.received','global','{}',$3,$4,'test.received',$5,
				'schema_less','live',0,'external','external',CURRENT_TIMESTAMP,'external_ingress','test',
				'{"flow_path":"."}','{}','[]','{}')`, eventID, sourceRunID, []byte("{}"), bundleHash, "sha256:"+strings.Repeat("b", 64)); err != nil {
				t.Fatalf("create source event: %v", err)
			}
			insertForkRun := func(kind string, revision int64, eventID any) (string, error) {
				t.Helper()
				runID := uuid.NewString()
				_, err := db.ExecContext(ctx, `INSERT INTO runs
					(run_id,bundle_hash,origin_kind,forked_from_run_id,forked_from_point_kind,forked_from_revision,forked_from_event_id)
					VALUES ($1,$2,'fork_materialization',$3,$4,$5,$6)`, runID, bundleHash, sourceRunID, kind, revision, eventID)
				return runID, err
			}
			eventRunID, err := insertForkRun("event", 1, eventID)
			if err != nil {
				t.Fatalf("event fork lineage refused: %v", err)
			}
			deploymentRunID, err := insertForkRun("deployment_revision", 2, nil)
			if err != nil {
				t.Fatalf("deployment revision lineage refused: %v", err)
			}
			for _, invalid := range []struct {
				name, kind string
				revision   int64
				eventID    any
			}{
				{"event_without_id", "event", 1, nil},
				{"deployment_with_id", "deployment_revision", 1, uuid.NewString()},
				{"zero_revision", "deployment_revision", 0, nil},
				{"unknown_kind", "unknown", 1, nil},
			} {
				t.Run(invalid.name, func(t *testing.T) {
					if _, err := insertForkRun(invalid.kind, invalid.revision, invalid.eventID); err == nil {
						t.Fatal("contradictory fork lineage committed")
					}
				})
			}
			bindingID := uuid.NewString()
			insertBinding := func(id, kind string, eventID any) error {
				t.Helper()
				_, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings
					(binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,fork_event_id,mode)
					VALUES ($1,$2,$3,$4,2,$5,'selected_contracts')`, id, deploymentRunID, sourceRunID, kind, eventID)
				return err
			}
			if err := insertBinding(uuid.NewString(), "event", nil); err == nil {
				t.Fatal("event binding without fork event committed")
			}
			if err := insertBinding(bindingID, "deployment_revision", nil); err != nil {
				t.Fatalf("deployment binding refused: %v", err)
			}
			insertOperation := func(id, kind string, eventID any) error {
				t.Helper()
				_, err := db.ExecContext(ctx, `INSERT INTO run_fork_operations
					(operation_id,actor,transport_hash,semantic_hash,request_json,source_run_id,fork_point_kind,fork_revision,fork_event_id,fork_run_id,selected_binding_id,state)
					VALUES ($1,'actor','transport','semantic','{}',$2,$3,2,$4,$5,$6,'materialized')`, id, sourceRunID, kind, eventID, deploymentRunID, bindingID)
				return err
			}
			if err := insertOperation(uuid.NewString(), "event", nil); err == nil {
				t.Fatal("event operation without fork event committed")
			}
			if err := insertOperation(uuid.NewString(), "deployment_revision", nil); err != nil {
				t.Fatalf("deployment operation refused: %v", err)
			}
			fingerprint := "sha256:" + strings.Repeat("a", 64)
			insertExecution := func(kind string, eventID any) error {
				t.Helper()
				_, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_runtime_executions
					(execution_id,fork_run_id,source_run_id,binding_id,fork_point_kind,fork_revision,fork_event_id,generation,
					 executable_coordinate_fingerprint,admission_fingerprint,container_plan_fingerprint,actor_census_fingerprint,
					 effective_config_fingerprint,declaration_plan_fingerprint,declaration_plan,preparation_fingerprint,preparation_binding,state,terminal_at)
					VALUES ($1,$2,$3,$4,$5,2,$6,1,'executable','admission','container','actors','config',$7,'{}',$7,'{}','failed',CURRENT_TIMESTAMP)`,
					uuid.NewString(), deploymentRunID, sourceRunID, bindingID, kind, eventID, fingerprint)
				return err
			}
			if err := insertExecution("event", nil); err == nil {
				t.Fatal("event runtime execution without fork event committed")
			}
			if err := insertExecution("deployment_revision", nil); err != nil {
				t.Fatalf("deployment runtime execution refused: %v", err)
			}
			eventBindingID := uuid.NewString()
			if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings
				(binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,fork_event_id,mode)
				VALUES ($1,$2,$3,'event',1,$4,'selected_contracts')`, eventBindingID, eventRunID, sourceRunID, eventID); err != nil {
				t.Fatalf("event binding refused: %v", err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_operations
				(operation_id,actor,transport_hash,semantic_hash,request_json,source_run_id,fork_point_kind,fork_revision,fork_event_id,fork_run_id,selected_binding_id,state)
				VALUES ($1,'actor','event-transport','event-semantic','{}',$2,'event',1,$3,$4,$5,'materialized')`,
				uuid.NewString(), sourceRunID, eventID, eventRunID, eventBindingID); err != nil {
				t.Fatalf("event operation refused: %v", err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_runtime_executions
				(execution_id,fork_run_id,source_run_id,binding_id,fork_point_kind,fork_revision,fork_event_id,generation,
				 executable_coordinate_fingerprint,admission_fingerprint,container_plan_fingerprint,actor_census_fingerprint,
				 effective_config_fingerprint,declaration_plan_fingerprint,declaration_plan,preparation_fingerprint,preparation_binding,state,terminal_at)
				VALUES ($1,$2,$3,$4,'event',1,$5,1,'event-executable','event-admission','event-container','event-actors','event-config',$6,'{}',$6,'{}','failed',CURRENT_TIMESTAMP)`,
				uuid.NewString(), eventRunID, sourceRunID, eventBindingID, eventID, fingerprint); err != nil {
				t.Fatalf("event runtime execution refused: %v", err)
			}
			insertRoute := func(forkRunID, kind string, revision int64, forkEventID any) error {
				t.Helper()
				_, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_route_recoveries
					(fork_run_id,source_run_id,fork_point_kind,fork_revision,fork_event_id,
					 owner,runtime_recovery_owner,mode,route_topology_owner,recipient_planning_owner,
					 frontier_evidence_fingerprint,route_topology_fingerprint,recipient_planning_fingerprint,
					 route_topology,recipient_planning)
					VALUES ($1,$2,$3,$4,$5,'route-owner','runtime-owner','selected_contracts',
					 'topology-owner','planning-owner','frontier','topology','planning','{}','{}')`,
					forkRunID, sourceRunID, kind, revision, forkEventID)
				return err
			}
			for _, invalid := range []struct {
				kind     string
				revision int64
				eventID  any
			}{
				{"event", 1, nil},
				{"deployment_revision", 2, eventID},
				{"deployment_revision", 0, nil},
				{"unknown", 2, nil},
			} {
				if err := insertRoute(deploymentRunID, invalid.kind, invalid.revision, invalid.eventID); err == nil {
					t.Fatalf("contradictory route recovery point committed: %+v", invalid)
				}
			}
			if err := insertRoute(deploymentRunID, "deployment_revision", 2, nil); err != nil {
				t.Fatalf("deployment route recovery refused: %v", err)
			}
			if err := insertRoute(eventRunID, "event", 1, eventID); err != nil {
				t.Fatalf("event route recovery refused: %v", err)
			}
			for _, route := range []struct {
				runID, kind, eventID string
				revision             int64
			}{
				{deploymentRunID, "deployment_revision", "", 2},
				{eventRunID, "event", eventID, 1},
			} {
				var kind, recoveredEventID string
				var revision int64
				if err := db.QueryRowContext(ctx, `SELECT fork_point_kind,fork_revision,COALESCE(CAST(fork_event_id AS TEXT),'')
					FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id=$1`, route.runID).
					Scan(&kind, &revision, &recoveredEventID); err != nil {
					t.Fatal(err)
				}
				if kind != route.kind || revision != route.revision || recoveredEventID != route.eventID {
					t.Fatalf("route recovery point = (%s,%d,%s), want %+v", kind, revision, recoveredEventID, route)
				}
			}
		})
	}
}

func bootstrappedForkPointSchemaDB(t *testing.T, backend string) *sql.DB {
	t.Helper()
	ctx := context.Background()
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	request := SchemaBootstrapRequest{PlatformPlans: plans, Origin: RuntimeStoreOrigin{
		SwarmVersion: "fork-point-schema-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}
	if backend == "postgres" {
		_, db, cleanup := testutil.StartEmptyPostgres(t)
		t.Cleanup(cleanup)
		owner, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		store, err := NewPostgres(owner)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.BootstrapSchema(ctx, request); err != nil {
			t.Fatal(err)
		}
		return db
	}
	store, err := NewSQLite(filepath.Join(t.TempDir(), ".swarm", "dev.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.BootstrapSchema(ctx, request); err != nil {
		t.Fatal(err)
	}
	return store.backend.ConstructionHandle()
}
