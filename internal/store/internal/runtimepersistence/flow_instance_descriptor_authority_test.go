package runtimepersistence_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type flowInstanceDescriptorAuthorityStore interface {
	externalStoreTestDurableEventBusStore
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.ActiveFlowInstanceDescriptorLister
}

type dynamicFlowStartupProjectionStore interface {
	InspectDynamicFlowRuntimeStartupProjection(context.Context, runtimecorrelation.BundleSourceFact) (runtimepipeline.DynamicFlowRuntimeStartupProjection, error)
}

func TestDynamicFlowRuntimeStartupProjectionScopesInSQLBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) (dynamicFlowStartupProjectionStore, *sql.DB, bool)
	}{
		{"postgres", func(t *testing.T) (dynamicFlowStartupProjectionStore, *sql.DB, bool) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return storetest.AdmitPostgresRuntimeStore(t, db), db, false
		}},
		{"sqlite", func(t *testing.T) (dynamicFlowStartupProjectionStore, *sql.DB, bool) {
			store := storetest.StartSQLiteRuntimeStore(t)
			return store, storetest.Database(store), true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, db, sqlite := tc.open(t)
			sourceA := mustExternalStoreTestBundleSourceFact()
			hashA, kindA := sourceA.StorageValues()
			hashB := "bundle-v1:sha256:" + strings.Repeat("b", 64)
			sourceB, err := runtimecorrelation.NewEphemeralBundleSourceFact(hashB)
			if err != nil {
				t.Fatal(err)
			}
			_, kindB := sourceB.StorageValues()
			runA, runB := uuid.NewString(), uuid.NewString()
			ctx := testAuthorActivityContext()
			fixtureA := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runA, BundleHash: hashA, BundleSource: kindA}
			fixtureB := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runB, BundleHash: hashB, BundleSource: kindB}
			if sqlite {
				runlifecyclefixture.RequireSQLite(t, ctx, db, fixtureA)
				runlifecyclefixture.RequireSQLite(t, ctx, db, fixtureB)
			} else {
				runlifecyclefixture.RequirePostgres(t, ctx, db, fixtureA)
				runlifecyclefixture.RequirePostgres(t, ctx, db, fixtureB)
			}
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runA, uuid.NewString(), "account/a", hashA, kindA)
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runB, uuid.NewString(), "account/b", hashB, kindB)
			query := `UPDATE flow_instance_runtime_readiness SET topology_ready_at = ? WHERE run_id = ? AND instance_id = ?`
			args := []any{time.Now().UTC(), runA, "account/a"}
			if !sqlite {
				query = `UPDATE flow_instance_runtime_readiness SET topology_ready_at = $1 WHERE run_id = $2::uuid AND instance_id = $3`
			}
			if _, err := db.ExecContext(ctx, query, args...); err != nil {
				t.Fatal(err)
			}
			projection, err := store.InspectDynamicFlowRuntimeStartupProjection(ctx, sourceA)
			if err != nil {
				t.Fatal(err)
			}
			if len(projection.Completed) != 1 || projection.Completed[0].InstancePath != "account/a" || len(projection.Pending) != 0 {
				t.Fatalf("source A projection = %#v", projection)
			}
			deleteQuery := `DELETE FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_id = ?`
			routeQuery := `INSERT INTO routing_rules (event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at) VALUES (?, 'node', 'receiver', ?, 'account', TRUE, 'active', ?)`
			deleteArgs := []any{runB, "account/b"}
			routeArgs := []any{"account/b/event", "account/b", time.Now().UTC()}
			if !sqlite {
				deleteQuery = `DELETE FROM flow_instance_runtime_readiness WHERE run_id = $1::uuid AND instance_id = $2`
				routeQuery = `INSERT INTO routing_rules (event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at) VALUES ($1, 'node', 'receiver', $2, 'account', TRUE, 'active', $3)`
			}
			if _, err := db.ExecContext(ctx, deleteQuery, deleteArgs...); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, routeQuery, routeArgs...); err != nil {
				t.Fatal(err)
			}
			if _, err := store.InspectDynamicFlowRuntimeStartupProjection(ctx, sourceB); err == nil || !strings.Contains(err.Error(), "no dynamic runtime readiness owner") {
				t.Fatalf("invalid source-owned route error = %v", err)
			}
		})
	}
}

func TestActiveFlowInstanceDescriptorAuthorityScopesCensusToExactRunBothStores(t *testing.T) {
	type backendCase struct {
		name  string
		setup func(*testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool)
	}
	backends := []backendCase{
		{
			name: "postgres",
			setup: func(t *testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db, false
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, storetest.Database(selected), true
			},
		},
	}

	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			selected, db, sqlite := backend.setup(t)
			runA, runB := uuid.NewString(), uuid.NewString()
			entityA, entityB := uuid.NewString(), uuid.NewString()
			source := mustExternalStoreTestBundleSourceFact()
			bundleHash, bundleSource := source.StorageValues()
			ctxA := runtimecorrelation.WithRunID(testAuthorActivityContext(), runA)
			ctxB := runtimecorrelation.WithRunID(testAuthorActivityContext(), runB)
			if sqlite {
				runlifecyclefixture.RequireSQLite(t, ctxA, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runA,
					BundleHash: bundleHash, BundleSource: bundleSource,
				})
				runlifecyclefixture.RequireSQLite(t, ctxB, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runB,
					BundleHash: bundleHash, BundleSource: bundleSource,
				})
			} else {
				runlifecyclefixture.RequirePostgres(t, ctxA, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runA,
					BundleHash: bundleHash, BundleSource: bundleSource,
				})
				runlifecyclefixture.RequirePostgres(t, ctxB, db, runlifecyclefixture.Fixture{
					Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runB,
					BundleHash: bundleHash, BundleSource: bundleSource,
				})
			}

			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runA, entityA, "account/a", bundleHash, bundleSource)
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runB, entityB, "account/b", bundleHash, bundleSource)

			for _, check := range []struct {
				ctx  context.Context
				path string
			}{
				{ctx: ctxA, path: "account/a"},
				{ctx: ctxB, path: "account/b"},
			} {
				descriptors, err := selected.ListActiveFlowInstanceDescriptors(check.ctx)
				if err != nil {
					t.Fatalf("ListActiveFlowInstanceDescriptors(%s): %v", check.path, err)
				}
				if len(descriptors) != 1 || descriptors[0].FlowInstance != check.path {
					t.Fatalf("descriptors for %s = %#v, want exact run-owned descriptor", check.path, descriptors)
				}
			}
		})
	}
}

func seedExactFlowInstanceDescriptorOwner(
	t *testing.T,
	db *sql.DB,
	sqlite bool,
	runID, entityID, instancePath, bundleHash, bundleSource string,
) {
	t.Helper()
	readiness := exactFlowInstanceDescriptorReadinessJSON(
		t, runID, bundleHash, bundleSource, "account", instancePath, entityID,
	)
	if sqlite {
		if _, err := db.Exec(`
			INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
			VALUES (?, 'account', 'template', '{}', 'active', CURRENT_TIMESTAMP)
		`, instancePath); err != nil {
			t.Fatalf("seed sqlite flow instance %s: %v", instancePath, err)
		}
		if _, err := db.Exec(`
			INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
			VALUES (?, ?, ?, 'account', 'active', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		`, entityID, runID, instancePath); err != nil {
			t.Fatalf("seed sqlite entity state %s: %v", instancePath, err)
		}
		if _, err := db.Exec(`
			INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
			VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		`, runID, instancePath, readiness); err != nil {
			t.Fatalf("seed sqlite readiness %s: %v", instancePath, err)
		}
		return
	}
	if _, err := db.Exec(`
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'account', 'template', '{}'::jsonb, 'active', NOW())
	`, instancePath); err != nil {
		t.Fatalf("seed postgres flow instance %s: %v", instancePath, err)
	}
	if _, err := db.Exec(`
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES ($1::uuid, $2::uuid, $3, 'account', 'active', '{}'::jsonb, NOW(), NOW())
	`, entityID, runID, instancePath); err != nil {
		t.Fatalf("seed postgres entity state %s: %v", instancePath, err)
	}
	if _, err := db.Exec(`
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
		VALUES ($1::uuid, $2, $3::jsonb, NOW(), NOW())
	`, runID, instancePath, readiness); err != nil {
		t.Fatalf("seed postgres readiness %s: %v", instancePath, err)
	}
}

func TestActiveFlowInstanceDescriptorAuthorityPreservesRoutesOnInvalidProvenanceBothStores(t *testing.T) {
	type backendCase struct {
		name  string
		setup func(*testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool)
	}
	backends := []backendCase{
		{
			name: "postgres",
			setup: func(t *testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db, false
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (flowInstanceDescriptorAuthorityStore, *sql.DB, bool) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, storetest.Database(selected), true
			},
		},
	}
	cases := []struct {
		name                string
		readiness           string
		readinessOnWrongRun bool
		foreignBundle       bool
		wantListError       string
		wantError           string
	}{
		{name: "no readiness row", wantListError: "missing exact readiness plan"},
		{name: "wrong-run readiness row", readiness: "exact", readinessOnWrongRun: true, wantListError: "missing exact readiness plan"},
		{name: "incomplete readiness plan", readiness: `{}`, wantListError: "validate"},
		{name: "stale readiness version", readiness: `{"version":3}`, wantListError: "unsupported version 3"},
		{name: "malformed readiness plan", readiness: `{"workflow_version":{"unexpected":true}}`, wantListError: "decode"},
		{name: "exact readiness owner", readiness: "exact"},
		{name: "foreign run source", readiness: "exact", foreignBundle: true, wantListError: "readiness source does not match persisted run"},
	}

	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					selected, db, sqlite := backend.setup(t)
					runID := uuid.NewString()
					ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
					source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
					planBundleHash, bundleSource := mustExternalStoreTestBundleSourceFact().StorageValues()
					bundleHash := planBundleHash
					if tc.foreignBundle {
						bundleHash = "bundle-v1:sha256:" + strings.Repeat("f", 64)
					}
					wrongRunID := uuid.NewString()
					seedFlowInstanceDescriptorAuthorityCase(
						t,
						ctx,
						db,
						sqlite,
						runID,
						wrongRunID,
						bundleHash,
						bundleSource,
						planBundleHash,
						tc.readiness,
						tc.readinessOnWrongRun,
					)

					descriptors, err := selected.ListActiveFlowInstanceDescriptors(ctx)
					if tc.wantListError != "" {
						if err == nil || !strings.Contains(err.Error(), tc.wantListError) {
							t.Fatalf("ListActiveFlowInstanceDescriptors error = %v, want %q", err, tc.wantListError)
						}
						return
					}
					if err != nil {
						t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
					}
					var testedDescriptor *runtimebus.ActiveFlowInstanceDescriptor
					for idx := range descriptors {
						if descriptors[idx].FlowInstance == "account/existing" {
							testedDescriptor = &descriptors[idx]
							break
						}
					}
					if testedDescriptor == nil {
						t.Fatalf("active descriptors = %#v, want account/existing", descriptors)
					}
					if !testedDescriptor.HasSemanticSource() {
						t.Fatalf("descriptor semantic source = %#v, want exact source", *testedDescriptor)
					}

					identity := runtimeflowidentity.DeriveRoute(notifyallchildren.ChildFlowID, "current")
					prior := runtimebus.FlowInstanceRouteRecord{
						Identity:       identity,
						EventPattern:   identity.InstancePath + "/prior.event",
						SubscriberType: "agent",
						SubscriberID:   "prior-agent",
						SourceFlow:     notifyallchildren.ChildFlowID,
					}
					if err := selected.ReplaceFlowInstanceRouteRecords(ctx, identity, []runtimebus.FlowInstanceRouteRecord{prior}); err != nil {
						t.Fatalf("seed prior exact route set: %v", err)
					}
					before, err := selected.ListFlowInstanceRouteRecords(ctx, identity)
					if err != nil {
						t.Fatalf("read prior exact route set: %v", err)
					}

					eventBus, err := newStoreTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
					if err != nil {
						t.Fatalf("NewEventBusWithOptions: %v", err)
					}
					for _, resolution := range []struct {
						name       string
						localEvent string
					}{
						{name: "select", localEvent: "account.notify.requested"},
						{name: "select-or-create", localEvent: "account.registered"},
					} {
						raw, err := json.Marshal(map[string]any{
							"account_id": "existing",
							"command":    "inspect",
						})
						if err != nil {
							t.Fatalf("marshal %s event: %v", resolution.name, err)
						}
						evt := eventtest.RunCreatingRootIngress(
							uuid.NewString(),
							events.EventType(source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, resolution.localEvent)),
							notifyallchildren.OwnerFlowID,
							"",
							raw,
							0,
							runID,
							"",
							events.EventEnvelope{},
							time.Now().UTC(),
						)
						_, pinErr := eventBus.CheckPublishRecipientPlan(ctx, evt)
						if tc.wantError != "" {
							if pinErr == nil || !strings.Contains(pinErr.Error(), tc.wantError) {
								t.Fatalf("%s source admission error = %v, want %q", resolution.name, pinErr, tc.wantError)
							}
						} else if pinErr != nil {
							t.Fatalf("%s exact source admission: %v", resolution.name, pinErr)
						}
						afterPin, readErr := selected.ListFlowInstanceRouteRecords(ctx, identity)
						if readErr != nil {
							t.Fatalf("read exact route set after %s pin: %v", resolution.name, readErr)
						}
						if !reflect.DeepEqual(afterPin, before) {
							t.Fatalf("%s pin mutated route state: before=%#v after=%#v", resolution.name, before, afterPin)
						}
					}
					err = eventBus.StageFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{
						Identity: identity,
						ActivationVariables: map[string]string{
							"account_id": "current",
						},
					})
					if tc.wantError != "" {
						if err == nil || !strings.Contains(err.Error(), tc.wantError) {
							t.Fatalf("StageFlowInstanceRouteContext error = %v, want %q", err, tc.wantError)
						}
						after, readErr := selected.ListFlowInstanceRouteRecords(ctx, identity)
						if readErr != nil {
							t.Fatalf("read exact route set after rejection: %v", readErr)
						}
						if !reflect.DeepEqual(after, before) {
							t.Fatalf("exact route set changed across provenance rejection: before=%#v after=%#v", before, after)
						}
						return
					}
					if err != nil {
						t.Fatalf("StageFlowInstanceRouteContext exact owner: %v", err)
					}
					after, err := selected.ListFlowInstanceRouteRecords(ctx, identity)
					if err != nil {
						t.Fatalf("read exact route set after replacement: %v", err)
					}
					if len(after) == 0 || reflect.DeepEqual(after, before) {
						t.Fatalf("exact readiness owner did not replace prior route set: before=%#v after=%#v", before, after)
					}
				})
			}
		})
	}
}

func seedFlowInstanceDescriptorAuthorityCase(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	sqlite bool,
	runID string,
	wrongRunID string,
	bundleHash string,
	bundleSource string,
	planBundleHash string,
	readiness string,
	readinessOnWrongRun bool,
) {
	t.Helper()
	const instancePath = "account/existing"
	const stagedInstancePath = "account/current"
	const config = `{"workflow_version":"1.0.0"}`
	entityID := uuid.NewString()
	stagedEntityID := uuid.NewString()
	stagedReadiness := exactFlowInstanceDescriptorReadinessJSON(t, runID, planBundleHash, bundleSource, notifyallchildren.ChildFlowID, stagedInstancePath, stagedEntityID)
	if readiness == "exact" {
		readiness = exactFlowInstanceDescriptorReadinessJSON(t, runID, planBundleHash, bundleSource, notifyallchildren.ChildFlowID, instancePath, entityID)
	}
	if sqlite {
		now := time.Now().UTC()
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
			RunID: runID, StartedAt: now, BundleHash: bundleHash, BundleSource: bundleSource,
		})
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
			RunID: wrongRunID, StartedAt: now, BundleHash: bundleHash, BundleSource: bundleSource,
		})
		if _, err := db.ExecContext(ctx, `
			INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
			VALUES
				(?, ?, 'template', ?, 'active', ?),
				(?, ?, 'template', ?, 'active', ?)
		`, instancePath, notifyallchildren.ChildFlowID, config, now, stagedInstancePath, notifyallchildren.ChildFlowID, config, now); err != nil {
			t.Fatalf("seed sqlite flow instance: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
			VALUES
				(?, ?, ?, 'account', 'active', '{"account_id":"existing"}', ?, ?),
				(?, ?, ?, 'account', 'active', '{"account_id":"current"}', ?, ?)
		`, entityID, runID, instancePath, now, now, stagedEntityID, runID, stagedInstancePath, now, now); err != nil {
			t.Fatalf("seed sqlite entity state: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?)
			`, runID, stagedInstancePath, stagedReadiness, now, now); err != nil {
			t.Fatalf("seed sqlite staged-owner readiness: %v", err)
		}
		if readiness != "" {
			readinessRunID := runID
			if readinessOnWrongRun {
				readinessRunID = wrongRunID
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?)
			`, readinessRunID, instancePath, readiness, now, now); err != nil {
				t.Fatalf("seed sqlite readiness: %v", err)
			}
		}
		return
	}

	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
		RunID: runID, BundleHash: bundleHash, BundleSource: bundleSource,
	})
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
		RunID: wrongRunID, BundleHash: bundleHash, BundleSource: bundleSource,
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES
			($1, $2, 'template', $3::jsonb, 'active', now()),
			($4, $2, 'template', $3::jsonb, 'active', now())
	`, instancePath, notifyallchildren.ChildFlowID, config, stagedInstancePath); err != nil {
		t.Fatalf("seed postgres flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES
			($1::uuid, $2::uuid, $3, 'account', 'active', '{"account_id":"existing"}'::jsonb, now(), now()),
			($4::uuid, $2::uuid, $5, 'account', 'active', '{"account_id":"current"}'::jsonb, now(), now())
	`, entityID, runID, instancePath, stagedEntityID, stagedInstancePath); err != nil {
		t.Fatalf("seed postgres entity state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
			VALUES ($1::uuid, $2, $3::jsonb, now(), now())
		`, runID, stagedInstancePath, stagedReadiness); err != nil {
		t.Fatalf("seed postgres staged-owner readiness: %v", err)
	}
	if readiness != "" {
		readinessRunID := runID
		if readinessOnWrongRun {
			readinessRunID = wrongRunID
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
			VALUES ($1::uuid, $2, $3::jsonb, now(), now())
		`, readinessRunID, instancePath, readiness); err != nil {
			t.Fatalf("seed postgres readiness: %v", err)
		}
	}
}

func exactFlowInstanceDescriptorReadinessJSON(
	t *testing.T,
	runID string,
	bundleHash string,
	bundleSource string,
	templateID string,
	instancePath string,
	entityID string,
) string {
	t.Helper()
	plan, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{
			TemplateID: templateID, ScopeKey: templateID,
			InstanceID: runtimeflowidentity.LogicalInstanceID(instancePath), InstancePath: instancePath,
			EntityID: entityID, HasStoredPath: true,
		},
		RunID: runID, BundleHash: bundleHash, BundleSource: bundleSource, WorkflowVersion: "1.0.0", ExecutionMode: "live",
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize exact descriptor readiness: %v", err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal exact descriptor readiness: %v", err)
	}
	return string(raw)
}
