package store_test

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
				return selected, selected.TestDatabase(), true
			},
		},
	}
	cases := []struct {
		name                 string
		readiness            string
		readinessOnWrongRun  bool
		foreignBundle        bool
		wantDescriptorSource bool
		wantError            string
	}{
		{name: "no readiness row", wantError: "missing exact semantic source"},
		{name: "wrong-run readiness row", readiness: `{"workflow_version":"1.0.0"}`, readinessOnWrongRun: true, wantError: "missing exact semantic source"},
		{name: "missing readiness version", readiness: `{}`, wantError: "missing exact semantic source"},
		{name: "malformed readiness version", readiness: `{"workflow_version":{"unexpected":true}}`, wantDescriptorSource: true, wantError: "semantic source does not match"},
		{name: "exact readiness owner", readiness: `{"workflow_version":"1.0.0"}`, wantDescriptorSource: true},
		{name: "foreign run source", readiness: `{"workflow_version":"1.0.0"}`, foreignBundle: true, wantDescriptorSource: true, wantError: "semantic source does not match"},
	}

	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					selected, db, sqlite := backend.setup(t)
					runID := uuid.NewString()
					ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
					source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
					bundleHash, bundleSource := mustExternalStoreTestBundleSourceFact().StorageValues()
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
						tc.readiness,
						tc.readinessOnWrongRun,
					)

					descriptors, err := selected.ListActiveFlowInstanceDescriptors(ctx)
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
					if got := testedDescriptor.HasSemanticSource(); got != tc.wantDescriptorSource {
						t.Fatalf("descriptor semantic source = %#v, HasSemanticSource=%t want %t", *testedDescriptor, got, tc.wantDescriptorSource)
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
	readiness string,
	readinessOnWrongRun bool,
) {
	t.Helper()
	const instancePath = "account/existing"
	const stagedInstancePath = "account/current"
	const config = `{"workflow_version":"1.0.0"}`
	entityID := uuid.NewString()
	stagedEntityID := uuid.NewString()
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
			VALUES (?, ?, '{"workflow_version":"1.0.0"}', ?, ?)
		`, runID, stagedInstancePath, now, now); err != nil {
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
		VALUES ($1::uuid, $2, '{"workflow_version":"1.0.0"}'::jsonb, now(), now())
	`, runID, stagedInstancePath); err != nil {
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
