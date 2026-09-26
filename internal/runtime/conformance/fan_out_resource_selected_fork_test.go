package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const selectedDeploymentEvent = "root.ready"

func selectedDeploymentVersion(t *testing.T, f *deploymentResourceFixture, rows []byte) durabledata.CompiledVersion {
	t.Helper()
	ref, err := durabledata.ParseDeclarationRef(".", selectedDeploymentEvent)
	if err != nil {
		t.Fatal(err)
	}
	bundle, ok := semanticview.Bundle(f.source)
	if !ok {
		t.Fatal("selected deployment source is not bundle-backed")
	}
	declaration, ok := bundle.DurableDataDeclarationByRef(ref)
	if !ok || declaration.BusinessKey != "account_id" {
		t.Fatalf("selected deployment declaration = %+v", declaration)
	}
	version, defects := durabledata.CompileJSONL(ref, declaration.Schema, declaration.BusinessKey, rows)
	if len(defects) != 0 {
		t.Fatalf("selected deployment rows rejected: %+v", defects)
	}
	return version
}

func selectedDeploymentResourceFixture(t *testing.T, backend, route string) *deploymentResourceFixture {
	return selectedDeploymentResourceFixtureWithProbe(t, backend, route, nil)
}

func selectedDeploymentResourceFixtureKeyless(t *testing.T, backend, route string) *deploymentResourceFixture {
	return selectedDeploymentResourceFixtureConfigured(t, backend, route, false, nil, nil, nil)
}

func selectedDeploymentResourceFixtureWithProbe(t *testing.T, backend, route string, probe runtimelifecycleprobe.Observer) *deploymentResourceFixture {
	return selectedDeploymentResourceFixtureWithDelivery(t, backend, route, probe, nil, nil)
}

func selectedDeploymentResourceFixtureWithDelivery(t *testing.T, backend, route string, probe runtimelifecycleprobe.Observer, wrapDelivery func(runtimedelivery.Store) runtimedelivery.Store, wrapExecutor func(startupownership.FanOutExecutor) startupownership.FanOutExecutor) *deploymentResourceFixture {
	return selectedDeploymentResourceFixtureConfigured(t, backend, route, true, probe, wrapDelivery, wrapExecutor)
}

func selectedDeploymentResourceFixtureConfigured(t *testing.T, backend, route string, keyed bool, probe runtimelifecycleprobe.Observer, wrapDelivery func(runtimedelivery.Store) runtimedelivery.Store, wrapExecutor func(startupownership.FanOutExecutor) startupownership.FanOutExecutor) *deploymentResourceFixture {
	t.Helper()
	root := canonicalrouting.CopySelectedDeploymentResource(t, route, keyed)
	repo := conformanceRepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load supported %s deployment route: %v", route, err)
	}
	f := &deploymentResourceFixture{source: semanticview.Wrap(bundle)}
	switch backend {
	case "sqlite":
		selected := storetest.StartSQLiteRuntimeStore(t)
		f.selected, f.db = selected, storetest.DatabaseForTest(selected)
	case "postgres":
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		f.selected, f.db = storetest.AdmitPostgresRuntimeStore(t, db), db
	default:
		t.Fatalf("unknown selected backend %q", backend)
	}
	fact := conformanceSourceArtifactFact(t, f.source)
	f.ctx = testAuthorActivityContextForBundle(context.Background(), fact)
	f.topology = newNotifyAllChildrenProcessTopology(t, f.ctx, f.selected, f.source)
	if probe == nil && wrapDelivery == nil && wrapExecutor == nil {
		f.boot(t)
	} else {
		var deliveryLifecycle runtimedelivery.Store
		if wrapDelivery != nil {
			deliveryLifecycle = wrapDelivery(f.selected)
		}
		f.runtime = newNotifyAllChildrenRuntime(t, f.selected, f.db, f.source, time.Now, notifyAllChildrenRuntimeOptions{
			processTopology: f.topology, testLifecycleProbe: probe, deliveryLifecycle: deliveryLifecycle,
			fanOutExecutor: func(coordinator *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
				executor := startupownership.FanOutExecutor(deploymentFanOutDiagnostic{FanOutExecutor: coordinator, t: t})
				if wrapExecutor != nil {
					executor = wrapExecutor(executor)
				}
				return executor
			},
		})
		if err := f.runtime.manager.Run(managedConformanceExecutionContextForBundle(t, f.ctx, fmt.Sprintf("deployment-resource-%d", time.Now().UnixNano()), f.runtime.sourceArtifactFact)); err != nil {
			t.Fatalf("normal deployment resource boot: %v", err)
		}
	}
	return f
}

// Keep the selected executor and durable operation reader real. Rebuilding the
// HTTP handler simulates losing its response/cache without replacing the store.
func deploymentForkServer(t *testing.T, f *deploymentResourceFixture, owner runforkexecution.SelectedContractExecutionOwner) *httptest.Server {
	return deploymentForkServerAt(t, f, owner, time.Now)
}

func deploymentForkServerAt(t *testing.T, f *deploymentResourceFixture, owner runforkexecution.SelectedContractExecutionOwner, now func() time.Time) *httptest.Server {
	t.Helper()
	availability, ok := f.selected.(apiv1.RunForkAvailabilityStore)
	if !ok {
		t.Fatalf("selected store %T lacks fork availability", f.selected)
	}
	operations, ok := f.selected.(apiv1.RunForkOperationReader)
	if !ok {
		t.Fatalf("selected store %T lacks fork operations", f.selected)
	}
	artifacts, ok := f.selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)
	if !ok {
		t.Fatalf("selected store %T lacks source artifact loader", f.selected)
	}
	loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{
		RepoRoot:         conformanceRepoRoot(t),
		PlatformSpecPath: filepath.Join(conformanceRepoRoot(t), "platform-spec.yaml"),
		Store:            artifacts,
	}
	methods := apiv1.OperatorRunForkHandlers(apiv1.RunForkHandlerOptions{
		Now:          now,
		Availability: availability, Operations: operations,
		Executor: apiv1.SelectedContractRunForkExecutor{
			ExecuteSelectedContractRunFork: func(ctx context.Context, req runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
				req.Owner = owner
				result, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, req)
				if err != nil {
					t.Logf("selected fork executor diagnosis: %v", err)
					logSelectedDeploymentForkFailure(t, f.db, result.Materialization.ForkRunID)
				}
				return result, err
			},
			SourceLoader: loader,
			AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{
				ExecutionPosture:  executionposture.Live,
				ProcessCapability: f.topology.capability,
			},
		},
	})
	handler, err := apiv1.NewHandler(apiv1.Options{
		PlatformSpecPath: filepath.Join(conformanceRepoRoot(t), "platform-spec.yaml"),
		AuthTokens:       []string{apiv1.DefaultLoopbackAPIToken},
		ProcessWorkOwner: conformanceTestProcessOwner(t),
		Handlers:         methods,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := authoractivity.WithScope(r.Context(), authoractivity.RuntimeScope(authorActivityTestRuntimeInstanceID))
		handler.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(server.Close)
	return server
}

func logSelectedDeploymentForkFailure(t *testing.T, db *sql.DB, forkRunID string) {
	t.Helper()
	if forkRunID == "" {
		t.Log("selected fork diagnostic: no acknowledged materialization")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var runStatus, controlStatus string
	if err := db.QueryRowContext(ctx, `SELECT r.status,c.control_status FROM runs r JOIN run_control_state c ON c.run_id=r.run_id WHERE r.run_id=$1`, forkRunID).Scan(&runStatus, &controlStatus); err != nil {
		t.Logf("selected fork diagnostic run/control query: %v", err)
	} else {
		t.Logf("selected fork diagnostic run=%s status=%s control=%s", forkRunID, runStatus, controlStatus)
	}
	feeds, err := db.QueryContext(ctx, `SELECT CAST(deployment_feed_id AS TEXT),status,cursor,cardinality,CAST(claim_owner AS TEXT),claim_generation,CAST(lease_expires_at AS TEXT),CAST(retry_ready_at AS TEXT),CAST(retry_failure AS TEXT),CAST(blocked_reason AS TEXT) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' ORDER BY deployment_feed_id`, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic feeds query: %v", err)
	} else {
		for feeds.Next() {
			var feedID, status string
			var cursor, cardinality, generation int64
			var claimant, lease, retryAt, retryFailure, blocked sql.NullString
			if err := feeds.Scan(&feedID, &status, &cursor, &cardinality, &claimant, &generation, &lease, &retryAt, &retryFailure, &blocked); err != nil {
				t.Logf("selected fork diagnostic feed scan: %v", err)
				break
			}
			t.Logf("selected fork diagnostic feed=%s status=%s cursor=%d/%d claim=%q generation=%d lease=%q retry_at=%q retry_failure=%q blocked=%q", feedID, status, cursor, cardinality, claimant.String, generation, lease.String, retryAt.String, retryFailure.String, blocked.String)
		}
		if err := feeds.Err(); err != nil {
			t.Logf("selected fork diagnostic feeds rows: %v", err)
		}
		feeds.Close()
	}
	executions, err := db.QueryContext(ctx, `SELECT CAST(execution_id AS TEXT),generation,state,CAST(lease_expires_at AS TEXT) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation`, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic executions query: %v", err)
	} else {
		for executions.Next() {
			var executionID, state string
			var generation int64
			var lease sql.NullString
			if err := executions.Scan(&executionID, &generation, &state, &lease); err != nil {
				t.Logf("selected fork diagnostic execution scan: %v", err)
				break
			}
			t.Logf("selected fork diagnostic execution=%s generation=%d state=%s lease=%q", executionID, generation, state, lease.String)
		}
		if err := executions.Err(); err != nil {
			t.Logf("selected fork diagnostic executions rows: %v", err)
		}
		executions.Close()
	}
	for _, check := range []struct {
		name  string
		query string
	}{
		{"outcomes", `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`},
		{"events", `SELECT COUNT(*) FROM events WHERE run_id=$1`},
		{"pipeline_receipts", `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'`},
		{"deliveries", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`},
		{"pending_deliveries", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress')`},
		{"failed_deliveries", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('failed','dead_letter')`},
		{"stamped_handoffs", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND continuation_handoff_at IS NOT NULL`},
		{"receiver_instances", `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`},
	} {
		var count int
		if err := db.QueryRowContext(ctx, check.query, forkRunID).Scan(&count); err != nil {
			t.Logf("selected fork diagnostic %s query: %v", check.name, err)
		} else {
			t.Logf("selected fork diagnostic %s=%d", check.name, count)
		}
	}
	receipts, err := db.QueryContext(ctx, `SELECT CAST(r.event_id AS TEXT),r.subscriber_type,r.subscriber_id,r.outcome,CAST(r.reason_code AS TEXT),CAST(r.failure AS TEXT) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 ORDER BY e.insertion_sequence,r.subscriber_type,r.subscriber_id`, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic receipts query: %v", err)
	} else {
		for receipts.Next() {
			var eventID, subscriberType, subscriberID, outcome string
			var reason, failure sql.NullString
			if err := receipts.Scan(&eventID, &subscriberType, &subscriberID, &outcome, &reason, &failure); err != nil {
				t.Logf("selected fork diagnostic receipt scan: %v", err)
				break
			}
			t.Logf("selected fork diagnostic receipt event=%s subscriber=%s/%s outcome=%s reason=%q failure=%s", eventID, subscriberType, subscriberID, outcome, reason.String, failure.String)
		}
		if err := receipts.Err(); err != nil {
			t.Logf("selected fork diagnostic receipts rows: %v", err)
		}
		receipts.Close()
	}
	deliveries, err := db.QueryContext(ctx, `SELECT CAST(delivery_id AS TEXT),CAST(event_id AS TEXT),subscriber_type,subscriber_id,status,retry_count,claim_version,CAST(next_eligible_at AS TEXT),CAST(continuation_handoff_at AS TEXT),CAST(reason_code AS TEXT),CAST(failure AS TEXT) FROM event_deliveries WHERE run_id=$1 ORDER BY created_at,delivery_id`, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic deliveries query: %v", err)
	} else {
		for deliveries.Next() {
			var deliveryID, eventID, subscriberType, subscriberID, status string
			var retryCount, claimVersion int64
			var nextEligible, handoff, reason, failure sql.NullString
			if err := deliveries.Scan(&deliveryID, &eventID, &subscriberType, &subscriberID, &status, &retryCount, &claimVersion, &nextEligible, &handoff, &reason, &failure); err != nil {
				t.Logf("selected fork diagnostic delivery scan: %v", err)
				break
			}
			t.Logf("selected fork diagnostic delivery=%s event=%s subscriber=%s/%s status=%s retries=%d claim_version=%d next_eligible=%q handoff=%q reason=%q failure=%s", deliveryID, eventID, subscriberType, subscriberID, status, retryCount, claimVersion, nextEligible.String, handoff.String, reason.String, failure.String)
		}
		if err := deliveries.Err(); err != nil {
			t.Logf("selected fork diagnostic deliveries rows: %v", err)
		}
		deliveries.Close()
	}
	attempts, err := db.QueryContext(ctx, `SELECT CAST(a.delivery_id AS TEXT),a.claim_version,a.closure_kind,CAST(a.outcome AS TEXT),CAST(a.reason_code AS TEXT),CAST(a.failure AS TEXT) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 ORDER BY a.delivery_id,a.claim_version`, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic attempts query: %v", err)
	} else {
		for attempts.Next() {
			var deliveryID, closure string
			var claimVersion int64
			var outcome, reason, failure sql.NullString
			if err := attempts.Scan(&deliveryID, &claimVersion, &closure, &outcome, &reason, &failure); err != nil {
				t.Logf("selected fork diagnostic attempt scan: %v", err)
				break
			}
			t.Logf("selected fork diagnostic attempt delivery=%s claim_version=%d closure=%s outcome=%q reason=%q failure=%s", deliveryID, claimVersion, closure, outcome.String, reason.String, failure.String)
		}
		if err := attempts.Err(); err != nil {
			t.Logf("selected fork diagnostic attempts rows: %v", err)
		}
		attempts.Close()
	}
	logs, err := db.QueryContext(ctx, `SELECT CAST(event_id AS TEXT),CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name='platform.runtime_log' ORDER BY insertion_sequence DESC LIMIT 20`, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic runtime logs query: %v", err)
	} else {
		for logs.Next() {
			var eventID, payload string
			if err := logs.Scan(&eventID, &payload); err != nil {
				t.Logf("selected fork diagnostic runtime log scan: %v", err)
				break
			}
			t.Logf("selected fork diagnostic runtime_log event=%s payload=%s", eventID, payload)
		}
		if err := logs.Err(); err != nil {
			t.Logf("selected fork diagnostic runtime logs rows: %v", err)
		}
		logs.Close()
	}
}

func waitDeploymentForkRowEvent(t *testing.T, ctx context.Context, db *sql.DB, runID string, wake func()) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var eventID string
		err := db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='root.ready' ORDER BY event_id LIMIT 1`, runID).Scan(&eventID)
		if err == nil {
			return eventID
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var intents, cursor, outcomes, events, deliveries, receiverInstances int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(cursor),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&intents, &cursor); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, runID).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, runID).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, runID).Scan(&receiverInstances); err != nil {
		t.Fatal(err)
	}
	var status, intentBundle, origin string
	var sourceKind, feedID, blocked, claimant, retry sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status,bundle_hash,origin_kind,source_kind,deployment_feed_id,blocked_reason,claim_owner,retry_ready_at FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&status, &intentBundle, &origin, &sourceKind, &feedID, &blocked, &claimant, &retry); err != nil {
		t.Fatal(err)
	}
	wake()
	time.Sleep(2 * time.Second)
	var afterWake int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='root.ready'`, runID).Scan(&afterWake); err != nil {
		t.Fatal(err)
	}
	var runStatus, bundleHash string
	if err := db.QueryRowContext(ctx, `SELECT status,bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&runStatus, &bundleHash); err != nil {
		t.Fatal(err)
	}
	var grants int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generation_grants WHERE bundle_hash=$1 AND state='admitted' AND selected_binding_id IS NULL`, bundleHash).Scan(&grants); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("deployment run %s never published its resource event without manual wake: intents=%d cursor=%d outcomes=%d events=%d deliveries=%d receiver_instances=%d intent_status=%s run_status=%s matching_bundle=%t origin=%s source=%+v feed=%+v grants=%d blocked=%+v claimant=%+v retry=%+v after_manual_wake=%d", runID, intents, cursor, outcomes, events, deliveries, receiverInstances, status, runStatus, intentBundle == bundleHash, origin, sourceKind, feedID, grants, blocked, claimant, retry, afterWake)
	return ""
}

func deploymentForkOwner(t *testing.T, f *deploymentResourceFixture) (runforkexecution.SelectedContractExecutionOwner, []runfork.SelectedForkRecoveryResult) {
	return deploymentForkOwnerWithOverrides(t, f, nil, nil)
}

func deploymentForkOwnerWithDelivery(t *testing.T, f *deploymentResourceFixture, delivery runtimedelivery.Store) (runforkexecution.SelectedContractExecutionOwner, []runfork.SelectedForkRecoveryResult) {
	return deploymentForkOwnerWithOverrides(t, f, delivery, nil)
}

func deploymentForkOwnerWithOverrides(t *testing.T, f *deploymentResourceFixture, delivery runtimedelivery.Store, fork runforkexecution.SelectedContractForkLifecycle) (runforkexecution.SelectedContractExecutionOwner, []runfork.SelectedForkRecoveryResult) {
	t.Helper()
	if delivery == nil {
		delivery = f.selected
	}
	durable := bus.DurableDependencies{
		ReplyContext: f.selected, RunLifecycle: f.selected, DeliveryLifecycle: delivery,
		FlowRoutes: f.selected, FlowRouteRecords: f.selected, FlowRouteSets: f.selected,
		FlowRouteTopology: f.selected, FlowRouteRollback: f.selected,
		ActiveAgents: f.selected, ActiveFlows: f.selected, TargetOwners: f.selected,
		PreparedEvents: f.selected, TargetFailureRecorder: f.selected,
		RunOrigins: f.selected, StandingRestarts: f.selected,
	}
	var owner runforkexecution.SelectedContractExecutionOwner
	var err error
	switch selected := f.selected.(type) {
	case *store.SQLiteRuntimeStore:
		forkOwner := fork
		if forkOwner == nil {
			forkOwner = selected
		}
		roles := manager.PersistenceRoles{
			LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected,
			EffectsRecovery: selected, DeliveryQuiescence: selected, EventExistence: selected,
			DirectiveOperations: selected, DirectiveTargets: selected, FlowRoutes: selected,
			StandingRestarts: selected,
		}
		owner, err = runforkexecution.NewSelectedContractExecutionOwner(
			pipeline.NewWorkflowPersistence(selected), forkOwner, selected, selected, selected,
			durable, selected.PipelineObligations(), selected, roles,
			selected, selected, selected, selected, selected, selected, selected,
			selected, selected, selected, selected, selected,
		)
	case *store.PostgresStore:
		forkOwner := fork
		if forkOwner == nil {
			forkOwner = selected
		}
		roles := manager.PersistenceRoles{
			LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected,
			EffectsRecovery: selected, DeliveryQuiescence: selected, EventExistence: selected,
			DirectiveOperations: selected, DirectiveTargets: selected, FlowRoutes: selected,
			StandingRestarts: selected,
		}
		owner, err = runforkexecution.NewSelectedContractExecutionOwner(
			pipeline.NewWorkflowPersistence(selected), forkOwner, selected, selected, selected,
			durable, selected.PipelineObligations(), selected, roles,
			selected, selected, selected, selected, selected, selected, selected,
			selected, selected, selected, selected, selected,
		)
	default:
		t.Fatalf("selected fork store %T is not supported", f.selected)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.BindSelectedProcess(f.ctx, conformanceTestProcessOwner(t), f.topology.capability); err != nil {
		t.Fatal(err)
	}
	artifacts, ok := f.selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)
	if !ok {
		t.Fatalf("selected store %T lacks source artifact loader", f.selected)
	}
	recovered, err := owner.RecoverSelectedForkContexts(f.ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), runforkexecution.SelectedForkRecoveryEnvironment{
		SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{
			RepoRoot: conformanceRepoRoot(t), PlatformSpecPath: filepath.Join(conformanceRepoRoot(t), "platform-spec.yaml"), Store: artifacts,
		},
		AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{
			ExecutionPosture: executionposture.Live, ProcessCapability: f.topology.capability,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.RetireSelectedContexts(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return owner, recovered
}

func deploymentForkRPC(t *testing.T, ctx context.Context, server *httptest.Server, params map[string]any) (apiv1.RunForkExecutionResult, json.RawMessage) {
	t.Helper()
	result, rpcErr, err := deploymentForkRPCRequest(ctx, server, params)
	if err != nil {
		t.Fatal(err)
	}
	return result, rpcErr
}

func deploymentForkRPCRequest(ctx context.Context, server *httptest.Server, params map[string]any) (apiv1.RunForkExecutionResult, json.RawMessage, error) {
	encoded, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "run.fork", "params": params})
	if err != nil {
		return apiv1.RunForkExecutionResult{}, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/rpc", bytes.NewReader(encoded))
	if err != nil {
		return apiv1.RunForkExecutionResult{}, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return apiv1.RunForkExecutionResult{}, nil, err
	}
	defer response.Body.Close()
	var envelope struct {
		Result apiv1.RunForkExecutionResult `json:"result"`
		Error  json.RawMessage              `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		return apiv1.RunForkExecutionResult{}, nil, err
	}
	if response.StatusCode != http.StatusOK {
		return apiv1.RunForkExecutionResult{}, envelope.Error, fmt.Errorf("run.fork HTTP status=%d error=%s", response.StatusCode, envelope.Error)
	}
	return envelope.Result, envelope.Error, nil
}

func selectedDeploymentPublicEvents(t *testing.T, ctx context.Context, server *httptest.Server, runID string) operatorread.OperatorEventListResult {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "event.list",
		"params": map[string]any{"filter": map[string]any{"run_id": runID, "event_name": selectedDeploymentEvent}, "limit": 200},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/rpc", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope struct {
		Result operatorread.OperatorEventListResult `json:"result"`
		Error  json.RawMessage                      `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(envelope.Error) != 0 {
		t.Fatalf("selected deployment event.list status=%d error=%s", response.StatusCode, envelope.Error)
	}
	return envelope.Result
}

type selectedDeploymentOperationSnapshot struct {
	id, state, result, updatedAt string
}

func selectedDeploymentForkOperationSnapshot(t *testing.T, f *deploymentResourceFixture, forkRunID string) selectedDeploymentOperationSnapshot {
	t.Helper()
	var snapshot selectedDeploymentOperationSnapshot
	if err := f.db.QueryRowContext(f.ctx, `SELECT CAST(operation_id AS TEXT),state,CAST(result_json AS TEXT),CAST(updated_at AS TEXT) FROM run_fork_operations WHERE fork_run_id=$1`, forkRunID).Scan(&snapshot.id, &snapshot.state, &snapshot.result, &snapshot.updatedAt); err != nil {
		t.Fatal(err)
	}
	if snapshot.id == "" || snapshot.state != "activated" || snapshot.result == "" || snapshot.updatedAt == "" {
		t.Fatalf("selected fork lacks exact durable activated operation: %+v", snapshot)
	}
	return snapshot
}

func assertSelectedDeploymentRows(t *testing.T, f *deploymentResourceFixture, server *httptest.Server, runID, route string, compiledVersion string, rows []byte) {
	assertSelectedDeploymentRowsWithClosedExecutions(t, f, server, runID, route, compiledVersion, rows, 1)
}

func assertSelectedDeploymentRowsWithClosedExecutions(t *testing.T, f *deploymentResourceFixture, server *httptest.Server, runID, route string, compiledVersion string, rows []byte, wantClosed int) {
	t.Helper()
	defer func() {
		if t.Failed() {
			logDeploymentResourceFailure(t, f.db, runID)
		}
	}()
	waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 3*time.Minute)
	var pin, intentVersion string
	if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='.' AND event_name='root.ready'`, runID).Scan(&pin); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRowContext(f.ctx, `SELECT source_resource_version_id FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&intentVersion); err != nil {
		t.Fatal(err)
	}
	if pin != compiledVersion || intentVersion != pin {
		t.Fatalf("%s selected route changed pinned data: pin=%s intent=%s want=%s", route, pin, intentVersion, compiledVersion)
	}
	var events, delivered, committed, pipelineReceipts, stampedHandoffs int
	for _, check := range []struct {
		query string
		value *int
	}{
		{`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='root.ready'`, &events},
		{`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='root.ready' AND d.status='delivered'`, &delivered},
		{`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, &committed},
		{`SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND e.event_name='root.ready' AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' AND r.outcome='success'`, &pipelineReceipts},
		{`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='root.ready' AND d.continuation_handoff_at IS NOT NULL`, &stampedHandoffs},
	} {
		if err := f.db.QueryRowContext(f.ctx, check.query, runID).Scan(check.value); err != nil {
			t.Fatal(err)
		}
	}
	if pipelineReceipts == 0 || stampedHandoffs == 0 {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && (pipelineReceipts == 0 || stampedHandoffs == 0) {
			time.Sleep(25 * time.Millisecond)
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND e.event_name='root.ready' AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' AND r.outcome='success'`, runID).Scan(&pipelineReceipts); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='root.ready' AND d.continuation_handoff_at IS NOT NULL`, runID).Scan(&stampedHandoffs); err != nil {
				t.Fatal(err)
			}
		}
	}
	if events != 1 || delivered != 1 || committed != 1 || pipelineReceipts != 1 || stampedHandoffs != 1 {
		t.Fatalf("%s selected route did not settle: events=%d delivered=%d committed=%d pipeline_receipts=%d stamped_handoffs=%d", route, events, delivered, committed, pipelineReceipts, stampedHandoffs)
	}
	var parentRunID sql.NullString
	if err := f.db.QueryRowContext(f.ctx, `SELECT CAST(forked_from_run_id AS TEXT) FROM runs WHERE run_id=$1`, runID).Scan(&parentRunID); err != nil {
		t.Fatal(err)
	}
	if parentRunID.Valid {
		var closed int
		if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 AND state='closed'`, runID).Scan(&closed); err != nil {
			t.Fatal(err)
		}
		if closed != wantClosed {
			t.Fatalf("%s selected child has %d closed executions after settled public result, want %d", route, closed, wantClosed)
		}
	}
	var want map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(rows), &want); err != nil {
		t.Fatal(err)
	}
	page := selectedDeploymentPublicEvents(t, f.ctx, server, runID)
	if len(page.Events) != 1 || page.NextCursor != "" || !reflect.DeepEqual(page.Events[0].Payload, want) || len(page.Events[0].Deliveries) != 1 || page.Events[0].Deliveries[0].Status != "delivered" {
		t.Fatalf("%s selected route lost document-shaped public readback: %+v want=%+v", route, page, want)
	}
	wantSubscriber := conformanceNode(t, "", "root-collector").Key()
	if route == "singleton" {
		wantSubscriber = conformanceNode(t, "consumer", "consumer-node").Key()
	}
	if page.Events[0].Deliveries[0].SubscriberID != wantSubscriber {
		t.Fatalf("%s selected route reached subscriber %q, want %q", route, page.Events[0].Deliveries[0].SubscriberID, wantSubscriber)
	}
}

func TestDeploymentSourceChangedPinPublicForkAndLostResponse(t *testing.T) {
	for _, route := range []string{"root", "singleton"} {
		t.Run(route, func(t *testing.T) {
			for _, backend := range []string{"sqlite", "postgres"} {
				t.Run(backend, func(t *testing.T) {
					f := selectedDeploymentResourceFixture(t, backend, route)
					server := f.operatorServer(t)
					originalRows := []byte("{\"account_id\":\"original\",\"document\":{\"nested\":[1,{\"value\":2}]}}\n")
					original := selectedDeploymentVersion(t, f, originalRows)
					originalPath := filepath.Join(t.TempDir(), "original.jsonl")
					if err := os.WriteFile(originalPath, originalRows, 0o600); err != nil {
						t.Fatal(err)
					}
					sourceRunID := startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+originalPath)
					forkEventID := waitDeploymentForkRowEvent(t, f.ctx, f.db, sourceRunID, f.runtime.fanOutServing.Wake)
					assertSelectedDeploymentRows(t, f, server, sourceRunID, route, string(original.VersionID), originalRows)
					page := selectedDeploymentPublicEvents(t, f.ctx, server, sourceRunID)
					if len(page.Events) != 1 || page.Events[0].EventID != forkEventID {
						t.Fatalf("public source event disagrees with delivered fork point: %+v point=%s", page.Events, forkEventID)
					}

					changedRows := []byte("{\"account_id\":\"changed\",\"document\":{\"nested\":[3,{\"value\":4}]}}\n")
					changed := selectedDeploymentVersion(t, f, changedRows)
					changedPath := filepath.Join(t.TempDir(), "changed.jsonl")
					if err := os.WriteFile(changedPath, changedRows, 0o600); err != nil {
						t.Fatal(err)
					}
					changedSourceID := startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+changedPath)
					waitDeploymentForkRowEvent(t, f.ctx, f.db, changedSourceID, f.runtime.fanOutServing.Wake)
					assertSelectedDeploymentRows(t, f, server, changedSourceID, route, string(changed.VersionID), changedRows)
					if original.VersionID == changed.VersionID {
						t.Fatal("changed dataset reused the original version identity")
					}
					owner, initialRecovery := deploymentForkOwner(t, f)
					if len(initialRecovery) != 0 {
						t.Fatalf("fresh source has selected recovery: %+v", initialRecovery)
					}
					forkServer := deploymentForkServer(t, f, owner)
					key := uuid.NewString()
					params := map[string]any{
						"source_run_id":       sourceRunID,
						"bundle_hash":         f.runtime.sourceArtifactFact.BundleHash(),
						"allow_source_freeze": true, "idempotency_key": key,
						"data_pin_overrides": []any{map[string]any{
							"declaration": map[string]any{"flow_path": ".", "event": selectedDeploymentEvent},
							"version_id":  string(changed.VersionID),
						}},
					}
					result, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
					if len(rpcErr) != 0 {
						t.Fatalf("changed-pin run.fork: %s", rpcErr)
					}
					if result.ForkRunID == "" || result.ForkRunID == sourceRunID || len(result.DataPins) != 1 || result.DataPins[0].VersionID != changed.VersionID {
						t.Fatalf("changed-pin fork lost exact selected version: %+v", result)
					}
					assertSelectedDeploymentRows(t, f, server, result.ForkRunID, route, string(changed.VersionID), changedRows)
					operationBefore := selectedDeploymentForkOperationSnapshot(t, f, result.ForkRunID)

					// Reconstruct the public handler after the successful response is lost.
					replayServer := deploymentForkServerAt(t, f, owner, func() time.Time { return time.Now().Add(48 * time.Hour) })
					replay, rpcErr := deploymentForkRPC(t, f.ctx, replayServer, params)
					if len(rpcErr) != 0 || !reflect.DeepEqual(replay, result) {
						t.Fatalf("durable fork replay differs: first=%+v replay=%+v error=%s", result, replay, rpcErr)
					}
					const parallelReplays = 4
					var concurrent [parallelReplays]apiv1.RunForkExecutionResult
					var concurrentErrors [parallelReplays]json.RawMessage
					var concurrentTransportErrors [parallelReplays]error
					var replayWG sync.WaitGroup
					for index := range concurrent {
						replayWG.Add(1)
						go func(index int) {
							defer replayWG.Done()
							concurrent[index], concurrentErrors[index], concurrentTransportErrors[index] = deploymentForkRPCRequest(f.ctx, replayServer, params)
						}(index)
					}
					replayWG.Wait()
					for index := range concurrent {
						if concurrentTransportErrors[index] != nil || len(concurrentErrors[index]) != 0 || !reflect.DeepEqual(concurrent[index], result) {
							t.Fatalf("concurrent same-key replay %d differs: result=%+v error=%s transport=%v", index, concurrent[index], concurrentErrors[index], concurrentTransportErrors[index])
						}
					}
					var childCount, intentCount int
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, sourceRunID).Scan(&childCount); err != nil {
						t.Fatal(err)
					}
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, result.ForkRunID).Scan(&intentCount); err != nil {
						t.Fatal(err)
					}
					if childCount != 1 || intentCount != 1 {
						t.Fatalf("lost-response retry duplicated child/feed: children=%d intents=%d", childCount, intentCount)
					}
					conflict := map[string]any{}
					for name, value := range params {
						conflict[name] = value
					}
					conflict["data_pin_overrides"] = []any{map[string]any{
						"declaration": map[string]any{"flow_path": ".", "event": selectedDeploymentEvent},
						"version_id":  string(original.VersionID),
					}}
					_, rpcErr = deploymentForkRPC(t, f.ctx, replayServer, conflict)
					if len(rpcErr) == 0 {
						t.Fatal("same keyed fork with changed pin was accepted")
					}
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, result.ForkRunID).Scan(&intentCount); err != nil {
						t.Fatal(err)
					}
					if intentCount != 1 {
						t.Fatalf("hash conflict mutated feed count=%d", intentCount)
					}
					if after := selectedDeploymentForkOperationSnapshot(t, f, result.ForkRunID); after != operationBefore {
						t.Fatalf("keyed replay changed durable activated operation: before=%+v after=%+v", operationBefore, after)
					}

					var generationsBefore int
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, result.ForkRunID).Scan(&generationsBefore); err != nil {
						t.Fatal(err)
					}
					if generationsBefore != 1 {
						t.Fatalf("selected fork has %d executable generations, want one", generationsBefore)
					}
					if err := owner.RetireSelectedContexts(f.ctx); err != nil {
						t.Fatal(err)
					}
					old := f.runtime
					join := beginServingLifetimeJoin(old, nil)
					assertServingJoinComplete(t, join, old, nil)
					f.boot(t)
					f.runtime.fanOutServing.Wake()
					assertSelectedDeploymentRows(t, f, server, result.ForkRunID, route, string(changed.VersionID), changedRows)
					recoveredOwner, recovery := deploymentForkOwner(t, f)
					var controlOnly bool
					for _, entry := range recovery {
						if entry.RunID == result.ForkRunID {
							controlOnly = entry.Disposition == runfork.SelectedForkRecoveryControlOnly
						}
					}
					if !controlOnly {
						t.Fatalf("completed selected fork did not recover control-only: %+v", recovery)
					}
					restartedServer := deploymentForkServer(t, f, recoveredOwner)
					restartedReplay, rpcErr := deploymentForkRPC(t, f.ctx, restartedServer, params)
					if len(rpcErr) != 0 || !reflect.DeepEqual(restartedReplay, result) {
						t.Fatalf("restart reinterpreted completed selected fork: first=%+v replay=%+v error=%s", result, restartedReplay, rpcErr)
					}
					if after := selectedDeploymentForkOperationSnapshot(t, f, result.ForkRunID); after != operationBefore {
						t.Fatalf("restart replay changed durable activated operation: before=%+v after=%+v", operationBefore, after)
					}
					var generationsAfter int
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, result.ForkRunID).Scan(&generationsAfter); err != nil {
						t.Fatal(err)
					}
					if generationsAfter != generationsBefore {
						t.Fatalf("restart resurrected selected executor: before=%d after=%d", generationsBefore, generationsAfter)
					}
					assertSelectedDeploymentRows(t, f, server, sourceRunID, route, string(original.VersionID), originalRows)
					assertSelectedDeploymentRows(t, f, server, changedSourceID, route, string(changed.VersionID), changedRows)
					t.Logf("fork=%s replay exact after handler rebuild", result.ForkRunID)
				})
			}
		})
	}
}

func TestDeploymentSourceDynamicReceiverSelectedForkRefusesWithoutMutation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedDeploymentResourceFixture(t, backend, "dynamic")
			f.runtime.fanOutServing.Close()
			server := f.operatorServer(t)
			start := func(name, accountID string) string {
				t.Helper()
				path := filepath.Join(t.TempDir(), name+".jsonl")
				if err := os.WriteFile(path, []byte(`{"account_id":"`+accountID+`","document":{"route":"dynamic"}}`+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				return startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
			}
			sourceRunID := start("source", "source")
			changedRunID := start("changed", "changed")
			var changedVersion string
			if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='.' AND event_name='root.ready'`, changedRunID).Scan(&changedVersion); err != nil {
				t.Fatal(err)
			}
			owner, _ := deploymentForkOwner(t, f)
			forkServer := deploymentForkServer(t, f, owner)
			params := map[string]any{
				"source_run_id":       sourceRunID,
				"bundle_hash":         f.runtime.sourceArtifactFact.BundleHash(),
				"allow_source_freeze": true, "idempotency_key": uuid.NewString(),
				"data_pin_overrides": []any{map[string]any{
					"declaration": map[string]any{"flow_path": ".", "event": selectedDeploymentEvent},
					"version_id":  changedVersion,
				}},
			}
			_, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
			if !bytes.Contains(rpcErr, []byte("selected_contract_deferred_work_owner_unavailable")) || !bytes.Contains(rpcErr, []byte("dynamic_flow_instance_creation")) {
				t.Fatalf("dynamic receiver selected fork refusal changed: %s", rpcErr)
			}
			var children, selectedExecutions int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, sourceRunID).Scan(&children); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE source_run_id=$1`, sourceRunID).Scan(&selectedExecutions); err != nil {
				t.Fatal(err)
			}
			if children != 0 || selectedExecutions != 0 {
				t.Fatalf("unsupported dynamic receiver fork mutated domain: children=%d selected_executions=%d", children, selectedExecutions)
			}
			var sourceCursor int
			if err := f.db.QueryRowContext(f.ctx, `SELECT cursor FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, sourceRunID).Scan(&sourceCursor); err != nil {
				t.Fatal(err)
			}
			if sourceCursor != 0 {
				t.Fatalf("unsupported dynamic receiver fork advanced source cursor=%d", sourceCursor)
			}
		})
	}
}
