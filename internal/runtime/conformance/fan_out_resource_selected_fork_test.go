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
	return selectedDeploymentResourceFixtureConfigured(t, backend, route, false, false, nil, nil, nil)
}

func selectedDeploymentResourceFixtureWithProbe(t *testing.T, backend, route string, probe runtimelifecycleprobe.Observer) *deploymentResourceFixture {
	return selectedDeploymentResourceFixtureWithDelivery(t, backend, route, probe, nil, nil)
}

func selectedDeploymentResourceFixtureWithDelivery(t *testing.T, backend, route string, probe runtimelifecycleprobe.Observer, wrapDelivery func(runtimedelivery.Store) runtimedelivery.Store, wrapExecutor func(startupownership.FanOutExecutor) startupownership.FanOutExecutor) *deploymentResourceFixture {
	return selectedDeploymentResourceFixtureConfigured(t, backend, route, true, false, probe, wrapDelivery, wrapExecutor)
}

func selectedDeploymentResourceFixtureConfigured(t *testing.T, backend, route string, keyed, closeAfterInput bool, probe runtimelifecycleprobe.Observer, wrapDelivery func(runtimedelivery.Store) runtimedelivery.Store, wrapExecutor func(startupownership.FanOutExecutor) startupownership.FanOutExecutor) *deploymentResourceFixture {
	t.Helper()
	var root string
	if closeAfterInput {
		if route != "singleton" || !keyed {
			t.Fatal("finite pending receiver proof requires its exact singleton fixture")
		}
		root = canonicalrouting.CopySelectedDeploymentResourceFiniteSingleton(t)
	} else {
		root = canonicalrouting.CopySelectedDeploymentResource(t, route, keyed)
	}
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
					logSelectedDeploymentForkFailure(t, f.selected, result.Materialization.ForkRunID)
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

func logSelectedDeploymentForkFailure(t *testing.T, selected any, forkRunID string) {
	t.Helper()
	if forkRunID == "" {
		t.Log("selected fork diagnostic: no acknowledged materialization")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sections, err := storetest.ReadSelectedDeploymentForkDiagnostics(ctx, selected, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic run=%s: %v", forkRunID, err)
		return
	}
	for _, section := range sections {
		t.Logf("selected fork diagnostic run=%s %s columns=%v rows=%+v error=%s", forkRunID, section.Name, section.Columns, section.Rows, section.Failure)
	}
}

func waitDeploymentForkRowEvent(t *testing.T, ctx context.Context, selected any, runID string, wake func()) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		eventID, err := storetest.ReadSelectedDeploymentResourceEvent(ctx, selected, runID)
		if err == nil {
			return eventID
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	before, err := storetest.ReadSelectedDeploymentResourceWait(ctx, selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	wake()
	time.Sleep(2 * time.Second)
	after, err := storetest.ReadSelectedDeploymentResourceWake(ctx, selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("deployment run %s never published its resource event without manual wake: intents=%d cursor=%d outcomes=%d events=%d deliveries=%d receiver_instances=%d intent_status=%s run_status=%s matching_bundle=%t origin=%s source=%+v feed=%+v grants=%d blocked=%+v claimant=%+v retry=%+v after_manual_wake=%d", runID, before.Intents, before.Cursor, before.Outcomes, before.Events, before.Deliveries, before.ReceiverInstances, before.Status, after.RunStatus, before.IntentBundle == after.BundleHash, before.Origin, before.SourceKind, before.FeedID, after.Grants, before.Blocked, before.Claimant, before.Retry, after.Events)
	return ""
}

func deploymentForkOwner(t *testing.T, f *deploymentResourceFixture) (runforkexecution.SelectedContractExecutionOwner, []runfork.SelectedForkRecoveryResult) {
	return deploymentForkOwnerWithOverrides(t, f, nil, nil)
}

func deploymentForkOwnerWithDelivery(t *testing.T, f *deploymentResourceFixture, delivery runtimedelivery.Store) (runforkexecution.SelectedContractExecutionOwner, []runfork.SelectedForkRecoveryResult) {
	return deploymentForkOwnerWithOverrides(t, f, delivery, nil)
}

func deploymentForkOwnerWithOverrides(t *testing.T, f *deploymentResourceFixture, delivery runtimedelivery.Store, fork runforkexecution.SelectedContractForkLifecycle) (runforkexecution.SelectedContractExecutionOwner, []runfork.SelectedForkRecoveryResult) {
	return deploymentForkOwnerWithReplay(t, f, delivery, fork, nil)
}

func deploymentForkOwnerWithReplay(t *testing.T, f *deploymentResourceFixture, delivery runtimedelivery.Store, fork runforkexecution.SelectedContractForkLifecycle, replay runforkexecution.SelectedContractReplayPersistence) (runforkexecution.SelectedContractExecutionOwner, []runfork.SelectedForkRecoveryResult) {
	return deploymentForkOwnerWithExecutor(t, f, delivery, fork, replay, nil)
}

func deploymentForkOwnerWithExecutor(t *testing.T, f *deploymentResourceFixture, delivery runtimedelivery.Store, fork runforkexecution.SelectedContractForkLifecycle, replay runforkexecution.SelectedContractReplayPersistence, decorate func(startupownership.FanOutExecutor) startupownership.FanOutExecutor) (runforkexecution.SelectedContractExecutionOwner, []runfork.SelectedForkRecoveryResult) {
	t.Helper()
	if delivery == nil {
		delivery = f.selected
	}
	if replay == nil {
		var ok bool
		replay, ok = f.selected.(runforkexecution.SelectedContractReplayPersistence)
		if !ok {
			t.Fatalf("selected store %T lacks the canonical fork publication owner", f.selected)
		}
	}
	durable := bus.DurableDependencies{
		ReplyContext: f.selected, RunLifecycle: f.selected, DeliveryLifecycle: delivery,
		ActiveAgents:   f.selected,
		PreparedEvents: f.selected, TargetFailureRecorder: f.selected,
		RunOrigins: f.selected, StandingRestarts: f.selected,
	}
	var owner runforkexecution.SelectedContractExecutionOwner
	var err error
	switch selected := f.selected.(type) {
	case *store.SQLiteRuntimeStore:
		durable.EmitFeedback = selected
		workflow := pipeline.NewWorkflowPersistence(selected)
		durable.Instances, durable.ConstructionPublications = workflow, workflow
		forkOwner := fork
		if forkOwner == nil {
			forkOwner = selected
		}
		roles := manager.PersistenceRoles{
			LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected,
			EffectsRecovery: selected, DeliveryQuiescence: selected, EventExistence: selected,
			DirectiveOperations: selected, DirectiveTargets: selected,
			StandingRestarts: selected,
		}
		owner, err = runforkexecution.NewSelectedContractExecutionOwner(
			workflow, forkOwner, selected, replay, selected,
			durable, selected.PipelineObligations(), selected, roles,
			selected, selected, selected, selected, selected, selected, selected,
			selected, selected, selected, selected, selected,
			selected, selected,
		)
	case *store.PostgresStore:
		durable.EmitFeedback = selected
		workflow := pipeline.NewWorkflowPersistence(selected)
		durable.Instances, durable.ConstructionPublications = workflow, workflow
		forkOwner := fork
		if forkOwner == nil {
			forkOwner = selected
		}
		roles := manager.PersistenceRoles{
			LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected,
			EffectsRecovery: selected, DeliveryQuiescence: selected, EventExistence: selected,
			DirectiveOperations: selected, DirectiveTargets: selected,
			StandingRestarts: selected,
		}
		owner, err = runforkexecution.NewSelectedContractExecutionOwner(
			workflow, forkOwner, selected, replay, selected,
			durable, selected.PipelineObligations(), selected, roles,
			selected, selected, selected, selected, selected, selected, selected,
			selected, selected, selected, selected, selected,
			selected, selected,
		)
	default:
		t.Fatalf("selected fork store %T is not supported", f.selected)
	}
	if err != nil {
		t.Fatal(err)
	}
	if decorate != nil {
		owner, err = runforkexecution.WithFanOutExecutorForTest(owner, decorate)
		if err != nil {
			t.Fatal(err)
		}
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
	assertSelectedDeploymentRowSet(t, f, server, runID, route, compiledVersion, [][]byte{rows}, wantClosed)
}

func assertSelectedDeploymentRowSet(t *testing.T, f *deploymentResourceFixture, server *httptest.Server, runID, route string, compiledVersion string, rows [][]byte, wantClosed int) {
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
	if events != len(rows) || delivered != len(rows) || committed != 1 || pipelineReceipts != len(rows) || stampedHandoffs != len(rows) {
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
	want := make(map[string]map[string]any, len(rows))
	for _, row := range rows {
		var payload map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(row), &payload); err != nil {
			t.Fatal(err)
		}
		key, ok := payload["account_id"].(string)
		if !ok || key == "" || want[key] != nil {
			t.Fatalf("invalid or duplicate expected row: %+v", payload)
		}
		want[key] = payload
	}
	page := selectedDeploymentPublicEvents(t, f.ctx, server, runID)
	if len(page.Events) != len(rows) || page.NextCursor != "" {
		t.Fatalf("%s selected route lost document-shaped public readback: %+v want=%+v", route, page, want)
	}
	wantSubscriber := conformanceNode(t, "", "root-collector").Key()
	if route == "singleton" {
		wantSubscriber = conformanceNode(t, "consumer", "consumer-node").Key()
	}
	for _, event := range page.Events {
		key, _ := event.Payload["account_id"].(string)
		if want[key] == nil || !reflect.DeepEqual(event.Payload, want[key]) || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" || event.Deliveries[0].SubscriberID != wantSubscriber {
			t.Fatalf("%s selected route lost exact row/subscriber readback: %+v want=%+v subscriber=%s", route, event, want[key], wantSubscriber)
		}
		delete(want, key)
	}
}

func TestDeploymentSourceChangedPinPublicForkReplayAfterReconstruction(t *testing.T) {
	proveSourceChangedPinPublicForkAndLostResponse(t, runfork.RunForkPointDeploymentRevision)
}

func TestEventSourceChangedPinPublicForkReplayAfterReconstruction(t *testing.T) {
	proveSourceChangedPinPublicForkAndLostResponse(t, runfork.RunForkPointEvent)
}

// The historical helper name is retained for declaration-keyed debt evidence.
// This proves replay after a decoded response, not a dropped wire response.
func proveSourceChangedPinPublicForkAndLostResponse(t *testing.T, pointKind runfork.RunForkPointKind) {
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
					forkEventID := waitDeploymentForkRowEvent(t, f.ctx, f.selected, sourceRunID, f.runtime.fanOutServing.Wake)
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
					waitDeploymentForkRowEvent(t, f.ctx, f.selected, changedSourceID, f.runtime.fanOutServing.Wake)
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
					if pointKind == runfork.RunForkPointEvent {
						params["fork_event_id"] = forkEventID
					}
					result, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
					if len(rpcErr) != 0 {
						t.Fatalf("changed-pin run.fork: %s", rpcErr)
					}
					if result.ForkRunID == "" || result.ForkRunID == sourceRunID || len(result.DataPins) != 1 || result.DataPins[0].VersionID != changed.VersionID {
						t.Fatalf("changed-pin fork lost exact selected version: %+v", result)
					}
					expectedEventID := ""
					if pointKind == runfork.RunForkPointEvent {
						expectedEventID = forkEventID
					}
					if result.ForkPointKind != string(pointKind) || result.ForkRevision <= 0 || result.ForkEventID != expectedEventID {
						t.Fatalf("public result lost its exact typed point: %+v", result)
					}
					forkRows := [][]byte{changedRows}
					if pointKind == runfork.RunForkPointEvent {
						// The explicit event cut replays its recorded row once; the
						// selected deployment feed contributes the changed row once.
						forkRows = append(forkRows, originalRows)
					}
					assertForkRows := func() {
						assertSelectedDeploymentRowSet(t, f, server, result.ForkRunID, route, string(changed.VersionID), forkRows, 1)
					}
					assertSelectedDeploymentRowSet(t, f, server, result.ForkRunID, route, string(changed.VersionID), forkRows, 0)
					beforeRefusals, err := storetest.ReadSelectedForkSourceDomain(f.ctx, f.selected, result.ForkRunID)
					if err != nil {
						t.Fatal(err)
					}
					workBeforeRefusals := conformanceTestProcessOwner(t).ActiveCount()
					canceled, cancel := context.WithCancel(f.ctx)
					cancel()
					for _, refused := range []struct {
						name  string
						owner runforkexecution.SelectedContractExecutionOwner
						ctx   context.Context
						runID string
					}{
						{"wrong_run", owner, f.ctx, uuid.NewString()},
						{"ordinary_run", owner, f.ctx, changedSourceID},
						{"unavailable_owner", runforkexecution.SelectedContractExecutionOwner{}, f.ctx, result.ForkRunID},
						{"canceled_caller", owner, canceled, result.ForkRunID},
					} {
						observed, err := refused.owner.PublishSelectedFixtureCloseForTest(refused.ctx, refused.runID,
							runforkexecution.SelectedFixtureCloseInputForTest{AccountID: "changed"})
						if err == nil || observed != (runforkexecution.SelectedFixtureCloseResultForTest{}) {
							t.Fatalf("%s fixture close acquired a child: %+v error=%v", refused.name, observed, err)
						}
					}
					afterRefusals, err := storetest.ReadSelectedForkSourceDomain(f.ctx, f.selected, result.ForkRunID)
					if err != nil || !reflect.DeepEqual(afterRefusals, beforeRefusals) || conformanceTestProcessOwner(t).ActiveCount() != workBeforeRefusals {
						t.Fatalf("refused fixture inputs changed child state or retained work: error=%v", err)
					}
					closeResult, err := owner.PublishSelectedFixtureCloseForTest(f.ctx, result.ForkRunID,
						runforkexecution.SelectedFixtureCloseInputForTest{AccountID: "changed"})
					if err != nil {
						t.Fatalf("close settled selected fixture through retained child: %v", err)
					}
					if closeResult.RunID != result.ForkRunID || closeResult.BindingID == "" || closeResult.ExecutionID == "" || closeResult.Generation != 1 || closeResult.EventID == "" {
						t.Fatalf("fixture close lost retained execution evidence: %+v", closeResult)
					}
					assertForkRows()
					if closes, err := storetest.CountRunEventNameStorage(f.ctx, f.selected, result.ForkRunID, "fixture.close.requested"); err != nil || closes != 1 {
						t.Fatalf("selected fixture close publication count=%d, want one: %v", closes, err)
					}
					operationBefore := selectedDeploymentForkOperationSnapshot(t, f, result.ForkRunID)

					// The first response was decoded; rebuild the public handler for replay.
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
						t.Fatalf("reconstructed-handler replay duplicated child/feed: children=%d intents=%d", childCount, intentCount)
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
					if err := f.topology.capability.Release(context.Background()); err != nil {
						t.Fatal(err)
					}
					f.topology = newNotifyAllChildrenProcessTopology(t, f.ctx, f.selected, f.source)
					f.boot(t)
					f.runtime.fanOutServing.Wake()
					assertForkRows()
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
					if _, err := recoveredOwner.PublishSelectedFixtureCloseForTest(f.ctx, result.ForkRunID,
						runforkexecution.SelectedFixtureCloseInputForTest{AccountID: "changed"}); err == nil {
						t.Fatal("completed control-only child acquired another close input")
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
