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
		f.selected = storetest.StartSQLiteRuntimeStore(t)
	case "postgres":
		f.selected = storetest.StartPostgresRuntimeStore(t)
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
		f.runtime = newNotifyAllChildrenRuntime(t, f.selected, f.source, time.Now, notifyAllChildrenRuntimeOptions{
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
	control, err := storetest.ReadDeploymentRunControlStorage(ctx, selected, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic run/control query: %v", err)
	} else {
		t.Logf("selected fork diagnostic run=%s status=%s control=%s", forkRunID, control.Status, control.ControlStatus)
	}
	feeds, err := storetest.ReadDeploymentFeedStorage(ctx, selected, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic feeds query: %v", err)
	} else {
		for _, row := range feeds {
			feedID, status, cursor, cardinality, claimant, generation, lease, retryAt, retryFailure, blocked := row.FeedID, row.Status, row.Cursor, row.Cardinality, row.Claimant, row.Generation, row.Lease, row.RetryAt, row.RetryFailure, row.Blocked
			t.Logf("selected fork diagnostic feed=%s status=%s cursor=%d/%d claim=%q generation=%d lease=%q retry_at=%q retry_failure=%q blocked=%q", feedID, status, cursor, cardinality, claimant.String, generation, lease.String, retryAt.String, retryFailure.String, blocked.String)
		}
	}
	executions, err := storetest.ReadSelectedDeploymentExecutionsStorage(ctx, selected, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic executions query: %v", err)
	} else {
		for _, row := range executions {
			executionID, generation, state, lease := row.ExecutionID, row.Generation, row.State, row.Lease
			t.Logf("selected fork diagnostic execution=%s generation=%d state=%s lease=%q", executionID, generation, state, lease.String)
		}
	}
	for _, check := range []struct {
		name string
		read func() (int, error)
	}{
		{"outcomes", func() (int, error) { return storetest.CountNestedRunFanOutOutcomes(ctx, selected, forkRunID) }},
		{"events", func() (int, error) { return storetest.CountPhysicalRunEvents(ctx, selected, forkRunID) }},
		{"pipeline_receipts", func() (int, error) { return storetest.CountDeploymentPipelineReceiptsStorage(ctx, selected, forkRunID) }},
		{"deliveries", func() (int, error) { return storetest.CountRunDeliveryStorage(ctx, selected, forkRunID) }},
		{"pending_deliveries", func() (int, error) {
			return storetest.CountDeploymentPendingDeliveriesStorage(ctx, selected, forkRunID)
		}},
		{"failed_deliveries", func() (int, error) { return storetest.CountDeploymentFailedDeliveriesStorage(ctx, selected, forkRunID) }},
		{"stamped_handoffs", func() (int, error) { return storetest.CountDeploymentStampedHandoffsStorage(ctx, selected, forkRunID) }},
		{"receiver_instances", func() (int, error) { return storetest.CountRunFlowInstanceStorage(ctx, selected, forkRunID) }},
	} {
		count, err := check.read()
		if err != nil {
			t.Logf("selected fork diagnostic %s query: %v", check.name, err)
		} else {
			t.Logf("selected fork diagnostic %s=%d", check.name, count)
		}
	}
	receipts, err := storetest.ReadDeploymentReceiptStorage(ctx, selected, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic receipts query: %v", err)
	} else {
		for _, row := range receipts {
			eventID, subscriberType, subscriberID, outcome, reason, failure := row.EventID, row.SubscriberType, row.SubscriberID, row.Outcome, row.Reason, row.Failure
			t.Logf("selected fork diagnostic receipt event=%s subscriber=%s/%s outcome=%s reason=%q failure=%s", eventID, subscriberType, subscriberID, outcome, reason.String, failure.String)
		}
	}
	deliveries, err := storetest.ReadDeploymentDeliveryStorage(ctx, selected, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic deliveries query: %v", err)
	} else {
		for _, row := range deliveries {
			deliveryID, eventID, subscriberType, subscriberID, status, retryCount, claimVersion, nextEligible, handoff, reason, failure := row.DeliveryID, row.EventID, row.SubscriberType, row.SubscriberID, row.Status, row.RetryCount, row.ClaimVersion, row.NextEligible, row.Handoff, row.Reason, row.Failure
			t.Logf("selected fork diagnostic delivery=%s event=%s subscriber=%s/%s status=%s retries=%d claim_version=%d next_eligible=%q handoff=%q reason=%q failure=%s", deliveryID, eventID, subscriberType, subscriberID, status, retryCount, claimVersion, nextEligible.String, handoff.String, reason.String, failure.String)
		}
	}
	attempts, err := storetest.ReadDeploymentAttemptStorage(ctx, selected, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic attempts query: %v", err)
	} else {
		for _, row := range attempts {
			deliveryID, claimVersion, closure, outcome, reason, failure := row.DeliveryID, row.ClaimVersion, row.Closure, row.Outcome, row.Reason, row.Failure
			t.Logf("selected fork diagnostic attempt delivery=%s claim_version=%d closure=%s outcome=%q reason=%q failure=%s", deliveryID, claimVersion, closure, outcome.String, reason.String, failure.String)
		}
	}
	logs, err := storetest.ReadDeploymentRuntimeLogStorage(ctx, selected, forkRunID)
	if err != nil {
		t.Logf("selected fork diagnostic runtime logs query: %v", err)
	} else {
		for _, row := range logs {
			eventID, payload := row.EventID, row.Payload
			t.Logf("selected fork diagnostic runtime_log event=%s payload=%s", eventID, payload)
		}
	}
}

func waitDeploymentForkRowEvent(t *testing.T, ctx context.Context, selected any, runID string, wake func()) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		eventID, err := storetest.ReadFirstRootReadyEventStorage(ctx, selected, runID)
		if err == nil {
			return eventID
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var intents, cursor, outcomes, events, deliveries, receiverInstances int
	progress, err := storetest.ReadDeploymentIntentProgressStorage(ctx, selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	intents, cursor = progress.Intents, progress.Cursor
	outcomes, err = storetest.CountNestedRunFanOutOutcomes(ctx, selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	events, err = storetest.CountPhysicalRunEvents(ctx, selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	deliveries, err = storetest.CountRunDeliveryStorage(ctx, selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	receiverInstances, err = storetest.CountRunFlowInstanceStorage(ctx, selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	var status, intentBundle, origin string
	var sourceKind, feedID, blocked, claimant, retry storetest.DeploymentNullableText
	metadata, err := storetest.ReadDeploymentIntentMetadataStorage(ctx, selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	status, intentBundle, origin = metadata.Status, metadata.BundleHash, metadata.Origin
	sourceKind, feedID, blocked, claimant, retry = metadata.SourceKind, metadata.FeedID, metadata.Blocked, metadata.Claimant, metadata.Retry
	wake()
	time.Sleep(2 * time.Second)
	afterWake, err := storetest.CountRunNamedEventStorage(ctx, selected, runID, "root.ready")
	if err != nil {
		t.Fatal(err)
	}
	run, err := storetest.ReadDeploymentRunStatusStorage(ctx, selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	runStatus, bundleHash := run.Status, run.BundleHash
	grants, err := storetest.CountAdmittedUnboundBundleGrantsStorage(ctx, selected, bundleHash)
	if err != nil {
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
		RunOrigins: f.selected, StandingRestarts: f.selected, ConstructionPublications: f.selected,
	}
	var owner runforkexecution.SelectedContractExecutionOwner
	var err error
	switch selected := f.selected.(type) {
	case *store.SQLiteRuntimeStore:
		durable.EmitFeedback = selected
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
		durable.EmitFeedback = selected
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
	observed, err := storetest.ReadSelectedDeploymentOperationStorage(f.ctx, f.selected, forkRunID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.id, snapshot.state, snapshot.result, snapshot.updatedAt = observed.ID, observed.State, observed.Result, observed.UpdatedAt
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
			logDeploymentResourceFailure(t, f.selected, runID)
		}
	}()
	waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 3*time.Minute)
	pin, err := storetest.ReadRootReadyResourcePinStorage(f.ctx, f.selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	intentVersion, err := storetest.ReadDeploymentIntentVersionStorage(f.ctx, f.selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	if pin != compiledVersion || intentVersion != pin {
		t.Fatalf("%s selected route changed pinned data: pin=%s intent=%s want=%s", route, pin, intentVersion, compiledVersion)
	}
	var events, delivered, committed, pipelineReceipts, stampedHandoffs int
	for _, check := range []struct {
		read  func() (int, error)
		value *int
	}{
		{func() (int, error) {
			return storetest.CountRunNamedEventStorage(f.ctx, f.selected, runID, "root.ready")
		}, &events},
		{func() (int, error) { return storetest.CountRootReadyDeliveredStorage(f.ctx, f.selected, runID) }, &delivered},
		{func() (int, error) {
			return storetest.CountDeploymentCommittedOutcomesStorage(f.ctx, f.selected, runID)
		}, &committed},
		{func() (int, error) {
			return storetest.CountRootReadySuccessfulPipelineReceiptsStorage(f.ctx, f.selected, runID)
		}, &pipelineReceipts},
		{func() (int, error) { return storetest.CountRootReadyStampedHandoffsStorage(f.ctx, f.selected, runID) }, &stampedHandoffs},
	} {
		count, err := check.read()
		if err != nil {
			t.Fatal(err)
		}
		*check.value = count
	}
	if pipelineReceipts == 0 || stampedHandoffs == 0 {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && (pipelineReceipts == 0 || stampedHandoffs == 0) {
			time.Sleep(25 * time.Millisecond)
			pipelineReceipts, err = storetest.CountRootReadySuccessfulPipelineReceiptsStorage(f.ctx, f.selected, runID)
			if err != nil {
				t.Fatal(err)
			}
			stampedHandoffs, err = storetest.CountRootReadyStampedHandoffsStorage(f.ctx, f.selected, runID)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if events != len(rows) || delivered != len(rows) || committed != 1 || pipelineReceipts != len(rows) || stampedHandoffs != len(rows) {
		t.Fatalf("%s selected route did not settle: events=%d delivered=%d committed=%d pipeline_receipts=%d stamped_handoffs=%d", route, events, delivered, committed, pipelineReceipts, stampedHandoffs)
	}
	parentRunID, err := storetest.ReadDeploymentParentRunStorage(f.ctx, f.selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	if parentRunID.Valid {
		closed, err := storetest.CountClosedSelectedDeploymentExecutionsStorage(f.ctx, f.selected, runID)
		if err != nil {
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

func TestDeploymentSourceChangedPinPublicForkAndLostResponse(t *testing.T) {
	proveSourceChangedPinPublicForkAndLostResponse(t, runfork.RunForkPointDeploymentRevision)
}

func TestEventSourceChangedPinPublicForkAndLostResponse(t *testing.T) {
	proveSourceChangedPinPublicForkAndLostResponse(t, runfork.RunForkPointEvent)
}

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
					assertForkRows()
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
					{
						var err error
						childCount, err = storetest.CountSelectedForkRunsForSource(f.ctx, f.selected, sourceRunID)
						if err != nil {
							t.Fatal(err)
						}
					}
					{
						var err error
						intentCount, err = storetest.CountRunFanOutIntentStorage(f.ctx, f.selected, result.ForkRunID)
						if err != nil {
							t.Fatal(err)
						}
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
					{
						var err error
						intentCount, err = storetest.CountRunFanOutIntentStorage(f.ctx, f.selected, result.ForkRunID)
						if err != nil {
							t.Fatal(err)
						}
					}
					if intentCount != 1 {
						t.Fatalf("hash conflict mutated feed count=%d", intentCount)
					}
					if after := selectedDeploymentForkOperationSnapshot(t, f, result.ForkRunID); after != operationBefore {
						t.Fatalf("keyed replay changed durable activated operation: before=%+v after=%+v", operationBefore, after)
					}

					generationsBefore, err := storetest.CountSelectedRuntimeExecutionStorage(f.ctx, f.selected, result.ForkRunID)
					if err != nil {
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
					restartedServer := deploymentForkServer(t, f, recoveredOwner)
					restartedReplay, rpcErr := deploymentForkRPC(t, f.ctx, restartedServer, params)
					if len(rpcErr) != 0 || !reflect.DeepEqual(restartedReplay, result) {
						t.Fatalf("restart reinterpreted completed selected fork: first=%+v replay=%+v error=%s", result, restartedReplay, rpcErr)
					}
					if after := selectedDeploymentForkOperationSnapshot(t, f, result.ForkRunID); after != operationBefore {
						t.Fatalf("restart replay changed durable activated operation: before=%+v after=%+v", operationBefore, after)
					}
					generationsAfter, err := storetest.CountSelectedRuntimeExecutionStorage(f.ctx, f.selected, result.ForkRunID)
					if err != nil {
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
			changedVersion, err := storetest.ReadRootReadyResourcePinStorage(f.ctx, f.selected, changedRunID)
			if err != nil {
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
			{
				var err error
				children, err = storetest.CountSelectedForkRunsForSource(f.ctx, f.selected, sourceRunID)
				if err != nil {
					t.Fatal(err)
				}
			}
			{
				var err error
				selectedExecutions, err = storetest.CountSelectedSourceRuntimeExecutionsStorage(f.ctx, f.selected, sourceRunID)
				if err != nil {
					t.Fatal(err)
				}
			}
			if children != 0 || selectedExecutions != 0 {
				t.Fatalf("unsupported dynamic receiver fork mutated domain: children=%d selected_executions=%d", children, selectedExecutions)
			}
			sourceCursor, err := storetest.ReadDeploymentIntentCursorStorage(f.ctx, f.selected, sourceRunID)
			if err != nil {
				t.Fatal(err)
			}
			if sourceCursor != 0 {
				t.Fatalf("unsupported dynamic receiver fork advanced source cursor=%d", sourceCursor)
			}
		})
	}
}
