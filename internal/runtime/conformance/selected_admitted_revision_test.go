package conformance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

type admittedRevisionForkFixture struct {
	runforkexecution.SelectedContractForkLifecycle
	advance func(context.Context, string)
	planned atomic.Bool
	point   runfork.RunForkPoint
	current runfork.RunForkPoint
	request runforkreadiness.MaterializeRequest
	result  runfork.RunForkMaterialization
}

func (f *admittedRevisionForkFixture) PlanRunFork(ctx context.Context, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
	plan, err := f.SelectedContractForkLifecycle.PlanRunFork(ctx, req)
	if err != nil || !f.planned.CompareAndSwap(false, true) {
		return plan, err
	}
	f.point = plan.ForkPoint
	f.advance(ctx, req.SourceRunID)
	current, err := f.SelectedContractForkLifecycle.PlanRunFork(ctx, req)
	f.current = current.ForkPoint
	return plan, err
}

func (f *admittedRevisionForkFixture) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error) {
	f.request = req
	result, err := f.SelectedContractForkLifecycle.MaterializeRunForkForSelectedContractExecution(ctx, req)
	f.result = result
	return result, err
}

func TestIssue2655ServedDeploymentKeepsAdmittedRevisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedExternalResourceFixture(t, backend)
			server := f.operatorServer(t)
			path := filepath.Join(t.TempDir(), "row.jsonl")
			if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			sourceRun := startDeploymentResourceRun(t, f, server, "--data", "task.assigned="+path)
			var calls atomic.Int64
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"model":"gpt-selected-external-proof","choices":[{"message":{"role":"assistant","content":"complete","tool_calls":[{"id":"emit-1","type":"function","function":{"name":"emit_task_completed","arguments":"{}"}}]}}],"usage":{"prompt_tokens":11,"completion_tokens":4,"total_tokens":15}}`))
			}))
			t.Cleanup(provider.Close)
			probe := &admittedRevisionForkFixture{SelectedContractForkLifecycle: f.selected.(runforkexecution.SelectedContractForkLifecycle)}
			probe.advance = func(ctx context.Context, runID string) {
				dialect := authoractivityfixture.DialectSQLite
				if backend == "postgres" {
					dialect = authoractivityfixture.DialectPostgres
				}
				storetest.InsertExistingRunRootEventRecord(t, ctx, f.db, dialect, uuid.NewString(), runID, "task.assigned", eventtest.Producer(events.EventProducerExternal, "admitted-revision-proof"), []byte(`{}`), events.EventEnvelope{}, time.Now().UTC())
			}
			owner := selectedExternalOwnerWithCut(t, f, &selectedExternalEffectCut{}, probe)
			forkServer := selectedExternalForkServer(t, f, owner, provider.URL)
			params := map[string]any{"source_run_id": sourceRun, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(), "allow_source_freeze": true, "idempotency_key": uuid.NewString()}
			ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
			defer cancel()
			result, rpcErr, err := deploymentForkRPCRequest(ctx, forkServer, params)
			if err != nil || len(rpcErr) != 0 {
				t.Fatalf("fork after canonical source advance: err=%v rpc=%s prepared=%+v current=%+v", err, rpcErr, probe.point, probe.current)
			}
			before, after := probe.point, probe.current
			before.Revision, after.Revision = 0, 0
			if probe.point.Kind != runfork.RunForkPointDeploymentRevision || probe.current.Revision <= probe.point.Revision || before != after {
				t.Fatalf("source advance did not isolate the revision: prepared=%+v current=%+v", probe.point, probe.current)
			}
			if probe.result.ForkPoint != probe.point || result.ForkEventID != probe.point.EventID || result.ForkRunID != probe.result.ForkRunID {
				t.Fatalf("child changed its admitted cut: materialization=%+v result=%+v", probe.result.ForkPoint, result)
			}
			operation := probe.request.ForkOperation
			if operation == nil || operation.ResolvedPoint == nil || *operation.ResolvedPoint != probe.point || probe.request.At != probe.point.Input {
				t.Fatalf("prepared point missing from detached operation: %+v", operation)
			}
			reader := f.selected.(apiv1.RunForkOperationReader)
			record, found, err := reader.LoadForkOperationByID(f.ctx, operation.OperationID)
			if err != nil || !found || record.Validate() != nil || record.Status != runfork.ForkOperationActivated || record.Request.ResolvedPoint == nil || *record.Request.ResolvedPoint != probe.point || record.ForkRunID != result.ForkRunID {
				t.Fatalf("durable operation lost the admitted point: found=%v record=%+v err=%v", found, record, err)
			}
			binding, err := f.selected.(interface {
				RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
			}).RequireRunForkSelectedContractBinding(f.ctx, result.ForkRunID)
			if err != nil || binding.ForkPoint != probe.point || binding.BindingID != record.BindingID {
				t.Fatalf("binding lost the admitted point: %+v err=%v", binding, err)
			}
			if calls.Load() != 1 {
				t.Fatalf("provider calls=%d want one", calls.Load())
			}
			attempts := selectedExternalAttempts(t, f, result.ForkRunID)
			if len(attempts) != 1 {
				t.Fatalf("child provider attempts=%+v want one", attempts)
			}
			// Lose the API cache; permanent operation replay must keep the same cut.
			rebuilt := selectedExternalForkServer(t, f, owner, provider.URL)
			replayed, replayErr := deploymentForkRPC(t, ctx, rebuilt, params)
			if len(replayErr) != 0 || !reflect.DeepEqual(replayed, result) || calls.Load() != 1 {
				t.Fatalf("permanent replay changed the child/cut: result=%+v replay=%+v error=%s calls=%d", result, replayed, replayErr, calls.Load())
			}
		})
	}
}
