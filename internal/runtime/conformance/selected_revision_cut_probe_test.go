package conformance

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

type revisionCutProbe struct {
	runforkexecution.SelectedContractForkLifecycle
	t             *testing.T
	advanceSource func(context.Context, string)
	advance       bool
	fixPoint      bool
	planned       bool
	mutation      chan string
}

func (p *revisionCutProbe) PlanRunFork(ctx context.Context, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
	plan, err := p.SelectedContractForkLifecycle.PlanRunFork(ctx, req)
	if err != nil || p.planned {
		return plan, err
	}
	p.planned = true
	if !p.advance {
		return plan, nil
	}
	p.advanceSource(ctx, req.SourceRunID)
	after, err := p.SelectedContractForkLifecycle.PlanRunFork(ctx, req)
	if err != nil {
		return plan, err
	}
	left, right := plan.ForkPoint, after.ForkPoint
	left.Revision, right.Revision = 0, 0
	if left != right || after.ForkPoint.Revision <= plan.ForkPoint.Revision {
		return plan, fmt.Errorf("cut did not isolate monotonic revision: non_revision_equal=%t before_revision=%d after_revision=%d", left == right, plan.ForkPoint.Revision, after.ForkPoint.Revision)
	}
	p.t.Logf("cut canonical root event committed; prepared_kind=%s prepared_revision=%d current_kind=%s current_revision=%d all_non_revision_fields_equal=%t", plan.ForkPoint.Kind, plan.ForkPoint.Revision, after.ForkPoint.Kind, after.ForkPoint.Revision, left == right)
	return plan, nil
}

func (p *revisionCutProbe) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error) {
	p.t.Logf("materialization selector_empty=%t fixed_operation_point=%t prepared_kind=%s prepared_revision=%d", req.At == "", req.ForkOperation != nil && req.ForkOperation.ResolvedPoint != nil, req.Preparation.ForkPoint.Kind, req.Preparation.ForkPoint.Revision)
	if p.fixPoint {
		if req.ForkOperation == nil {
			return runfork.RunForkMaterialization{}, fmt.Errorf("fixed-point counterfactual requires an actual operation")
		}
		operation := *req.ForkOperation
		point := req.Preparation.ForkPoint
		operation.ResolvedPoint = &point
		req.ForkOperation = &operation
	}
	materialized, err := p.SelectedContractForkLifecycle.MaterializeRunForkForSelectedContractExecution(ctx, req)
	message := ""
	if err != nil {
		message = err.Error()
	}
	p.mutation <- message
	return materialized, err
}

func TestDisposableSelectedDeploymentRevisionCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cutName := range []string{"unchanged", "advance_unpinned", "advance_fixed_counterfactual"} {
			t.Run(backend+"/"+cutName, func(t *testing.T) {
				f := selectedExternalResourceFixture(t, backend)
				server := f.operatorServer(t)
				path := filepath.Join(t.TempDir(), "row.jsonl")
				if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				runID := startDeploymentResourceRun(t, f, server, "--data", "task.assigned="+path)
				var calls atomic.Int64
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"model":"gpt-selected-external-proof","choices":[{"message":{"role":"assistant","content":"complete","tool_calls":[{"id":"emit-1","type":"function","function":{"name":"emit_task_completed","arguments":"{}"}}]}}],"usage":{"prompt_tokens":11,"completion_tokens":4,"total_tokens":15}}`))
				}))
				t.Cleanup(provider.Close)
				probe := &revisionCutProbe{SelectedContractForkLifecycle: f.selected.(runforkexecution.SelectedContractForkLifecycle), t: t, advance: cutName != "unchanged", fixPoint: cutName == "advance_fixed_counterfactual"}
				probe.mutation = make(chan string, 1)
				probe.advanceSource = func(ctx context.Context, runID string) {
					dialect := authoractivityfixture.DialectSQLite
					if backend == "postgres" {
						dialect = authoractivityfixture.DialectPostgres
					}
					storetest.InsertExistingRunRootEventRecord(t, ctx, f.db, dialect, uuid.NewString(), runID, "task.assigned", eventtest.Producer(events.EventProducerExternal, "revision-cut"), []byte(`{}`), events.EventEnvelope{}, time.Now().UTC())
				}
				cut := &selectedExternalEffectCut{stage: "response_observed", entered: make(chan effects.Attempt, 1)}
				owner := selectedExternalOwnerWithCut(t, f, cut, probe)
				forkServer := selectedExternalForkServer(t, f, owner, provider.URL)
				params := map[string]any{"source_run_id": runID, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(), "allow_source_freeze": true, "idempotency_key": uuid.NewString()}
				ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
				defer cancel()
				_, rpcErr, transportErr := deploymentForkRPCRequest(ctx, forkServer, params)
				if transportErr != nil {
					t.Fatalf("cut transport: %v", transportErr)
				}
				var mutationError string
				select {
				case mutationError = <-probe.mutation:
				default:
					t.Fatalf("cut did not enter materialization: rpc=%s", rpcErr)
				}
				if cutName == "advance_unpinned" {
					if calls.Load() != 0 || len(cut.entered) != 0 || !strings.Contains(string(rpcErr), "-32603") || !strings.Contains(mutationError, "selected preparation differs from transaction's fixed admitted plan") {
						t.Fatalf("unpinned revision cut did not refuse before provider: calls=%d cut=%d rpc=%s", calls.Load(), len(cut.entered), rpcErr)
					}
				} else if calls.Load() != 1 || len(cut.entered) != 1 || mutationError != "" {
					t.Fatalf("control did not reach real response_observed: calls=%d cut=%d rpc=%s", calls.Load(), len(cut.entered), rpcErr)
				}
			})
		}
	}
}
