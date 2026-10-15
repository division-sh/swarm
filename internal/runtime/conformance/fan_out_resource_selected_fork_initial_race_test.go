package conformance

import (
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Two first callers share the durable key but can have distinct invocation IDs.
// The materialization transaction must not commit a child for the losing call.
func TestDeploymentSourceSelectedForkConcurrentFirstMaterialization(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedDeploymentResourceFixture(t, backend, "singleton")
			f.runtime.fanOutServing.Close()
			server := f.operatorServer(t)
			path := filepath.Join(t.TempDir(), "source.jsonl")
			if err := os.WriteFile(path, []byte("{\"account_id\":\"race\",\"document\":{\"origin\":\"source\"}}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			sourceRunID := startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
			owner, recovered := deploymentForkOwner(t, f)
			if len(recovered) != 0 {
				t.Fatalf("fresh selected owner inherited work: %+v", recovered)
			}
			forkServer := deploymentForkServer(t, f, owner)
			params := map[string]any{
				"source_run_id":       sourceRunID,
				"bundle_hash":         f.runtime.sourceArtifactFact.BundleHash(),
				"allow_source_freeze": true,
				"idempotency_key":     uuid.NewString(),
			}
			type outcome struct {
				result apiv1.RunForkExecutionResult
				rpcErr []byte
				err    error
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			outcomes := make([]outcome, 2)
			for i := range outcomes {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					outcomes[i].result, outcomes[i].rpcErr, outcomes[i].err = deploymentForkRPCRequest(f.ctx, forkServer, params)
				}(i)
			}
			close(start)
			wg.Wait()
			for _, outcome := range outcomes {
				if outcome.err != nil {
					t.Fatalf("concurrent transport failed: %v", outcome.err)
				}
			}
			var childCount, operationCount int
			{
				var err error
				childCount, err = storetest.CountSelectedForkRunsForSource(f.ctx, f.selected, sourceRunID)
				if err != nil {
					t.Fatal(err)
				}
			}
			operationCount, err := storetest.CountSelectedForkSourceOperationsStorage(f.ctx, f.selected, sourceRunID)
			if err != nil {
				t.Fatal(err)
			}
			if childCount != 1 || operationCount != 1 {
				t.Fatalf("first contenders committed children=%d operations=%d, want one each; errors=%s / %s",
					childCount, operationCount, outcomes[0].rpcErr, outcomes[1].rpcErr)
			}
			resolved, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
			if len(rpcErr) != 0 || resolved.ForkRunID == "" {
				t.Fatalf("same-key retry failed to resolve exact child: result=%+v error=%s contenders=%s / %s",
					resolved, rpcErr, outcomes[0].rpcErr, outcomes[1].rpcErr)
			}
			if resolved.ForkPointKind != "deployment_revision" || resolved.ForkRevision <= 0 || resolved.ForkEventID != "" {
				t.Fatalf("same-key replay lost eventless deployment point: %+v", resolved)
			}
			for _, outcome := range outcomes {
				if len(outcome.rpcErr) == 0 && !reflect.DeepEqual(outcome.result, resolved) {
					t.Fatalf("contender reported success for a different immutable result: result=%+v resolved=%+v", outcome.result, resolved)
				}
			}
			{
				var err error
				childCount, err = storetest.CountSelectedForkRunsForSource(f.ctx, f.selected, sourceRunID)
				if err != nil {
					t.Fatal(err)
				}
			}
			operationCount, err = storetest.CountSelectedForkSourceOperationsStorage(f.ctx, f.selected, sourceRunID)
			if err != nil {
				t.Fatal(err)
			}
			if childCount != 1 || operationCount != 1 {
				t.Fatalf("concurrent first materialization committed children=%d operations=%d, want one each", childCount, operationCount)
			}
			point, err := storetest.ReadSelectedDeploymentPointStorage(f.ctx, f.selected, resolved.ForkRunID)
			if err != nil {
				t.Fatalf("read exact first-materialization authority: %v", err)
			}
			runKind, operationKind, bindingKind, operationChildID := point.RunKind, point.OperationKind, point.BindingKind, point.ChildID
			runRevision, operationRevision, bindingRevision := point.RunRevision, point.OperationRevision, point.BindingRevision
			if runKind != "deployment_revision" || operationKind != runKind || bindingKind != runKind ||
				runRevision <= 0 || operationRevision != runRevision || bindingRevision != runRevision ||
				operationChildID != resolved.ForkRunID {
				t.Fatalf("first-materialization point disagreement: run=%s/%d operation=%s/%d child=%s binding=%s/%d",
					runKind, runRevision, operationKind, operationRevision, operationChildID, bindingKind, bindingRevision)
			}
		})
	}
}
