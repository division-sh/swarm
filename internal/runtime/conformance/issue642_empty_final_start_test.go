package conformance

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642EventlessEmptyStartCompletesWithoutInventedIngressBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			repo := conformanceRepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopySelectedDeploymentEmptyFinalRoot(t), contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			f := newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle))
			path := filepath.Join(t.TempDir(), "empty.jsonl")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			source := startDeploymentResourceRun(t, f, f.operatorServer(t), "--data", "root.ready="+path)
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, source, 30*time.Second)
			before, err := storetest.ReadSelectedForkSourceDomain(f.ctx, f.selected, source)
			if err != nil {
				t.Fatal(err)
			}
			owner, _ := deploymentForkOwner(t, f)
			params := map[string]any{"source_run_id": source, "at_start": true, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(), "allow_source_freeze": true, "idempotency_key": uuid.NewString()}
			server := deploymentForkServer(t, f, owner)
			result, rpcErr := deploymentForkRPC(t, f.ctx, server, params)
			if len(rpcErr) != 0 || result.ForkRunID == "" || result.ForkPointKind != string(runfork.RunForkPointRunStart) || result.ForkRevision != 1 || result.ForkEventID != "" || result.ExecutedEventCount != 0 || len(result.DataPins) != 1 {
				t.Fatalf("empty terminal start fork: %+v error=%s", result, rpcErr)
			}
			reader := f.selected.(interface {
				LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
			})
			wait, cancel := context.WithTimeout(f.ctx, 20*time.Second)
			defer cancel()
			for {
				header, err := reader.LoadRunHeader(wait, result.ForkRunID)
				if err != nil {
					t.Fatal(err)
				}
				if header.Status == "completed" {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("genuinely final empty child did not complete: %+v error=%v", header, wait.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			childBefore, err := storetest.ReadSelectedForkSourceDomain(wait, f.selected, result.ForkRunID)
			if err != nil {
				t.Fatal(err)
			}
			for _, run := range []string{source, result.ForkRunID} {
				for _, event := range []string{"root.ready", "fixture.close.requested"} {
					page := deploymentResourcePublicEventsByName(t, wait, f.operatorServer(t), run, event, "")
					if len(page.Events) != 0 || page.NextCursor != "" {
						t.Fatalf("empty completed run invented an event/receiver: %+v", page)
					}
				}
				outcomes, err := storetest.ReadFanOutPublishedOutcomeStorage(wait, f.selected, run)
				if err != nil || len(outcomes) != 0 {
					t.Fatalf("empty completed run invented ordinal outcomes: %+v error=%v", outcomes, err)
				}
			}
			replay, rpcErr := deploymentForkRPC(t, wait, server, params)
			if len(rpcErr) != 0 || !reflect.DeepEqual(result, replay) {
				t.Fatalf("empty completed fork lost its acknowledgment: %+v error=%s", replay, rpcErr)
			}
			cleanup, finish := context.WithTimeout(context.WithoutCancel(f.ctx), 10*time.Second)
			defer finish()
			if err := owner.RetireSelectedContexts(cleanup); err != nil {
				t.Fatal(err)
			}
			old := f.runtime
			join := beginServingLifetimeJoin(old, nil)
			assertServingJoinComplete(t, join, old, nil)
			if err := f.topology.capability.Release(cleanup); err != nil {
				t.Fatal(err)
			}
			f.topology = newNotifyAllChildrenProcessTopology(t, f.ctx, f.selected, f.source)
			f.boot(t)
			recovered, inventory := deploymentForkOwner(t, f)
			matches := 0
			for _, entry := range inventory {
				if entry.RunID == result.ForkRunID {
					matches++
					if entry.Disposition != runfork.SelectedForkRecoveryTerminal || entry.Continuation != nil || entry.Operation == nil {
						t.Fatalf("completed empty child recovered execution: %+v", entry)
					}
				}
			}
			if matches != 1 {
				t.Fatalf("empty completed child missing from exact recovery inventory: %+v", inventory)
			}
			replay, rpcErr = deploymentForkRPC(t, f.ctx, deploymentForkServerAt(t, f, recovered, func() time.Time { return time.Now().Add(48 * time.Hour) }), params)
			if len(rpcErr) != 0 || !reflect.DeepEqual(result, replay) {
				t.Fatalf("empty terminal restart lost permanent replay: %+v error=%s", replay, rpcErr)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(f.ctx, f.selected, source)
			childAfter, childErr := storetest.ReadSelectedForkSourceDomain(f.ctx, f.selected, result.ForkRunID)
			if err != nil || childErr != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(childBefore, childAfter) {
				t.Fatalf("empty terminal fork/recovery changed source or completed child facts: errors=%v/%v", err, childErr)
			}
		})
	}
}
