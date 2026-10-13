package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642MultipleRootFeedsStartForkAndRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, emptySecond := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/second_empty=%t", backend, emptySecond), func(t *testing.T) {
				repo := conformanceRepoRoot(t)
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyTwoRootDeploymentFeeds(t), contracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				f := newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle))
				server := f.operatorServer(t)
				source := uuid.NewString()
				args := []string{"run", "start", "--connect", server.URL, "--run-id", source}
				payloads := map[string]string{"root.ready": `{"account_id":"first","document":{"source":"first-version"}}`, "root.other.ready": `{"account_id":"second","document":{"source":"second-version"}}`}
				wantOutcomes := 2
				if emptySecond {
					payloads["root.other.ready"], wantOutcomes = "", 1
				}
				for _, event := range []string{"root.ready", "root.other.ready"} {
					path := filepath.Join(t.TempDir(), "rows.jsonl")
					var rows []byte
					if payloads[event] != "" {
						rows = []byte(payloads[event] + "\n")
					}
					if err := os.WriteFile(path, rows, 0o600); err != nil {
						t.Fatal(err)
					}
					args = append(args, "--data", event+"="+path)
				}
				args = append(args, "--no-follow")
				var stdout, stderr bytes.Buffer
				if code := cliapp.Execute(f.ctx, args, &stdout, &stderr, nil, nil); code != 0 || !strings.Contains(stdout.String(), "run_id="+source) {
					t.Fatalf("operator two-root-input run.start: code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
				}
				waitNotifyAllChildrenRuntimeWithin(t, f.runtime, source, 30*time.Second)
				planning := f.selected.(interface {
					PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
				})
				plan, err := planning.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: source, AtStart: true})
				if err != nil || plan.ForkPoint.Kind != runfork.RunForkPointRunStart || plan.ForkPoint.Revision != 1 || plan.StartFirstTurn != nil || plan.EventCountAtFork != 0 || len(plan.FanOutObligations) != 2 {
					t.Fatalf("multiple feeds do not have an eventless original start: %+v error=%v", plan, err)
				}
				versions := make(map[string]string, 2)
				for _, obligation := range plan.FanOutObligations {
					intent := obligation.Intent
					if intent.Request.Deployment == nil || intent.Request.Deployment.Declaration.FlowPath != "." || intent.Cursor != 0 || len(obligation.Outcomes) != 0 {
						t.Fatalf("original creation already issued or misaddressed a feed: %+v", obligation)
					}
					name := intent.Request.Deployment.Declaration.EventName
					_, known := payloads[name]
					if !known || versions[name] != "" {
						t.Fatalf("original feed declaration duplicated or changed: %+v", intent)
					}
					wantCardinality := 1
					if payloads[name] == "" {
						wantCardinality = 0
					}
					if intent.Request.Cardinality != wantCardinality {
						t.Fatalf("original feed changed exact cardinality: %+v want=%d", intent, wantCardinality)
					}
					versions[name] = string(intent.Request.Source.VersionID)
				}
				before, err := storetest.ReadSelectedForkSourceDomain(f.ctx, f.selected, source)
				if err != nil {
					t.Fatal(err)
				}
				process := conformanceTestProcessOwner(t)
				initialBaseline := process.ActiveCount()
				owner, _ := deploymentForkOwner(t, f)
				params := map[string]any{"source_run_id": source, "at_start": true, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(), "allow_source_freeze": true, "idempotency_key": uuid.NewString()}
				forkServer := deploymentForkServer(t, f, owner)
				result, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
				if len(rpcErr) != 0 || result.ForkRunID == "" || result.ForkRevision != 1 || result.ForkPointKind != string(runfork.RunForkPointRunStart) || result.ForkEventID != "" || result.ExecutedEventCount != 0 || len(result.DataPins) != 2 {
					t.Fatalf("multiple-feed start fork: result=%+v error=%s", result, rpcErr)
				}
				seen := make(map[string]bool, 2)
				for _, pin := range result.DataPins {
					if pin.Declaration.FlowPath != "." || versions[pin.Declaration.EventName] != string(pin.VersionID) || seen[pin.Declaration.EventName] || pin.RunID != result.ForkRunID {
						t.Fatalf("child lost an exact independent pin: %+v", pin)
					}
					seen[pin.Declaration.EventName] = true
				}
				wait, cancel := context.WithTimeout(f.ctx, 30*time.Second)
				defer cancel()
				var settled []storetest.FanOutPublishedOutcomeStorageEvidence
				for {
					settled, err = storetest.ReadFanOutPublishedOutcomeStorage(wait, f.selected, result.ForkRunID)
					if err != nil {
						t.Fatal(err)
					}
					if len(settled) == wantOutcomes {
						break
					}
					select {
					case <-wait.Done():
						t.Fatalf("multiple child feeds did not issue exactly %d outcomes: %+v error=%v", wantOutcomes, settled, wait.Err())
					case <-time.After(10 * time.Millisecond):
					}
				}
				events := map[string]bool{}
				for _, outcome := range settled {
					if outcome.Ordinal != 0 || outcome.Kind != "committed" || outcome.EventID == "" || events[outcome.EventID] {
						t.Fatalf("multiple child feed outcome changed identity/count: %+v", settled)
					}
					events[outcome.EventID] = true
				}
				childServer := f.operatorServer(t)
				for event, raw := range payloads {
					if raw == "" {
						page := deploymentResourcePublicEventsByName(t, wait, childServer, result.ForkRunID, event, "")
						if len(page.Events) != 0 || page.NextCursor != "" {
							t.Fatalf("empty child feed invented events/receiver: %+v", page)
						}
						continue
					}
					var expected map[string]any
					if err := json.Unmarshal([]byte(raw), &expected); err != nil {
						t.Fatal(err)
					}
					for {
						page := deploymentResourcePublicEventsByName(t, wait, childServer, result.ForkRunID, event, "")
						if len(page.Events) == 1 && len(page.Events[0].Deliveries) == 1 && page.Events[0].Deliveries[0].Status == "delivered" {
							if !events[page.Events[0].EventID] || page.NextCursor != "" || !reflect.DeepEqual(page.Events[0].Payload, expected) {
								t.Fatalf("child feed payload/event changed: %+v", page)
							}
							break
						}
						select {
						case <-wait.Done():
							t.Fatalf("child feed %s did not settle: %+v error=%v", event, page, wait.Err())
						case <-time.After(10 * time.Millisecond):
						}
					}
				}
				childBefore, err := storetest.ReadSelectedForkSourceDomain(wait, f.selected, result.ForkRunID)
				if err != nil {
					t.Fatal(err)
				}
				replay, rpcErr := deploymentForkRPC(t, wait, forkServer, params)
				if len(rpcErr) != 0 || !reflect.DeepEqual(result, replay) {
					t.Fatalf("multiple-feed same-key fork minted different work: %+v error=%s", replay, rpcErr)
				}
				cleanup, finish := context.WithTimeout(context.WithoutCancel(f.ctx), 10*time.Second)
				defer finish()
				if err := owner.RetireSelectedContexts(cleanup); err != nil || process.ActiveCount() != initialBaseline {
					t.Fatalf("multiple-feed child did not join retirement: work=%d baseline=%d error=%v", process.ActiveCount(), initialBaseline, err)
				}
				old := f.runtime
				join := beginServingLifetimeJoin(old, nil)
				assertServingJoinComplete(t, join, old, nil)
				if err := f.topology.capability.Release(cleanup); err != nil {
					t.Fatal(err)
				}
				f.topology = newNotifyAllChildrenProcessTopology(t, f.ctx, f.selected, f.source)
				f.boot(t)
				restartBaseline := process.ActiveCount()
				recovered, _ := deploymentForkOwner(t, f)
				restartedServer := deploymentForkServer(t, f, recovered)
				replay, rpcErr = deploymentForkRPC(t, f.ctx, restartedServer, params)
				if len(rpcErr) != 0 || !reflect.DeepEqual(result, replay) {
					t.Fatalf("multiple-feed restart lost the durable acknowledgment: %+v error=%s", replay, rpcErr)
				}
				after, err := storetest.ReadSelectedForkSourceDomain(f.ctx, f.selected, source)
				childAfter, childErr := storetest.ReadSelectedForkSourceDomain(f.ctx, f.selected, result.ForkRunID)
				if err != nil || childErr != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(childBefore, childAfter) {
					t.Fatalf("multiple-feed fork/recovery changed completed source or child business evidence: errors=%v/%v", err, childErr)
				}
				joined, stop := context.WithTimeout(context.WithoutCancel(f.ctx), 10*time.Second)
				defer stop()
				if err := recovered.RetireSelectedContexts(joined); err != nil || process.ActiveCount() != restartBaseline {
					t.Fatalf("multiple-feed successor did not join retirement: work=%d baseline=%d error=%v", process.ActiveCount(), restartBaseline, err)
				}
			})
		}
	}
}
