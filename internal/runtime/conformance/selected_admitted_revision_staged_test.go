package conformance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestIssue2655StagedRevisionRefusalAndRetryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedDeploymentResourceFixture(t, backend, "singleton")
			f.runtime.fanOutServing.Close()
			server := f.operatorServer(t)
			start := func(name string, body []byte) string {
				t.Helper()
				path := filepath.Join(t.TempDir(), name+".jsonl")
				if err := os.WriteFile(path, body, 0o600); err != nil {
					t.Fatal(err)
				}
				return startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
			}
			sourceRun := start("source", []byte("{\"account_id\":\"source\",\"document\":{}}\n"))
			laterRows := []byte("{\"account_id\":\"later\",\"document\":{\"value\":2}}\n")
			laterVersion := selectedDeploymentVersion(t, f, laterRows)
			selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
			operation := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "staged-revision-proof", IdempotencyKey: uuid.NewString(), TransportHash: "staged-request",
				SourceRunID: sourceRun, TargetBundleHash: f.runtime.sourceArtifactFact.BundleHash(), ContractSelection: selection,
				DataPinOverrides: []durabledata.ExplicitPin{{Declaration: laterVersion.Manifest.Declaration, VersionID: laterVersion.VersionID}},
			}
			owner, recovered := deploymentForkOwner(t, f)
			if len(recovered) != 0 {
				t.Fatal("fresh staged source unexpectedly requires recovery")
			}
			ctx := worklifetime.WithProcess(f.ctx, conformanceTestProcessOwner(t))
			repo := conformanceRepoRoot(t)
			prepared, err := owner.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{
				ForkOperation: &operation, SourceRunID: sourceRun, ExpectedBundleHash: operation.TargetBundleHash, ContractSelection: selection,
				DataPinOverrides: append([]durabledata.ExplicitPin(nil), operation.DataPinOverrides...),
				SourceLoader:     runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: f.selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)},
				AgentRuntime:     runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: f.topology.capability},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := prepared.Close(); err != nil {
					t.Error(err)
				}
			})
			request, err := prepared.MaterializationRequest()
			if err != nil || request.ForkOperation == nil || request.ForkOperation.ResolvedPoint == nil {
				t.Fatalf("project staged operation: request=%+v err=%v", request.ForkOperation, err)
			}
			point := request.Preparation.ForkPoint
			if point.Kind != runfork.RunForkPointDeploymentRevision || point.Validate() != nil || *request.ForkOperation.ResolvedPoint != point || operation.ResolvedPoint != nil {
				t.Fatal("preparation did not bind its own operation without mutating the caller")
			}
			operation.Actor = "changed caller"
			operation.DataPinOverrides[0].VersionID = durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("a", 64))
			dialect := authoractivityfixture.DialectSQLite
			if backend == "postgres" {
				dialect = authoractivityfixture.DialectPostgres
			}
			storetest.InsertExistingRunRootEventRecord(t, ctx, f.db, dialect, uuid.NewString(), sourceRun, selectedDeploymentEvent, eventtest.Producer(events.EventProducerExternal, "staged-revision-proof"), []byte(`{"account_id":"post-cut","document":{}}`), events.EventEnvelope{}, time.Now().UTC())
			fork := f.selected.(runforkexecution.SelectedContractForkLifecycle)
			current, err := fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun})
			if err != nil || current.ForkPoint.Revision <= point.Revision {
				t.Fatalf("canonical source did not advance: point=%+v current=%+v err=%v", point, current.ForkPoint, err)
			}
			snapshot := func() map[string]storetest.SelectedForkStorageTableSnapshot {
				t.Helper()
				state, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, f.selected)
				if err != nil {
					t.Fatal(err)
				}
				return state
			}
			for _, test := range []struct {
				name   string
				change func(*runforkreadiness.MaterializeRequest)
			}{
				{"nonpositive", func(r *runforkreadiness.MaterializeRequest) { r.Preparation.ForkPoint.Revision = 0 }},
				{"noncommitted", func(r *runforkreadiness.MaterializeRequest) {
					r.Preparation.ForkPoint.Revision += 1000
					r.ForkOperation.ResolvedPoint = nil
				}},
				{"wrong_kind", func(r *runforkreadiness.MaterializeRequest) { r.Preparation.ForkPoint.Kind = "unknown" }},
				{"event_evidence", func(r *runforkreadiness.MaterializeRequest) { r.Preparation.ForkPoint.EventName = "work.changed" }},
				{"selector", func(r *runforkreadiness.MaterializeRequest) { r.At = uuid.NewString() }},
				{"foreign_run", func(r *runforkreadiness.MaterializeRequest) { r.SourceRunID = uuid.NewString() }},
				{"conflicting_operation", func(r *runforkreadiness.MaterializeRequest) {
					p := point
					p.Revision++
					r.ForkOperation.ResolvedPoint = &p
				}},
				{"fingerprint", func(r *runforkreadiness.MaterializeRequest) {
					r.Preparation.Coordinates.AdmittedPlanFingerprint = strings.Repeat("f", 64)
				}},
				{"missing_operation", func(r *runforkreadiness.MaterializeRequest) { r.ForkOperation = nil }},
			} {
				t.Run(test.name, func(t *testing.T) {
					bad, err := prepared.MaterializationRequest()
					if err != nil {
						t.Fatal(err)
					}
					test.change(&bad)
					before := snapshot()
					if _, err := fork.MaterializeRunForkForSelectedContractExecution(ctx, bad); err == nil {
						t.Fatal("invalid staged evidence materialized a child")
					}
					if after := snapshot(); !reflect.DeepEqual(before, after) {
						t.Fatal("refused staged evidence changed application storage")
					}
				})
			}
			before := snapshot()
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := fork.MaterializeRunForkForSelectedContractExecution(canceled, request); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation changed refusal: %v", err)
			}
			if after := snapshot(); !reflect.DeepEqual(before, after) {
				t.Fatal("canceled materialization changed application storage")
			}
			collector := storetest.CollectTransactions(t, f.selected, storetest.TransactionProbeOptions{})
			_, err = fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err == nil || !strings.Contains(err.Error(), string(laterVersion.VersionID)) {
				t.Fatalf("unimported override did not refuse at the durable-data owner: %v", err)
			}
			counts := collector.Snapshot().Total
			if counts.Failed == 0 || counts.RollbackAttempts == 0 || counts.Ordering.RolledBackHolders == 0 || counts.Mutation.DomainCalls == 0 {
				t.Fatalf("late materialization refusal did not roll back its native mutation: %+v", counts)
			}
			if after := snapshot(); !reflect.DeepEqual(before, after) {
				t.Fatal("late materialization refusal leaked child, operation or history")
			}
			// Retain the required version through ordinary import, then retry the
			// exact prepared request: neither source advancement nor retry moves N.
			_ = start("later", laterRows)
			materialized, err := fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil || materialized.ForkPoint != point || len(materialized.DataPins) != 1 || materialized.DataPins[0].VersionID != laterVersion.VersionID {
				t.Fatalf("retry changed the fixed point/version: result=%+v err=%v", materialized, err)
			}
			beforeReplay := snapshot()
			// A nil operation point is still filled from verified preparation,
			// not from the newer head, for the native staged consumer.
			replay, err := prepared.MaterializationRequest()
			if err != nil {
				t.Fatal(err)
			}
			replay.ForkOperation.ResolvedPoint = nil
			repeated, err := fork.MaterializeRunForkForSelectedContractExecution(ctx, replay)
			if err != nil || repeated.ForkRunID != materialized.ForkRunID || repeated.ForkPoint != point {
				t.Fatalf("repeat rematerialized or moved the child: result=%+v err=%v", repeated, err)
			}
			if after := snapshot(); !reflect.DeepEqual(beforeReplay, after) {
				t.Fatal("exact staged replay changed application storage")
			}
			record, found, err := f.selected.(apiv1.RunForkOperationReader).LoadForkOperationByID(ctx, request.ForkOperation.OperationID)
			if err != nil || !found || record.Validate() != nil || record.Request.Actor != "staged-revision-proof" || record.Request.ResolvedPoint == nil || *record.Request.ResolvedPoint != point {
				t.Fatalf("staged operation adopted caller changes or latest head: record=%+v found=%v err=%v", record, found, err)
			}
		})
	}
}
