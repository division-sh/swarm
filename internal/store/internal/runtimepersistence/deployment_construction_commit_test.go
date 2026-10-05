package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

func TestDeploymentConstructionNativeCommitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, commit := range []bool{false, true} {
			cut := "rollback_before_ack"
			if commit {
				cut = "commit_before_lost_ack"
			}
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, connector := newP16RaceStore(t, backend)
				f := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, map[string]string{
					"schema.yaml":        "name: deployment-construction\npins:\n  outputs:\n    - records.ready\n",
					"events.yaml":        "records.ready:\n  body: text\n",
					"nested/schema.yaml": "name: nested\n",
				}, nil, ownStoreTestAgentManager, nil)
				dataOwner := selected.(interface {
					EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
					ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
				})
				catalog, err := contracts.BuildDurableDataCatalog(f.bundle)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := dataOwner.EnsureSourceArtifactWithData(f.ctx, f.bundle.SourceArtifact, catalog); err != nil {
					t.Fatal(err)
				}
				ref, err := durabledata.ParseDeclarationRef(".", "records.ready")
				if err != nil {
					t.Fatal(err)
				}
				imported, err := dataOwner.ExecuteDataSourceOperation(f.ctx, durabledata.SourceCommand{
					Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash,
					Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"body\":\"recorded\"}\n"),
				})
				if err != nil {
					t.Fatal(err)
				}
				runID := uuid.NewString()
				ctx := correlation.WithRunID(f.ctx, runID)
				at := time.Now().UTC().Truncate(time.Microsecond)
				source := semanticview.Wrap(f.bundle)
				plan, err := f.manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
					ContractBundle: source, OccurredAt: at,
					Instance: flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, "", ""),
				})
				if err != nil {
					t.Fatal(err)
				}
				command := runtimebus.DeploymentRunCreationCommand{
					RunCreation: durabledata.RunCreationCommand{RunID: runID, Actor: "operator", BundleHash: catalog.BundleHash,
						Data: durabledata.RunCreationDataEnvelope{Pins: []durabledata.ExplicitPin{{Declaration: ref, VersionID: imported.Candidate.VersionID}}}},
					Idempotency: apiidempotency.Request{Method: "run.start", Actor: apiidempotency.BearerActor("operator"), Now: at, TTL: time.Hour},
					Root:        runtimebus.FlowInstanceActivationCommand{Plan: plan},
				}
				for _, construction := range plan.ConstructionPlans() {
					command.Root.RouteTopology = append(command.Root.RouteTopology, runtimebus.FlowInstanceRouteRecordSet{
						Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: construction.Identity.Route()},
					})
				}
				owner := selected.(runtimebus.DeploymentRunCreationCommitOwner)
				fault := errors.New("injected deployment physical COMMIT acknowledgment loss")
				var commits atomic.Int32
				connector.arm(func(tx driver.Tx) error {
					commits.Add(1)
					if commit {
						return errors.Join(fault, tx.Commit())
					}
					return errors.Join(fault, tx.Rollback())
				})
				result, err := owner.CommitDeploymentRunCreation(ctx, command)
				if !errors.Is(err, fault) || result.Acknowledged || len(result.Activations) != 0 || commits.Load() != 1 {
					t.Fatalf("uncertain deployment returned authority: %+v err=%v commits=%d", result, err, commits.Load())
				}
				want := 0
				if commit {
					want = 1
				}
				for _, table := range []string{"runs", "resource_version_pins", "fan_out_intents", "resource_run_creation_operations"} {
					var rows int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", runID).Scan(&rows); err != nil || rows != want {
						t.Fatalf("atomic %s: rows=%d want=%d err=%v", table, rows, want, err)
					}
				}
				for _, table := range []string{"flow_instances", "flow_instance_runtime_readiness"} {
					var rows int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", runID).Scan(&rows); err != nil || rows != len(plan.ConstructionPlans())*want {
						t.Fatalf("atomic tree %s: rows=%d want=%d err=%v", table, rows, len(plan.ConstructionPlans())*want, err)
					}
				}
				result, err = owner.CommitDeploymentRunCreation(ctx, command)
				if err != nil || !result.Acknowledged || result.Replay != commit || len(result.Activations) != (1-want) {
					t.Fatalf("exact deployment reconciliation: %+v err=%v", result, err)
				}
				before := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
				if replay, err := owner.CommitDeploymentRunCreation(ctx, command); err != nil || !replay.Acknowledged || !replay.Replay || len(replay.Activations) != 0 || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
					t.Fatalf("permanent receipt repeated construction: %+v err=%v", replay, err)
				}
				var occurrences int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&occurrences); err != nil || occurrences != 0 {
					t.Fatalf("feed-only creation fabricated an event: %d err=%v", occurrences, err)
				}
			})
		}
	}
}
