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
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
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
					"schema.yaml":        "pins:\n  outputs:\n    - records.ready\n",
					"events.yaml":        "records.ready:\n  body: text\n",
					"nested/schema.yaml": "stages: []\n", "manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n", "nested/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
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
				live, found, err := selected.(runlifecycle.LiveRunReader).LoadLiveRun(ctx, runlifecycle.LiveRunScope{FlowPath: "."})
				if err != nil || !found || live.RunID != runID {
					t.Fatalf("rows publication requires the original live run: %+v %t %v", live, found, err)
				}
				publicationID, sourceID := uuid.NewString(), uuid.NewString()
				rows := runtimebus.DeploymentRunCreationCommand{
					PublicationID: publicationID, LiveRun: &live, Idempotency: command.Idempotency,
					RunCreation: durabledata.RunCreationCommand{PublicationID: publicationID, RunID: runID, Actor: "operator", BundleHash: catalog.BundleHash,
						Data: durabledata.RunCreationDataEnvelope{Imports: []durabledata.FusedImport{{Declaration: ref,
							SourceInvocationID: sourceID, ExpectedHead: durabledata.VersionHead(imported.Candidate.VersionID),
							InputFormat: "jsonl", Input: []byte("{\"body\":\"week-3\"}\n")}}}},
				}
				connector.arm(func(tx driver.Tx) error {
					if commit {
						return errors.Join(fault, tx.Commit())
					}
					return errors.Join(fault, tx.Rollback())
				})
				published, err := owner.CommitDeploymentRunCreation(ctx, rows)
				if published.Acknowledged || !errors.Is(err, fault) {
					t.Fatalf("rows publication fabricated commit acknowledgement: %+v err=%v", published, err)
				}
				var bound int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_feed_publication_members WHERE publication_id=$1`, publicationID).Scan(&bound); err != nil || bound != want {
					t.Fatalf("rows publication physical cut left partial bindings: %d want=%d err=%v", bound, want, err)
				}
				published, err = owner.CommitDeploymentRunCreation(ctx, rows)
				if err != nil || !published.Acknowledged || published.Replay != commit || len(published.Activations) != 0 || published.Record.Summary.PublicationID != publicationID || published.Record.Summary.Kind != "rows_publication" {
					t.Fatalf("publish week-3 rows into existing run: %+v err=%v", published, err)
				}
				if replay, err := owner.CommitDeploymentRunCreation(ctx, rows); err != nil || !replay.Acknowledged || !replay.Replay || !reflect.DeepEqual(replay.Record, published.Record) {
					t.Fatalf("rows publication lost its exact permanent replay: %+v err=%v", replay, err)
				}
				var runs, feeds, pins int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id=$1`, runID).Scan(&runs); err != nil || runs != 1 {
					t.Fatalf("rows publication created or replaced a run: %d err=%v", runs, err)
				}
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&feeds); err != nil || feeds != 2 {
					t.Fatalf("rows publication replaced an earlier feed: %d err=%v", feeds, err)
				}
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_version_pins WHERE run_id=$1 AND version_id=$2`, runID, imported.Candidate.VersionID).Scan(&pins); err != nil || pins != 1 {
					t.Fatalf("rows publication replaced the original immutable pin: %d err=%v", pins, err)
				}
				var resourceOwner *storedurabledata.Owner
				switch selected := selected.(type) {
				case *SQLiteRuntimeStore:
					resourceOwner = selected.durableDataOwner
				case *PostgresStore:
					resourceOwner = selected.durableDataOwner
				}
				feedID := published.Record.DeploymentFeedIDs()[0]
				firstSource, err := resourceOwner.LoadFeedSource(ctx, runID, catalog.BundleHash, feedID, ref)
				if err != nil || firstSource.VersionID == imported.Candidate.VersionID {
					t.Fatalf("new rows did not retain their own version: %+v err=%v", firstSource, err)
				}
				next := rows
				next.PublicationID = uuid.NewString()
				next.RunCreation.PublicationID = next.PublicationID
				next.RunCreation.Data.Imports = append([]durabledata.FusedImport(nil), rows.RunCreation.Data.Imports...)
				next.RunCreation.Data.Imports[0].SourceInvocationID = uuid.NewString()
				next.RunCreation.Data.Imports[0].ExpectedHead = durabledata.VersionHead(firstSource.VersionID)
				next.RunCreation.Data.Imports[0].Input = []byte("{\"body\":\"week-5\"}\n")
				later, err := owner.CommitDeploymentRunCreation(ctx, next)
				if err != nil || !later.Acknowledged || later.Replay || later.Record.Summary.RunID != runID {
					t.Fatalf("later publication into same live run: %+v err=%v", later, err)
				}
				values, err := resourceOwner.LoadPinnedRowsRange(ctx, runID, catalog.BundleHash, firstSource, 0, 1)
				if err != nil || len(values) != 1 || values[0].(map[string]any)["body"] != "week-3" {
					t.Fatalf("later publication changed retained week-3 bytes: %+v err=%v", values, err)
				}
				latest, err := resourceOwner.LoadFeedSource(ctx, runID, catalog.BundleHash, later.Record.DeploymentFeedIDs()[0], ref)
				if err != nil {
					t.Fatal(err)
				}
				pruned, err := resourceOwner.Prune(ctx, durabledata.PruneCommand{PruneInvocationID: uuid.NewString(), Actor: "operator",
					Declaration: ref, VersionID: firstSource.VersionID, ExpectedHead: durabledata.VersionHead(latest.VersionID)})
				if err != nil || pruned.Outcome != "refused_pinned" {
					t.Fatalf("later publication dropped old publication retention: %+v err=%v", pruned, err)
				}
			})
		}
	}
}
