package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
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
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func deploymentObservationReadCases(id, bundle string) []fanOutSemanticReadCase {
	return []fanOutSemanticReadCase{
		{"pin", func(ctx context.Context, owner any) (any, error) {
			return ReadRootReadyResourcePinStorageForTest(ctx, owner, id)
		}, "", true},
		{"version", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentIntentVersionStorageForTest(ctx, owner, id)
		}, "", true},
		{"progress", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentIntentProgressStorageForTest(ctx, owner, id)
		}, DeploymentIntentProgressStorage{}, false},
		{"metadata", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentIntentMetadataStorageForTest(ctx, owner, id)
		}, DeploymentIntentMetadataStorage{}, true},
		{"feeds", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentFeedStorageForTest(ctx, owner, id)
		}, []DeploymentFeedStorage(nil), false},
		{"intents", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentIntentDiagnosticsStorageForTest(ctx, owner, id)
		}, []DeploymentIntentDiagnosticStorage(nil), false},
		{"committed", func(ctx context.Context, owner any) (any, error) {
			return CountDeploymentCommittedOutcomesStorageForTest(ctx, owner, id)
		}, 0, false},
		{"receipts_count", func(ctx context.Context, owner any) (any, error) {
			return CountDeploymentPipelineReceiptsStorageForTest(ctx, owner, id)
		}, 0, false},
		{"successful_receipts", func(ctx context.Context, owner any) (any, error) {
			return CountRootReadySuccessfulPipelineReceiptsStorageForTest(ctx, owner, id)
		}, 0, false},
		{"receipts", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentReceiptStorageForTest(ctx, owner, id)
		}, []DeploymentReceiptStorage(nil), false},
		{"pending", func(ctx context.Context, owner any) (any, error) {
			return CountDeploymentPendingDeliveriesStorageForTest(ctx, owner, id)
		}, 0, false},
		{"failed", func(ctx context.Context, owner any) (any, error) {
			return CountDeploymentFailedDeliveriesStorageForTest(ctx, owner, id)
		}, 0, false},
		{"handoffs", func(ctx context.Context, owner any) (any, error) {
			return CountDeploymentStampedHandoffsStorageForTest(ctx, owner, id)
		}, 0, false},
		{"delivered", func(ctx context.Context, owner any) (any, error) {
			return CountRootReadyDeliveredStorageForTest(ctx, owner, id)
		}, 0, false},
		{"root_handoffs", func(ctx context.Context, owner any) (any, error) {
			return CountRootReadyStampedHandoffsStorageForTest(ctx, owner, id)
		}, 0, false},
		{"deliveries", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentDeliveryStorageForTest(ctx, owner, id)
		}, []DeploymentDeliveryStorage(nil), false},
		{"attempts", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentAttemptStorageForTest(ctx, owner, id)
		}, []DeploymentAttemptStorage(nil), false},
		{"delivery_statuses", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentDeliveryStatusStorageForTest(ctx, owner, id)
		}, []DeploymentDeliveryStatusStorage(nil), false},
		{"operation", func(ctx context.Context, owner any) (any, error) {
			return ReadSelectedDeploymentOperationStorageForTest(ctx, owner, id)
		}, SelectedDeploymentOperationStorage{}, true},
		{"executions", func(ctx context.Context, owner any) (any, error) {
			return ReadSelectedDeploymentExecutionsStorageForTest(ctx, owner, id)
		}, []SelectedDeploymentExecutionStorage(nil), false},
		{"closed", func(ctx context.Context, owner any) (any, error) {
			return CountClosedSelectedDeploymentExecutionsStorageForTest(ctx, owner, id)
		}, 0, false},
		{"run_status", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentRunStatusStorageForTest(ctx, owner, id)
		}, DeploymentRunStatusStorage{}, true},
		{"run_control", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentRunControlStorageForTest(ctx, owner, id)
		}, DeploymentRunControlStorage{}, true},
		{"parent", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentParentRunStorageForTest(ctx, owner, id)
		}, DeploymentNullableText{}, true},
		{"grants", func(ctx context.Context, owner any) (any, error) {
			return CountAdmittedUnboundBundleGrantsStorageForTest(ctx, owner, bundle)
		}, 0, false},
		{"first_event", func(ctx context.Context, owner any) (any, error) {
			return ReadFirstRootReadyEventStorageForTest(ctx, owner, id)
		}, "", true},
		{"root_events", func(ctx context.Context, owner any) (any, error) {
			return CountRunNamedEventStorageForTest(ctx, owner, id, "root.ready")
		}, 0, false},
		{"logs", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentRuntimeLogStorageForTest(ctx, owner, id)
		}, []DeploymentRuntimeLogStorage(nil), false},
		{"recent_events", func(ctx context.Context, owner any) (any, error) {
			return ReadDeploymentRecentEventStorageForTest(ctx, owner, id)
		}, []DeploymentRecentEventStorage(nil), false},
	}
}

func TestDeploymentObservationPortsKeepAll29IndependentNativeCutsAndExactEmptyPollBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newCompetingRevisionPairFixture(t, backend)
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range deploymentObservationReadCases(uuid.NewString(), "foreign-bundle") {
				got, err := tc.read(f.ctx, f.store)
				if (err != nil) != tc.absentError || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("foreign %s borrowed current storage or changed absence: %+v %v", tc.name, got, err)
				}
				if tc.absentError && !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("foreign %s changed missing-row refusal: %v", tc.name, err)
				}
				if tc.name == "first_event" && err != sql.ErrNoRows {
					t.Fatalf("empty poll lost its exact original sentinel: %T %v", err, err)
				}
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 29 || counts.Total.ReadCommits != 22 || counts.Total.WriteCommits != 0 {
				t.Fatalf("observation collapsed independent queries, wrote data or failed to settle empty poll: %+v", counts)
			}
			restore()
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("observations changed complete history or lifecycle clocks")
			}
		})
	}
}

func TestDeploymentObservationPortsRefuseRawCancelledInvalidAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, owner any, id string, cause error) {
		for _, tc := range deploymentObservationReadCases(id, "foreign-bundle") {
			got, err := tc.read(ctx, owner)
			if err == nil || cause != nil && !errors.Is(err, cause) || !reflect.DeepEqual(got, tc.empty) {
				t.Fatalf("unavailable %s returned evidence: %+v %v", tc.name, got, err)
			}
		}
	}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(context.Background(), owner, uuid.NewString(), nil)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			for _, tc := range deploymentObservationReadCases("invalid", "") {
				if got, err := tc.read(f.ctx, f.store); err == nil || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("invalid %s returned evidence: %+v %v", tc.name, got, err)
				}
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			refuse(cancelled, f.store, state.Identity.RunID, context.Canceled)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(f.ctx, f.store, state.Identity.RunID, nil)
		})
	}
}

func nativeDeploymentObservationFixture(t *testing.T, backend string) (receiverConfigActivationFixture, string, string) {
	t.Helper()
	selected, _, _ := newP16RaceStore(t, backend)
	f := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, map[string]string{
		"schema.yaml": "name: native-deployment-observation\npins:\n  outputs:\n    - root.ready\n",
		"events.yaml": "root.ready:\n  body: text\n",
	}, nil, ownStoreTestAgentManager, nil)
	data := selected.(interface {
		EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
		ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	})
	catalog, err := contracts.BuildDurableDataCatalog(f.bundle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := data.EnsureSourceArtifactWithData(f.ctx, f.bundle.SourceArtifact, catalog); err != nil {
		t.Fatal(err)
	}
	ref, err := durabledata.ParseDeclarationRef(".", "root.ready")
	if err != nil {
		t.Fatal(err)
	}
	imported, err := data.ExecuteDataSourceOperation(f.ctx, durabledata.SourceCommand{Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash,
		Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"body\":\"first\"}\n{\"body\":\"second\"}\n")})
	if err != nil || imported.Outcome != "accepted" {
		t.Fatalf("native data import: %+v %v", imported, err)
	}
	runID := uuid.NewString()
	f.ctx = correlation.WithRunID(f.ctx, runID)
	at := time.Now().UTC().Truncate(time.Microsecond)
	source := semanticview.Wrap(f.bundle)
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, OccurredAt: at,
		Instance: flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, "", "")})
	if err != nil {
		t.Fatal(err)
	}
	command := runtimebus.DeploymentRunCreationCommand{
		RunCreation: durabledata.RunCreationCommand{RunID: runID, Actor: "operator", BundleHash: catalog.BundleHash, Data: durabledata.RunCreationDataEnvelope{Pins: []durabledata.ExplicitPin{{Declaration: ref, VersionID: imported.Candidate.VersionID}}}},
		Idempotency: apiidempotency.Request{Method: "run.start", Actor: apiidempotency.BearerActor("operator"), Now: at, TTL: time.Hour},
		Root:        runtimebus.FlowInstanceActivationCommand{Plan: plan},
	}
	if result, err := selected.(runtimebus.DeploymentRunCreationCommitOwner).CommitDeploymentRunCreation(f.ctx, command); err != nil || !result.Acknowledged || result.Replay {
		t.Fatalf("native deployment: %+v %v", result, err)
	}
	return f, runID, string(imported.Candidate.VersionID)
}

func TestDeploymentObservationReadsLawfulPinsFeedsNullableMetadataAndControlBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, runID, version := nativeDeploymentObservationFixture(t, backend)
			before := competingRevisionPairSnapshot(t, f)
			pin, err := ReadRootReadyResourcePinStorageForTest(f.ctx, f.store, runID)
			if err != nil || pin != version {
				t.Fatalf("native pin changed scope/version: %q %v", pin, err)
			}
			intentVersion, err := ReadDeploymentIntentVersionStorageForTest(f.ctx, f.store, runID)
			if err != nil || intentVersion != version {
				t.Fatalf("native intent changed pinned version: %q %v", intentVersion, err)
			}
			var expected DeploymentIntentProgressStorage
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*),COALESCE(SUM(cursor),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&expected.Intents, &expected.Cursor); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadDeploymentIntentProgressStorageForTest(f.ctx, f.store, runID); err != nil || got != expected || got.Intents != 1 {
				t.Fatalf("native progress: %+v want=%+v %v", got, expected, err)
			}
			metadata, err := ReadDeploymentIntentMetadataStorageForTest(f.ctx, f.store, runID)
			if err != nil || metadata != deploymentMetadataStorageOracle(t, f, runID) || metadata.Origin != "deployment" || metadata.BundleHash != f.bundle.SourceArtifact.BundleHash() || !metadata.FeedID.Valid || metadata.FeedID.String == "" {
				t.Fatalf("native nullable metadata: %+v %v", metadata, err)
			}
			feeds, err := ReadDeploymentFeedStorageForTest(f.ctx, f.store, runID)
			if err != nil || !reflect.DeepEqual(feeds, deploymentFeedStorageOracle(t, f, runID)) || len(feeds) != 1 || feeds[0].FeedID != metadata.FeedID.String || feeds[0].Cardinality != 2 || feeds[0].Cursor != int64(expected.Cursor) || feeds[0].Status != metadata.Status {
				t.Fatalf("native ordered feed: %+v %v", feeds, err)
			}
			intents, err := ReadDeploymentIntentDiagnosticsStorageForTest(f.ctx, f.store, runID)
			if err != nil || len(intents) != 1 || intents[0].Cardinality != 2 || intents[0].Cursor != expected.Cursor || intents[0].Status != metadata.Status {
				t.Fatalf("native intent diagnostics: %+v %v", intents, err)
			}
			var status, hash string
			if err := f.db.QueryRowContext(f.ctx, `SELECT status,bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&status, &hash); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadDeploymentRunStatusStorageForTest(f.ctx, f.store, runID); err != nil || got.Status != status || got.BundleHash != hash {
				t.Fatalf("native run storage: %+v %v", got, err)
			}
			parent, err := ReadDeploymentParentRunStorageForTest(f.ctx, f.store, runID)
			if err != nil || parent.Valid || parent.String != "" {
				t.Fatalf("native NULL parent: %+v %v", parent, err)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("positive reads mutated complete storage or clocks")
			}
		})
	}
}

func deploymentMetadataStorageOracle(t *testing.T, f receiverConfigActivationFixture, runID string) DeploymentIntentMetadataStorage {
	t.Helper()
	var out DeploymentIntentMetadataStorage
	var source, feed, blocked, claimant, retry sql.NullString
	err := f.db.QueryRowContext(f.ctx, `SELECT status,bundle_hash,origin_kind,source_kind,deployment_feed_id,blocked_reason,claim_owner,retry_ready_at FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Status, &out.BundleHash, &out.Origin, &source, &feed, &blocked, &claimant, &retry)
	if err != nil {
		t.Fatal(err)
	}
	out.SourceKind, out.FeedID = DeploymentNullableText(source), DeploymentNullableText(feed)
	out.Blocked, out.Claimant, out.Retry = DeploymentNullableText(blocked), DeploymentNullableText(claimant), DeploymentNullableText(retry)
	return out
}

func deploymentFeedStorageOracle(t *testing.T, f receiverConfigActivationFixture, runID string) []DeploymentFeedStorage {
	t.Helper()
	rows, err := f.db.QueryContext(f.ctx, `SELECT CAST(deployment_feed_id AS TEXT),status,cursor,cardinality,CAST(claim_owner AS TEXT),claim_generation,CAST(lease_expires_at AS TEXT),CAST(retry_ready_at AS TEXT),CAST(retry_failure AS TEXT),CAST(blocked_reason AS TEXT) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' ORDER BY deployment_feed_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []DeploymentFeedStorage
	for rows.Next() {
		var row DeploymentFeedStorage
		var claimant, lease, retryAt, retryFailure, blocked sql.NullString
		if err := rows.Scan(&row.FeedID, &row.Status, &row.Cursor, &row.Cardinality, &claimant, &row.Generation, &lease, &retryAt, &retryFailure, &blocked); err != nil {
			t.Fatal(err)
		}
		row.Claimant, row.Lease, row.RetryAt = DeploymentNullableText(claimant), DeploymentNullableText(lease), DeploymentNullableText(retryAt)
		row.RetryFailure, row.Blocked = DeploymentNullableText(retryFailure), DeploymentNullableText(blocked)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
