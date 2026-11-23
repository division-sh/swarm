package conformance

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestDeploymentSourceSelectedForkParksOtherFeedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range []string{"same_batch", "prior_batch", "legacy_recovery"} {
			t.Run(backend+"/"+variant, func(t *testing.T) {
				proveSelectedNamedFeedFork(t, backend, variant)
			})
		}
	}
}

func proveSelectedNamedFeedFork(t *testing.T, backend, variant string) {
	t.Helper()
	repo := conformanceRepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyPublicCompletionFeedOnly(t), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	f := newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle))
	f.runtime.fanOutServing.Close()
	names := []string{"scan.requested", "score.observed"}
	input := [][]byte{[]byte("{\"topic\":\"named\"}\n"), []byte("{\"label\":\"other\"}\n")}
	var sourceID, batchID string
	if variant == "legacy_recovery" {
		var data durabledata.RunCreationDataEnvelope
		for i, name := range names {
			ref, err := durabledata.ParseDeclarationRef(".", name)
			if err != nil {
				t.Fatal(err)
			}
			data.Imports = append(data.Imports, durabledata.FusedImport{
				Declaration: ref, SourceInvocationID: uuid.NewString(),
				ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: input[i],
			})
		}
		sourceID = createDeploymentResourceFixtureRun(t, f, data)
	} else {
		sourceID = uuid.NewString()
		prepareDeploymentRowsFixtureRun(t, f, sourceID, ".")
		server := f.operatorServer(t)
		dir := t.TempDir()
		var data []string
		for i, name := range names {
			path := filepath.Join(dir, name+".jsonl")
			if err := os.WriteFile(path, input[i], 0600); err != nil {
				t.Fatal(err)
			}
			data = append(data, name+"="+path)
		}
		if variant == "prior_batch" {
			publishNamedFeedBatch(t, f.ctx, server.URL, sourceID, data[1:])
			data = data[:1]
		}
		batchID = publishNamedFeedBatch(t, f.ctx, server.URL, sourceID, data)
	}
	planner := f.selected.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	})
	before, err := planner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: sourceID, PublicationID: batchID})
	if err != nil || len(before.FanOutObligations) != 2 || before.EventCountAtFork != 0 {
		t.Fatalf("source cut must hold both unissued feeds: %+v err=%v", before, err)
	}
	var named, other runfork.RunForkFanOutObligation
	for _, o := range before.FanOutObligations {
		if o.Intent.Cursor != 0 || o.Intent.Request.Cardinality != 1 {
			t.Fatalf("source advanced before its fixed cut: %+v", o)
		}
		switch o.Intent.Source.Declaration.EventName {
		case names[0]:
			named = o
		case names[1]:
			other = o
		}
	}
	if named.Intent.Request.Key.DeploymentFeedID == "" || other.Intent.Request.Key.DeploymentFeedID == "" {
		t.Fatal("fixture lacks exact named and other source members")
	}
	pauseDeploymentFixtureRun(t, f, sourceID)
	params := map[string]any{
		"source_run_id": sourceID, "publication_id": named.Intent.Request.Key.DeploymentFeedID,
		"idempotency_key": uuid.NewString(),
	}
	var owner runforkexecution.SelectedContractExecutionOwner
	var recoveredID string
	if variant == "legacy_recovery" {
		owner, recoveredID = restartNamedFeedForkAtActivation(t, f, params)
	} else {
		var recovered []runfork.SelectedForkRecoveryResult
		owner, recovered = deploymentForkOwner(t, f)
		if len(recovered) != 0 {
			t.Fatalf("fresh selected owner found competing work: %+v", recovered)
		}
	}
	result, refused := deploymentForkRPC(t, f.ctx, deploymentForkServer(t, f, owner), params)
	if len(refused) != 0 || result.ForkRunID == "" || result.ForkRevision != before.ForkPoint.Revision ||
		(recoveredID != "" && result.ForkRunID != recoveredID) {
		t.Fatalf("named feed fork: %+v refusal=%s", result, refused)
	}
	retry, refused := deploymentForkRPC(t, f.ctx, deploymentForkServer(t, f, owner), params)
	if len(refused) != 0 || !reflect.DeepEqual(retry, result) {
		t.Fatalf("reconstructed handler changed the acknowledged fork: %+v refusal=%s", retry, refused)
	}
	childBatchID := ""
	if batchID != "" {
		childBatchID = durabledata.ForkRowsPublicationID(result.ForkRunID, batchID)
	}
	childMemberID := (durabledata.DeploymentFeed{RunID: result.ForkRunID, Publication: childBatchID, Declaration: named.Intent.Source.Declaration}).PublicationID()
	var cursor int
	var selectedVersion durabledata.VersionID
	if err := f.db.QueryRowContext(f.ctx, `SELECT cursor,source_resource_version_id FROM fan_out_intents
		WHERE run_id=$1 AND deployment_feed_id=$2`, result.ForkRunID, childMemberID).Scan(&cursor, &selectedVersion); err != nil ||
		cursor != 1 || selectedVersion != named.Intent.Source.VersionID {
		t.Fatalf("exact child member %s: cursor=%d version=%s err=%v", childMemberID, cursor, selectedVersion, err)
	}
	var childFeeds, namedRows, otherEvents, sourceFeeds, childRuns int
	for _, check := range []struct {
		id, query string
		got       *int
	}{
		{result.ForkRunID, "SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'", &childFeeds},
		{result.ForkRunID, "SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND source_resource_event_name='scan.requested' AND cursor=1 AND status='closed'", &namedRows},
		{result.ForkRunID, "SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='score.observed'", &otherEvents},
		{sourceID, "SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' AND cursor=0 AND status='open'", &sourceFeeds},
		{sourceID, "SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1", &childRuns},
	} {
		if err := f.db.QueryRowContext(f.ctx, check.query, check.id).Scan(check.got); err != nil {
			t.Fatal(err)
		}
	}
	if childFeeds != 1 || namedRows != 1 || otherEvents != 0 || sourceFeeds != 2 || childRuns != 1 {
		t.Fatalf("named-only work or source custody changed: child feeds=%d named settled=%d other events=%d source feeds=%d children=%d", childFeeds, namedRows, otherEvents, sourceFeeds, childRuns)
	}
	var delivered int
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_outcomes o
		JOIN event_deliveries d ON d.event_id=o.event_id AND d.run_id=o.run_id
		WHERE o.run_id=$1 AND o.deployment_feed_id=$2 AND o.ordinal=0 AND o.outcome_kind='committed'
		AND d.status='delivered' AND d.claim_version=1`, result.ForkRunID, childMemberID).Scan(&delivered); err != nil || delivered != 1 {
		t.Fatalf("named ordinal did not settle exactly once: delivered=%d err=%v", delivered, err)
	}
}

func publishNamedFeedBatch(t *testing.T, ctx context.Context, endpoint, runID string, data []string) string {
	t.Helper()
	args := []string{"run", "start", "--connect", endpoint, "--run-id", runID, "--idempotency-key", uuid.NewString(), "--no-follow"}
	for _, input := range data {
		args = append(args, "--data", input)
	}
	var stdout, stderr bytes.Buffer
	if code := cliapp.Execute(ctx, args, &stdout, &stderr, nil, nil); code != 0 {
		t.Fatalf("commit source feed batch: exit=%d stderr=%s", code, stderr.String())
	}
	_, receipt, ok := strings.Cut(stdout.String(), "rows published: publication_id=")
	batchID, _, _ := strings.Cut(receipt, "\n")
	if _, err := uuid.Parse(batchID); !ok || err != nil {
		t.Fatalf("source batch receipt: %s err=%v", stdout.String(), err)
	}
	return batchID
}

func restartNamedFeedForkAtActivation(t *testing.T, f *deploymentResourceFixture, params map[string]any) (runforkexecution.SelectedContractExecutionOwner, string) {
	t.Helper()
	cut := selectedDeploymentBeforeActivationCrash{
		SelectedContractForkLifecycle: f.selected.(runforkexecution.SelectedContractForkLifecycle),
		entered:                       make(chan string, 1),
	}
	owner, recovered := deploymentForkOwnerWithOverrides(t, f, nil, cut)
	if len(recovered) != 0 {
		t.Fatalf("fresh selected owner found competing work: %+v", recovered)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	_, rpcErr, transportErr := deploymentForkRPCRequest(ctx, deploymentForkServer(t, f, owner), params)
	if transportErr == nil && len(rpcErr) == 0 {
		t.Fatal("interrupted activation returned success")
	}
	var childID string
	select {
	case childID = <-cut.entered:
	default:
		t.Fatalf("did not reach pre-activation cut: refusal=%s transport=%v", rpcErr, transportErr)
	}
	if err := owner.RetireSelectedContexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldRuntime := f.runtime
	assertServingJoinComplete(t, beginServingLifetimeJoin(oldRuntime, nil), oldRuntime, nil)
	if err := f.topology.capability.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.ctx = testAuthorActivityContextForBundle(context.Background(), f.runtime.sourceArtifactFact)
	f.topology = newNotifyAllChildrenProcessTopology(t, f.ctx, f.selected, f.source)
	f.ctx = originalConformanceFixtureContext(t, f.ctx, f.selected)
	f.boot(t)
	restarted, recovery := deploymentForkOwner(t, f)
	for _, recovered := range recovery {
		if recovered.RunID == childID {
			return restarted, childID
		}
	}
	t.Fatalf("recovery did not retain the exact materialized child %s: %+v", childID, recovery)
	return restarted, ""
}
