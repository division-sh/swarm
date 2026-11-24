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
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestDeploymentSourceSelectedForkParksOtherFeedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range []string{"same_batch", "prior_batch", "settled_other", "partial_other", "legacy_recovery"} {
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
	names := []string{"scan.requested", "score.observed"}
	input := [][]byte{[]byte("{\"topic\":\"named\"}\n"), []byte("{\"label\":\"other\"}\n")}
	if variant == "settled_other" {
		input[1] = nil
	}
	sourceID := uuid.NewString()
	var f *deploymentResourceFixture
	var gate *selectedDeploymentClaimGate
	var sourceOther operatorread.OperatorEventListResult
	if variant == "partial_other" {
		input[1] = []byte("{\"label\":\"00-settled\"}\n{\"label\":\"01-pending\"}\n")
		gate = &selectedDeploymentClaimGate{eventName: names[1], allow: func(e events.Event) bool {
			return e.RunID() != sourceID || !bytes.Contains(e.Payload(), []byte(`"01-pending"`))
		}}
		gate.held.Store(true)
		f = newDeploymentResourceFixtureConfigured(t, backend, semanticview.Wrap(bundle), notifyAllChildrenRuntimeOptions{}, func(store runtimedelivery.Store) runtimedelivery.Store {
			gate.Store = store
			return gate
		})
	} else {
		f = newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle))
		f.runtime.fanOutServing.Close()
	}
	var batchID string
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
		if variant == "prior_batch" || variant == "partial_other" {
			publishNamedFeedBatch(t, f.ctx, server.URL, sourceID, data[1:])
			data = data[:1]
		}
		if variant == "partial_other" {
			sourceOther = waitNamedFeedPartialCut(t, f, sourceID, names[1], gate)
			f.runtime.fanOutServing.Close()
		}
		batchID = publishNamedFeedBatch(t, f.ctx, server.URL, sourceID, data)
	}
	planner := f.selected.(namedFeedForkPlanner)
	before, err := planner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: sourceID, PublicationID: batchID})
	if err != nil || len(before.FanOutObligations) != 2 || (variant != "partial_other" && before.EventCountAtFork != 0) {
		t.Fatalf("source cut must hold both feeds: %+v err=%v", before, err)
	}
	var named, other runfork.RunForkFanOutObligation
	for _, o := range before.FanOutObligations {
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
	otherCursor, otherCardinality := 0, 1
	if variant == "settled_other" {
		otherCardinality = 0
	}
	if variant == "partial_other" {
		otherCursor, otherCardinality = 2, 2
		requireNamedFeedPartialCut(t, other, sourceOther)
	}
	if named.Intent.Cursor != 0 || named.Intent.Request.Cardinality != 1 ||
		other.Intent.Cursor != otherCursor || other.Intent.Request.Cardinality != otherCardinality {
		t.Fatalf("source advanced before its fixed cut: named=%+v other=%+v", named.Intent, other.Intent)
	}
	pauseDeploymentFixtureRun(t, f, sourceID)
	params := map[string]any{
		"source_run_id": sourceID, "publication_id": named.Intent.Request.Key.DeploymentFeedID,
		"idempotency_key": uuid.NewString(),
	}
	var owner runforkexecution.SelectedContractExecutionOwner
	var recovery runfork.SelectedForkRecoveryResult
	if variant == "legacy_recovery" || variant == "partial_other" {
		owner, recovery = restartNamedFeedForkAtActivation(t, f, params)
	} else {
		var recovered []runfork.SelectedForkRecoveryResult
		owner, recovered = deploymentForkOwner(t, f)
		if len(recovered) != 0 {
			t.Fatalf("fresh selected owner found competing work: %+v", recovered)
		}
	}
	result, refused := deploymentForkRPC(t, f.ctx, deploymentForkServer(t, f, owner), params)
	if len(refused) != 0 || result.ForkRunID == "" || result.ForkRevision != before.ForkPoint.Revision ||
		(recovery.RunID != "" && result.ForkRunID != recovery.RunID) {
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
	wantChildFeeds, wantSourceFeeds := 1, 2
	if variant == "settled_other" {
		wantChildFeeds, wantSourceFeeds = 2, 1
		otherID := (durabledata.DeploymentFeed{RunID: result.ForkRunID, Publication: childBatchID, Declaration: other.Intent.Source.Declaration}).PublicationID()
		var retained int
		if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1
			AND deployment_feed_id=$2 AND status='closed' AND cardinality=0`, result.ForkRunID, otherID).Scan(&retained); err != nil || retained != 1 {
			t.Fatalf("named selection lost the other feed's settled history: retained=%d err=%v", retained, err)
		}
	}
	if variant == "partial_other" {
		wantSourceFeeds = 1
		requireRecoveredNamedFeedHistory(t, f.ctx, planner, recovery, before.ForkPoint.Revision, named, other)
		if after := deploymentResourcePublicEventsByName(t, f.ctx, f.operatorServer(t), sourceID, names[1], ""); !reflect.DeepEqual(sourceOther, after) {
			t.Fatal("named selection changed the other feed's source deliveries")
		}
	}
	if childFeeds != wantChildFeeds || namedRows != 1 || otherEvents != 0 || sourceFeeds != wantSourceFeeds || childRuns != 1 {
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

type namedFeedForkPlanner interface {
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
}

func waitNamedFeedPartialCut(t *testing.T, f *deploymentResourceFixture, runID, eventName string, gate *selectedDeploymentClaimGate) operatorread.OperatorEventListResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	server := f.operatorServer(t)
	for {
		cut := deploymentResourcePublicEventsByName(t, ctx, server, runID, eventName, "")
		status := map[string]string{}
		for _, e := range cut.Events {
			if label, ok := e.Payload["label"].(string); ok && len(e.Deliveries) == 1 {
				status[label] = e.Deliveries[0].Status
			}
		}
		if len(cut.Events) == 2 && cut.NextCursor == "" && gate.deferred.Load() > 0 &&
			status["00-settled"] == "delivered" && status["01-pending"] == "pending" {
			return cut
		}
		select {
		case <-ctx.Done():
			t.Fatalf("other feed did not reach a real partial cut: %+v", cut)
		case <-tick.C:
		}
	}
}

func requireNamedFeedPartialCut(t *testing.T, other runfork.RunForkFanOutObligation, cut operatorread.OperatorEventListResult) {
	t.Helper()
	if len(other.Outcomes) != 1 || len(other.PendingDeployment) != 1 {
		t.Fatalf("other feed needs one settled ordinal and one pending delivery: %+v", other)
	}
	settled, pending := other.Outcomes[0], other.PendingDeployment[0]
	for _, e := range cut.Events {
		switch e.Payload["label"] {
		case "00-settled":
			if settled.Ordinal != 0 || settled.SourceEventID != e.EventID || settled.EventID != "" ||
				settled.Kind != fanoutobligation.OutcomeCommitted || settled.InheritedDisposition != fanoutobligation.InheritedSucceeded {
				t.Fatalf("settled source ordinal lost attribution: %+v", settled)
			}
		case "01-pending":
			if pending.Ordinal != 1 || pending.SourceEventID != e.EventID || pending.Phase != runfork.RunForkDeploymentPendingReceiver ||
				!reflect.DeepEqual(pending.SourceDeliveryIDs, []string{e.Deliveries[0].DeliveryID}) {
				t.Fatalf("pending source ordinal is not the held receiver: %+v", pending)
			}
		}
	}
}

func requireRecoveredNamedFeedHistory(t *testing.T, ctx context.Context, planner namedFeedForkPlanner, recovery runfork.SelectedForkRecoveryResult, revision int64, named, other runfork.RunForkFanOutObligation) {
	t.Helper()
	record := recovery.Operation
	if record == nil || record.ForkRunID != recovery.RunID {
		t.Fatalf("recovery lacks the exact child operation: %+v", recovery)
	}
	op := record.Request
	if op.PublicationID != named.Intent.Request.Key.DeploymentFeedID || op.ResolvedPoint == nil || op.ResolvedPoint.Revision != revision {
		t.Fatalf("recovery changed the named feed or fixed cut: %+v", op)
	}
	plan, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: op.SourceRunID, PublicationID: op.PublicationID, ResolvedPoint: op.ResolvedPoint})
	if err != nil || plan.DeploymentFeedSelection == nil || plan.DeploymentFeedSelection.Named != op.PublicationID ||
		len(plan.DeploymentFeedSelection.Parked) != 1 || !reflect.DeepEqual(plan.DeploymentFeedSelection.Parked[0], other) {
		t.Fatalf("child recovery cut lost the other feed's ordinal attribution or exact tail: %+v err=%v", plan.DeploymentFeedSelection, err)
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

func restartNamedFeedForkAtActivation(t *testing.T, f *deploymentResourceFixture, params map[string]any) (runforkexecution.SelectedContractExecutionOwner, runfork.SelectedForkRecoveryResult) {
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
			return restarted, recovered
		}
	}
	t.Fatalf("recovery did not retain the exact materialized child %s: %+v", childID, recovery)
	return restarted, runfork.SelectedForkRecoveryResult{}
}
