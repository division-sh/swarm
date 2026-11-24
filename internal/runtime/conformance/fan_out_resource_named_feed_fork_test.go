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
	"github.com/division-sh/swarm/internal/store/storetest"
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
		f = newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle), notifyAllChildrenRuntimeOptions{wrapDeploymentDelivery: func(store runtimedelivery.Store) runtimedelivery.Store {
			gate.Store = store
			return gate
		}})
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
	childPlan, err := planner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: result.ForkRunID})
	if err != nil {
		t.Fatalf("read exact child intent versions: %v", err)
	}
	namedRows := 0
	for _, o := range childPlan.FanOutObligations {
		intent := o.Intent
		if intent.Source.Declaration.EventName == names[0] && intent.Cursor == 1 && intent.Status == fanoutobligation.StatusClosed {
			namedRows++
			if intent.Request.Key.DeploymentFeedID != childMemberID || intent.Source.VersionID != named.Intent.Source.VersionID {
				t.Fatalf("wrong named child member or exact version: %+v", intent)
			}
		}
	}
	childIntents, sourceIntents := namedFeedInventory(t, f, result.ForkRunID), namedFeedInventory(t, f, sourceID)
	if intent := childIntents[childMemberID]; intent.Cursor != 1 || intent.Status != fanoutobligation.StatusClosed {
		t.Fatalf("named child member is not closed at ordinal 1: %+v", intent)
	}
	sourceFeeds := 0
	for _, intent := range sourceIntents {
		if intent.Cursor == 0 && intent.Status == fanoutobligation.StatusOpen {
			sourceFeeds++
		}
	}
	otherEvents, err := storetest.CountRunEventNameStorage(f.ctx, f.selected, result.ForkRunID, names[1])
	if err != nil {
		t.Fatal(err)
	}
	headers, next, err := f.selected.(operatorread.RunReader).ListRunHeaders(f.ctx, operatorread.RunHeaderListOptions{})
	if err != nil || next != "" {
		t.Fatalf("run inventory must be complete: next=%q err=%v", next, err)
	}
	var children []string
	for _, h := range headers {
		if h.Origin.SourceRunID() == sourceID {
			children = append(children, h.RunID)
		}
	}
	wantChildFeeds, wantSourceFeeds := 1, 2
	if variant == "settled_other" {
		wantChildFeeds, wantSourceFeeds = 2, 1
		otherID := (durabledata.DeploymentFeed{RunID: result.ForkRunID, Publication: childBatchID, Declaration: other.Intent.Source.Declaration}).PublicationID()
		if retained := childIntents[otherID]; retained.Status != fanoutobligation.StatusClosed || retained.Cardinality != 0 {
			t.Fatalf("named selection lost the other feed's settled history: %+v", retained)
		}
	}
	if variant == "partial_other" {
		wantSourceFeeds = 1
		requireRecoveredNamedFeedHistory(t, f.ctx, planner, recovery, before.ForkPoint.Revision, named, other)
		if after := deploymentResourcePublicEventsByName(t, f.ctx, f.operatorServer(t), sourceID, names[1], ""); !reflect.DeepEqual(sourceOther, after) {
			t.Fatal("named selection changed the other feed's source deliveries")
		}
	}
	if len(childIntents) != wantChildFeeds || namedRows != 1 || otherEvents != 0 || sourceFeeds != wantSourceFeeds ||
		!reflect.DeepEqual(children, []string{result.ForkRunID}) {
		t.Fatalf("named-only work or source custody changed: child feeds=%d named settled=%d other events=%d source feeds=%d children=%v", len(childIntents), namedRows, otherEvents, sourceFeeds, children)
	}
	requireNamedFeedDelivery(t, f, result.ForkRunID, childMemberID)
	if variant == "legacy_recovery" {
		requireDefaultDescendantRetainsParking(t, f, owner, result.ForkRunID, named, other)
	}
}

func requireDefaultDescendantRetainsParking(t *testing.T, f *deploymentResourceFixture, owner runforkexecution.SelectedContractExecutionOwner, sourceID string, named, other runfork.RunForkFanOutObligation) {
	t.Helper()
	bundleHash := f.runtime.sourceArtifactFact.BundleHash()
	data := f.selected.(interface {
		LoadPinnedSource(context.Context, string, string, durabledata.DeclarationRef) (durabledata.PinnedSource, error)
	})
	for depth := 0; depth < 2; depth++ {
		pin, err := data.LoadPinnedSource(f.ctx, sourceID, bundleHash, other.Intent.Source.Declaration)
		if err != nil || pin.VersionID != other.Intent.Source.VersionID || pin.RowCount != 1 {
			t.Fatalf("descendant source must retain the real parked legacy pin: %+v err=%v", pin, err)
		}
		pauseDeploymentFixtureRun(t, f, sourceID)
		result, refused := deploymentForkRPC(t, f.ctx, deploymentForkServer(t, f, owner), map[string]any{
			"source_run_id": sourceID, "idempotency_key": uuid.NewString(),
		})
		if len(refused) != 0 || result.ForkRunID == "" {
			t.Fatalf("default descendant at depth %d: %+v refusal=%s", depth, result, refused)
		}
		feedID := func(declaration durabledata.DeclarationRef) string {
			return (durabledata.DeploymentFeed{RunID: result.ForkRunID, Declaration: declaration}).PublicationID()
		}
		intents := namedFeedInventory(t, f, result.ForkRunID)
		kept := intents[feedID(named.Intent.Source.Declaration)]
		if _, resurrected := intents[feedID(other.Intent.Source.Declaration)]; resurrected || len(intents) != 1 ||
			kept.Cursor != 1 || kept.Status != fanoutobligation.StatusClosed {
			t.Fatalf("default descendant must retain the settled legacy feed, not resurrect the parked one: depth=%d feeds=%+v", depth, intents)
		}
		sourceID = result.ForkRunID
	}
}

func namedFeedInventory(t *testing.T, f *deploymentResourceFixture, runID string) map[string]fanoutobligation.IntentReadback {
	t.Helper()
	page, err := f.selected.(operatorread.FanOutReader).ListFanOutIntents(f.ctx, fanoutobligation.ListQuery{RunID: runID})
	if err != nil || page.RunID != runID || page.NextCursor != "" {
		t.Fatalf("feed inventory must be complete for %s: %+v err=%v", runID, page, err)
	}
	intents := make(map[string]fanoutobligation.IntentReadback)
	for _, intent := range page.Intents {
		if id := intent.Key.DeploymentFeedID; id != "" {
			if _, exists := intents[id]; exists || intent.Key.RunID != runID {
				t.Fatalf("duplicate or foreign feed: %+v", intent)
			}
			intents[id] = intent
		}
	}
	return intents
}

func requireNamedFeedDelivery(t *testing.T, f *deploymentResourceFixture, runID, memberID string) {
	t.Helper()
	zero := 0
	page, err := f.selected.(operatorread.FanOutReader).ListFanOutIntents(f.ctx, fanoutobligation.ListQuery{
		RunID: runID, OutcomeFromOrdinal: &zero, Filter: fanoutobligation.ListFilter{DeploymentFeedID: memberID},
	})
	if err != nil || page.DeploymentOutcomes == nil || len(page.DeploymentOutcomes.Outcomes) != 1 {
		t.Fatalf("missing exact named ordinal: %+v err=%v", page, err)
	}
	outcome := page.DeploymentOutcomes.Outcomes[0]
	if outcome.Ordinal != 0 || outcome.Kind != fanoutobligation.OutcomeCommitted || outcome.EventID == "" {
		t.Fatalf("wrong named ordinal outcome: %+v", outcome)
	}
	event, err := f.selected.(operatorread.ObservabilityReader).LoadOperatorEvent(f.ctx, outcome.EventID)
	if err != nil || event.EventID != outcome.EventID || event.RunID != runID {
		t.Fatalf("wrong named ordinal event: %+v err=%v", event, err)
	}
	var delivered int
	for _, delivery := range event.Deliveries {
		snapshot, err := f.selected.Snapshot(f.ctx, delivery.DeliveryID)
		if err != nil || snapshot.DeliveryID != delivery.DeliveryID || snapshot.EventID != event.EventID || snapshot.RunID != runID {
			t.Fatalf("wrong named ordinal delivery: %+v err=%v", snapshot, err)
		}
		if snapshot.Status == runtimedelivery.StatusDelivered && snapshot.ClaimVersion == 1 {
			delivered++
		}
	}
	if delivered != 1 {
		t.Fatalf("named ordinal did not settle exactly once: delivered=%d", delivered)
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
