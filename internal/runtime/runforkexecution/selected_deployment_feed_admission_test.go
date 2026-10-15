package runforkexecution

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/google/uuid"
)

func selectedDeploymentFeedRequest(runID string, declaration durabledata.DeclarationRef, versionByte string, cardinality int) fanoutobligation.IntentRequest {
	version := durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat(versionByte, 64))
	return fanoutobligation.IntentRequest{
		Key: fanoutobligation.IntentKey{RunID: runID, DeploymentFeedID: uuid.NewString()},
		Deployment: &fanoutobligation.DeploymentOrigin{
			BundleHash:  "bundle-v2:sha256:" + strings.Repeat("c", 64),
			Declaration: declaration, VersionID: version,
			SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("d", 64)),
		},
		Source:      fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion, Declaration: declaration, VersionID: version},
		Cardinality: cardinality,
	}
}

func selectedDeploymentSourcesForTest(feeds ...fanoutobligation.IntentRequest) []durabledata.PinnedSource {
	sources := make([]durabledata.PinnedSource, 0, len(feeds))
	for _, feed := range feeds {
		sources = append(sources, durabledata.PinnedSource{Declaration: feed.Deployment.Declaration,
			VersionID: feed.Deployment.VersionID, SchemaDigest: feed.Deployment.SchemaDigest, RowCount: feed.Cardinality})
	}
	return sources
}

func TestSelectedDeploymentFeedAgreementUsesFixedSourceDeclarations(t *testing.T) {
	sourceRun, forkRun := uuid.NewString(), uuid.NewString()
	first := durabledata.DeclarationRef{FlowPath: ".", EventName: "root.ready"}
	second := durabledata.DeclarationRef{FlowPath: "child", EventName: "child.ready"}
	sourceFirst := selectedDeploymentFeedRequest(sourceRun, first, "a", 0)
	sourceSecond := selectedDeploymentFeedRequest(sourceRun, second, "b", 2)
	plan := runfork.RunForkPlan{SourceRunID: sourceRun, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7}, FanOutObligations: []runfork.RunForkFanOutObligation{
		{Intent: fanoutobligation.Intent{Request: sourceFirst}},
		{Intent: fanoutobligation.Intent{Request: sourceSecond}},
	}}
	childFirst := selectedDeploymentFeedRequest(forkRun, first, "e", 3)
	childSecond := selectedDeploymentFeedRequest(forkRun, second, "f", 0)
	pins := selectedDeploymentSourcesForTest(childFirst, childSecond)
	bundle := childFirst.Deployment.BundleHash
	if err := validateSelectedDeploymentFeedAgreement(plan, []fanoutobligation.IntentRequest{childSecond, childFirst}, pins, forkRun, bundle); err != nil {
		t.Fatalf("changed pins and zero-row feed must retain exact declarations: %v", err)
	}
	if err := validateSelectedDeploymentFeedAgreement(runfork.RunForkPlan{}, nil, nil, forkRun, bundle); err != nil {
		t.Fatalf("event-only fork acquired deployment work: %v", err)
	}

	for _, tc := range []struct {
		name  string
		plan  runfork.RunForkPlan
		feeds []fanoutobligation.IntentRequest
	}{
		{"missing", plan, []fanoutobligation.IntentRequest{childFirst}},
		{"extra", plan, []fanoutobligation.IntentRequest{childFirst, childSecond, childFirst}},
		{"duplicate_child", plan, []fanoutobligation.IntentRequest{childFirst, childFirst}},
		{"wrong_source_run", runfork.RunForkPlan{SourceRunID: uuid.NewString(), FanOutObligations: plan.FanOutObligations}, []fanoutobligation.IntentRequest{childFirst, childSecond}},
		{"duplicate_source", runfork.RunForkPlan{SourceRunID: sourceRun, FanOutObligations: append(append([]runfork.RunForkFanOutObligation(nil), plan.FanOutObligations...), plan.FanOutObligations[0])}, []fanoutobligation.IntentRequest{childFirst, childSecond}},
		{"unbound_revision", runfork.RunForkPlan{SourceRunID: sourceRun, FanOutObligations: plan.FanOutObligations}, []fanoutobligation.IntentRequest{childFirst, childSecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateSelectedDeploymentFeedAgreement(tc.plan, tc.feeds, pins, forkRun, bundle); err == nil {
				t.Fatal("contradictory deployment feed agreement was accepted")
			}
		})
	}

	t.Run("wrong_child_run", func(t *testing.T) {
		if err := validateSelectedDeploymentFeedAgreement(plan, []fanoutobligation.IntentRequest{sourceFirst, childSecond}, pins, forkRun, bundle); err == nil {
			t.Fatal("source-run feed was accepted as child work")
		}
	})
	t.Run("wrong_bundle", func(t *testing.T) {
		if err := validateSelectedDeploymentFeedAgreement(plan, []fanoutobligation.IntentRequest{childFirst, childSecond}, pins, forkRun, "bundle-v2:sha256:"+strings.Repeat("f", 64)); err == nil {
			t.Fatal("other-bundle feed was accepted")
		}
	})
	t.Run("malformed_child", func(t *testing.T) {
		bad := childFirst
		bad.Deployment = &fanoutobligation.DeploymentOrigin{}
		if err := validateSelectedDeploymentFeedAgreement(plan, []fanoutobligation.IntentRequest{bad, childSecond}, pins, forkRun, bundle); err == nil {
			t.Fatal("malformed child feed was accepted")
		}
	})
	t.Run("missing_inherited_pin", func(t *testing.T) {
		if err := validateSelectedDeploymentFeedAgreement(plan, []fanoutobligation.IntentRequest{childFirst}, pins[:1], forkRun, bundle); err == nil {
			t.Fatal("missing inherited evidence became a fresh feed")
		}
	})
	t.Run("new_child_pin", func(t *testing.T) {
		fresh := plan
		fresh.FanOutObligations = nil
		if err := validateSelectedDeploymentFeedAgreement(fresh, []fanoutobligation.IntentRequest{childFirst, childSecond}, pins, forkRun, bundle); err != nil {
			t.Fatalf("exact new child pins were mistaken for source feeds: %v", err)
		}
	})
	for _, change := range []string{"version", "schema", "cardinality", "negative_cardinality", "declaration", "duplicate_pin"} {
		t.Run("child_pin_"+change, func(t *testing.T) {
			changed := append([]durabledata.PinnedSource(nil), pins...)
			switch change {
			case "version":
				changed[0].VersionID = childSecond.Deployment.VersionID
			case "schema":
				changed[0].SchemaDigest = durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("e", 64))
			case "cardinality":
				changed[0].RowCount++
			case "negative_cardinality":
				changed[0].RowCount = -1
			case "declaration":
				changed[0].Declaration.EventName = "other.ready"
			case "duplicate_pin":
				changed[1] = changed[0]
			}
			if err := validateSelectedDeploymentFeedAgreement(plan, []fanoutobligation.IntentRequest{childFirst, childSecond}, changed, forkRun, bundle); err == nil {
				t.Fatal("contradictory native child metadata was accepted")
			}
		})
	}
}

type selectedDeploymentPinReaderForTest struct {
	SelectedDeploymentFanOutStore
	source durabledata.PinnedSource
	err    error
	runID  string
	bundle string
	ref    durabledata.DeclarationRef
	calls  int
}

func (r *selectedDeploymentPinReaderForTest) LoadPinnedSource(_ context.Context, runID, bundle string, ref durabledata.DeclarationRef) (durabledata.PinnedSource, error) {
	r.runID, r.bundle, r.ref = runID, bundle, ref
	r.calls++
	return r.source, r.err
}

func TestSelectedDeploymentPinnedSourcesKeepNativeChildAuthority(t *testing.T) {
	runID := uuid.NewString()
	feed := selectedDeploymentFeedRequest(runID, durabledata.DeclarationRef{FlowPath: ".", EventName: "items.ready"}, "a", 1)
	source := selectedDeploymentSourcesForTest(feed)[0]
	pin := durabledata.Pin{RunID: runID, RunState: runfork.RunForkMaterializedStatus, Declaration: source.Declaration,
		VersionID: source.VersionID, SchemaDigest: source.SchemaDigest, Selection: "fork_override"}
	for _, change := range []string{"exact", "wrong_run", "wrong_version", "wrong_schema", "wrong_declaration", "reader_error"} {
		t.Run(change, func(t *testing.T) {
			reader := &selectedDeploymentPinReaderForTest{source: source}
			captured := pin
			switch change {
			case "wrong_run":
				captured.RunID = uuid.NewString()
			case "wrong_version":
				reader.source.VersionID = durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("b", 64))
			case "wrong_schema":
				reader.source.SchemaDigest = durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("b", 64))
			case "wrong_declaration":
				reader.source.Declaration.EventName = "other.ready"
			case "reader_error":
				reader.err = errors.New("canonical pin read failed")
			}
			container := selectedContractForkLocalRuntimeContainer{
				req: publishSelectedContractForkEventsRequest{ForkRunID: runID, DataPins: []durabledata.Pin{captured},
					LoadedSource: LoadedSelectedContractSource{SourceArtifactFact: testEphemeralSourceArtifactFact(feed.Deployment.BundleHash)}},
				ports: &selectedContractExecutionPorts{deploymentFanOut: reader},
			}
			got, err := container.selectedDeploymentPinnedSources(context.Background())
			if change == "exact" {
				if err != nil || len(got) != 1 || got[0] != source {
					t.Fatalf("exact canonical pin projection: sources=%+v err=%v", got, err)
				}
			} else if err == nil || len(got) != 0 {
				t.Fatalf("crossed pin authority accepted: sources=%+v err=%v", got, err)
			}
			if change == "wrong_run" {
				if reader.calls != 0 {
					t.Fatal("foreign child reached canonical pin lookup")
				}
			} else if reader.calls != 1 || reader.runID != runID || reader.bundle != feed.Deployment.BundleHash || reader.ref != pin.Declaration {
				t.Fatalf("canonical lookup changed original pin coordinates: %+v", reader)
			}
			if reader.err != nil && !errors.Is(err, reader.err) {
				t.Fatalf("canonical pin read error was lost: %v", err)
			}
		})
	}
}

func TestSelectedDeploymentRevisionFrontierRequiresExactFeedWork(t *testing.T) {
	sourceRun := uuid.NewString()
	declaration := durabledata.DeclarationRef{FlowPath: ".", EventName: "root.ready"}
	feed := selectedDeploymentFeedRequest(sourceRun, declaration, "a", 0)
	plan := runfork.RunForkPlan{
		SourceRunID: sourceRun, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 3},
		FanOutObligations: []runfork.RunForkFanOutObligation{{Intent: fanoutobligation.Intent{Request: feed}}},
	}
	zero := runfork.RunForkContractFrontierAdmission{}
	if err := admitSelectedDeploymentRevisionFrontier(plan, zero); err != nil {
		t.Fatalf("exact zero-row deployment feed was rejected: %v", err)
	}
	if err := admitSelectedDeploymentRevisionFrontier(runfork.RunForkPlan{}, runfork.RunForkContractFrontierAdmission{FrontierEventCount: 1}); err != nil {
		t.Fatalf("existing event-frontier execution was rejected: %v", err)
	}
	for _, tc := range []struct {
		name string
		plan runfork.RunForkPlan
	}{
		{"no_work", runfork.RunForkPlan{SourceRunID: sourceRun, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 3}}},
		{"event_point", runfork.RunForkPlan{SourceRunID: sourceRun, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: uuid.NewString(), Revision: 3}, FanOutObligations: plan.FanOutObligations}},
		{"unbound_revision", runfork.RunForkPlan{SourceRunID: sourceRun, FanOutObligations: plan.FanOutObligations}},
		{"wrong_source_run", runfork.RunForkPlan{SourceRunID: uuid.NewString(), ForkPoint: runfork.RunForkPoint{Revision: 3}, FanOutObligations: plan.FanOutObligations}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := admitSelectedDeploymentRevisionFrontier(tc.plan, zero); err == nil {
				t.Fatal("zero-frontier execution without exact deployment revision/feed work was admitted")
			}
		})
	}
}

func TestSelectedEventFrontierRecognizesOwedTimerWithoutGrantingExecution(t *testing.T) {
	plan := runfork.RunForkPlan{
		SourceRunID:         uuid.NewString(),
		ForkPoint:           runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: uuid.NewString(), Revision: 3},
		WorkflowTimers:      []timerobligation.WorkflowTimerActivationRecord{{Status: "active"}},
		UnsupportedBlockers: []runfork.RunForkUnsupportedBlocker{{Code: runfork.RunForkBlockerTimerHistoryUnproven}},
	}
	// This is availability only. Exact timer decoding and persisted child
	// readback must still succeed before native execution admission.
	if err := admitSelectedDeploymentRevisionFrontier(plan, runfork.RunForkContractFrontierAdmission{}); err != nil {
		t.Fatalf("owed event-cut timer was rejected before native admission: %v", err)
	}
	if plan.ExecutionReady || len(plan.UnsupportedBlockers) != 1 {
		t.Fatal("availability discharged native timer proof")
	}
	for _, status := range []string{"fired", "cancelled", ""} {
		plan.WorkflowTimers[0].Status = status
		if err := admitSelectedDeploymentRevisionFrontier(plan, runfork.RunForkContractFrontierAdmission{}); err == nil {
			t.Fatalf("terminal or absent timer invented owed work: %q", status)
		}
	}
	plan.WorkflowTimers[0].Status = "active"
	plan.ForkPoint.Revision = 0
	if err := admitSelectedDeploymentRevisionFrontier(plan, runfork.RunForkContractFrontierAdmission{}); err == nil {
		t.Fatal("unbound cut acquired availability")
	}
}
