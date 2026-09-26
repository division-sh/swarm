package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
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
	bundle := childFirst.Deployment.BundleHash
	if err := validateSelectedDeploymentFeedAgreement(plan, []fanoutobligation.IntentRequest{childSecond, childFirst}, forkRun, bundle); err != nil {
		t.Fatalf("changed pins and zero-row feed must retain exact declarations: %v", err)
	}
	if err := validateSelectedDeploymentFeedAgreement(runfork.RunForkPlan{}, nil, forkRun, bundle); err != nil {
		t.Fatalf("event-only fork acquired deployment work: %v", err)
	}

	for _, tc := range []struct {
		name  string
		plan  runfork.RunForkPlan
		feeds []fanoutobligation.IntentRequest
	}{
		{"missing", plan, []fanoutobligation.IntentRequest{childFirst}},
		{"extra", runfork.RunForkPlan{}, []fanoutobligation.IntentRequest{childFirst}},
		{"duplicate_child", plan, []fanoutobligation.IntentRequest{childFirst, childFirst}},
		{"wrong_source_run", runfork.RunForkPlan{SourceRunID: uuid.NewString(), FanOutObligations: plan.FanOutObligations}, []fanoutobligation.IntentRequest{childFirst, childSecond}},
		{"duplicate_source", runfork.RunForkPlan{SourceRunID: sourceRun, FanOutObligations: append(append([]runfork.RunForkFanOutObligation(nil), plan.FanOutObligations...), plan.FanOutObligations[0])}, []fanoutobligation.IntentRequest{childFirst, childSecond}},
		{"unbound_revision", runfork.RunForkPlan{SourceRunID: sourceRun, FanOutObligations: plan.FanOutObligations}, []fanoutobligation.IntentRequest{childFirst, childSecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateSelectedDeploymentFeedAgreement(tc.plan, tc.feeds, forkRun, bundle); err == nil {
				t.Fatal("contradictory deployment feed agreement was accepted")
			}
		})
	}

	t.Run("wrong_child_run", func(t *testing.T) {
		if err := validateSelectedDeploymentFeedAgreement(plan, []fanoutobligation.IntentRequest{sourceFirst, childSecond}, forkRun, bundle); err == nil {
			t.Fatal("source-run feed was accepted as child work")
		}
	})
	t.Run("wrong_bundle", func(t *testing.T) {
		if err := validateSelectedDeploymentFeedAgreement(plan, []fanoutobligation.IntentRequest{childFirst, childSecond}, forkRun, "bundle-v2:sha256:"+strings.Repeat("f", 64)); err == nil {
			t.Fatal("other-bundle feed was accepted")
		}
	})
	t.Run("malformed_child", func(t *testing.T) {
		bad := childFirst
		bad.Deployment = &fanoutobligation.DeploymentOrigin{}
		if err := validateSelectedDeploymentFeedAgreement(plan, []fanoutobligation.IntentRequest{bad, childSecond}, forkRun, bundle); err == nil {
			t.Fatal("malformed child feed was accepted")
		}
	})
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
