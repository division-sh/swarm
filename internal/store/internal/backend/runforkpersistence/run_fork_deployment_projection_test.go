package runforkpersistence

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestRunForkFixedRevisionDeploymentFeedProjection(t *testing.T) {
	runID, feedID := uuid.NewString(), uuid.NewString()
	at := time.Now().UTC()
	fact := runForkRevisionFanOutFact{
		FactKind: "intent", OriginKind: string(fanoutobligation.OriginDeployment), DeploymentFeedID: feedID,
		BundleHash:             "bundle-v2:sha256:" + strings.Repeat("a", 64),
		DeploymentSchemaDigest: "resource-schema-v1:sha256:" + strings.Repeat("b", 64),
		SourceKind:             string(fanoutobligation.SourceResourceVersion),
		SourceResourceFlowPath: ".", SourceResourceEventName: "items.ready",
		SourceResourceVersionID: "resource-version-v1:sha256:" + strings.Repeat("c", 64),
		Cardinality:             3, Cursor: 0, Status: string(fanoutobligation.StatusOpen), CreatedAt: at,
	}
	snapshot := &runForkRevisionSnapshot{RunID: runID, FanOutFacts: []runForkRevisionFanOutFact{fact}}
	obligations, err := loadRunForkFanOutObligationsFromRevision(snapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(obligations) != 1 || obligations[0].Intent.Request.Key != (fanoutobligation.IntentKey{RunID: runID, DeploymentFeedID: feedID}) ||
		obligations[0].Intent.Request.Deployment == nil || obligations[0].Intent.Request.Deployment.VersionID != obligations[0].Intent.Source.VersionID {
		t.Fatalf("fixed deployment projection lost exact source: %+v", obligations)
	}
	for _, mutate := range []func(*runForkRevisionFanOutFact){
		func(f *runForkRevisionFanOutFact) { f.TriggeringDeliveryID = uuid.NewString() },
		func(f *runForkRevisionFanOutFact) { f.DeploymentSchemaDigest = "" },
		func(f *runForkRevisionFanOutFact) { f.Capsule = []byte(`{"payload":{}}`) },
		func(f *runForkRevisionFanOutFact) { f.SemanticDigest = "borrowed-handler-plan" },
	} {
		bad := fact
		mutate(&bad)
		if _, err := loadRunForkFanOutObligationsFromRevision(&runForkRevisionSnapshot{RunID: runID, FanOutFacts: []runForkRevisionFanOutFact{bad}}, nil); err == nil {
			t.Fatalf("hostile deployment revision fact accepted: %+v", bad)
		}
	}
}

func TestForkDeploymentCarriageMatchesExactPinVersion(t *testing.T) {
	ref := durabledata.DeclarationRef{FlowPath: ".", EventName: "items.ready"}
	version := durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("c", 64))
	schema := durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("b", 64))
	key := fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()}
	now := time.Now().UTC()
	intent := fanoutobligation.Intent{
		Request: fanoutobligation.IntentRequest{
			Key: key, Deployment: &fanoutobligation.DeploymentOrigin{
				BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), Declaration: ref,
				VersionID: version, SchemaDigest: schema,
			},
			Source: fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion, Declaration: ref, VersionID: version}, Cardinality: 2,
		},
		Source: fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion, Declaration: ref, VersionID: version},
		Cursor: 1, Status: fanoutobligation.StatusOpen, NextChunkSize: fanoutobligation.InitialChunkSize,
		CreatedAt: now, UpdatedAt: now,
	}
	obligation := runfork.RunForkFanOutObligation{Intent: intent, Outcomes: []fanoutobligation.Outcome{{
		Ordinal: 0, Kind: fanoutobligation.OutcomeCommitted, SourceEventID: uuid.NewString(),
		InheritedDisposition: fanoutobligation.InheritedNoRoute, CreatedAt: now,
	}}}
	for _, test := range []struct {
		name     string
		target   durabledata.PinnedSource
		inherit  bool
		cursor   int
		status   fanoutobligation.Status
		outcomes int
		reject   bool
	}{
		{"same-pin", durabledata.PinnedSource{Declaration: ref, VersionID: version, SchemaDigest: schema, RowCount: 2}, true, 1, fanoutobligation.StatusOpen, 1, false},
		{"changed-pin", durabledata.PinnedSource{Declaration: ref, VersionID: durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("d", 64)), SchemaDigest: schema, RowCount: 3}, false, 0, fanoutobligation.StatusOpen, 0, false},
		{"changed-empty-pin", durabledata.PinnedSource{Declaration: ref, VersionID: durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("d", 64)), SchemaDigest: schema, RowCount: 0}, false, 0, fanoutobligation.StatusClosed, 0, false},
		{"same-version-new-cardinality", durabledata.PinnedSource{Declaration: ref, VersionID: version, SchemaDigest: schema, RowCount: 3}, false, 0, "", 0, true},
		{"negative-cardinality", durabledata.PinnedSource{Declaration: ref, VersionID: version, SchemaDigest: schema, RowCount: -1}, false, 0, "", 0, true},
		{"different-declaration", durabledata.PinnedSource{Declaration: durabledata.DeclarationRef{FlowPath: ".", EventName: "other"}, VersionID: version, SchemaDigest: schema, RowCount: 2}, false, 0, "", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := projectForkDeploymentCarriage(obligation, test.target)
			if (err != nil) != test.reject {
				t.Fatalf("carriage err=%v reject=%t", err, test.reject)
			}
			if !test.reject && (got.inherit != test.inherit || got.cursor != test.cursor || got.status != test.status || len(got.outcomes) != test.outcomes) {
				t.Fatalf("carriage=%+v", got)
			}
		})
	}
}

func TestFreshForkDeploymentCarriageUsesExactChildCardinality(t *testing.T) {
	for _, count := range []int{-1, 0, 1, 32} {
		got, err := freshForkDeploymentCarriage(durabledata.PinnedSource{RowCount: count})
		if count < 0 {
			if err == nil {
				t.Fatal("negative child cardinality became a fresh feed")
			}
			continue
		}
		want := fanoutobligation.StatusOpen
		if count == 0 {
			want = fanoutobligation.StatusClosed
		}
		if err != nil || got.status != want || got.inherit || got.cursor != 0 || got.blockedReason != "" || len(got.outcomes) != 0 {
			t.Fatalf("fresh carriage for %d rows invented inherited progress: %+v err=%v", count, got, err)
		}
	}
}

func TestRunForkFixedRevisionDeploymentTerminalPrefix(t *testing.T) {
	runID, feedID, sourceEventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	at := time.Now().UTC()
	intent := runForkRevisionFanOutFact{
		FactKind: "intent", OriginKind: string(fanoutobligation.OriginDeployment), DeploymentFeedID: feedID,
		BundleHash:             "bundle-v2:sha256:" + strings.Repeat("a", 64),
		DeploymentSchemaDigest: "resource-schema-v1:sha256:" + strings.Repeat("b", 64),
		SourceKind:             string(fanoutobligation.SourceResourceVersion),
		SourceResourceFlowPath: ".", SourceResourceEventName: "items.ready",
		SourceResourceVersionID: "resource-version-v1:sha256:" + strings.Repeat("c", 64),
		Cardinality:             2, Cursor: 1, Status: string(fanoutobligation.StatusOpen), CreatedAt: at,
	}
	ordinal := 0
	outcome := runForkRevisionFanOutFact{
		FactKind: "outcome", OriginKind: string(fanoutobligation.OriginDeployment), DeploymentFeedID: feedID,
		Ordinal: &ordinal, OutcomeKind: string(fanoutobligation.OutcomeCommitted),
		SourceOutcomeEventID: sourceEventID, InheritedDisposition: string(fanoutobligation.InheritedNoRoute), CreatedAt: at,
	}
	snapshot := &runForkRevisionSnapshot{RunID: runID, FanOutFacts: []runForkRevisionFanOutFact{outcome, intent}}
	obligations, err := loadRunForkFanOutObligationsFromRevision(snapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(obligations) != 1 || obligations[0].Intent.Cursor != 1 || len(obligations[0].Outcomes) != 1 ||
		obligations[0].Outcomes[0].SourceEventID != sourceEventID || obligations[0].Intent.Request.Key.DeploymentFeedID != feedID {
		t.Fatalf("deployment terminal prefix lost: %+v", obligations)
	}
	other := outcome
	other.DeploymentFeedID = uuid.NewString()
	if _, err := loadRunForkFanOutObligationsFromRevision(&runForkRevisionSnapshot{RunID: runID, FanOutFacts: []runForkRevisionFanOutFact{intent, other}}, nil); err == nil {
		t.Fatal("outcome from another deployment feed was attached to this prefix")
	}
}

func TestRunForkDeploymentNeedsNoHandlerPlanProof(t *testing.T) {
	const bundle = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	ref := durabledata.DeclarationRef{FlowPath: ".", EventName: "items.ready"}
	version := durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("c", 64))
	source := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion, Declaration: ref, VersionID: version}
	at := time.Now().UTC()
	plan := runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{{
		Intent: fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{
			Key:        fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()},
			Deployment: &fanoutobligation.DeploymentOrigin{BundleHash: bundle, Declaration: ref, VersionID: version, SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("b", 64))},
			Source:     source,
		}, Source: source, Status: fanoutobligation.StatusClosed, NextChunkSize: fanoutobligation.InitialChunkSize, CreatedAt: at, UpdatedAt: at},
	}}}
	refs, err := resolveRunForkFanOutPlanRefs(plan, bundle, semanticview.FanOutPlanCarriage{})
	if err != nil || len(refs) != 0 {
		t.Fatalf("deployment borrowed a handler plan: refs=%v err=%v", refs, err)
	}
	plan.FanOutObligations[0].Intent.Request.Deployment.SchemaDigest = ""
	if _, err := resolveRunForkFanOutPlanRefs(plan, bundle, semanticview.FanOutPlanCarriage{}); err == nil {
		t.Fatal("plan-free deployment history bypassed its own source admission")
	}
}
