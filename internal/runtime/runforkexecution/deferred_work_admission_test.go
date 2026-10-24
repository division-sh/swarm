package runforkexecution

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestSelectedContractDeferredWorkAdmissionCapabilityMatrix(t *testing.T) {
	basePlan := runfork.RunForkPlan{
		SourceRunID: uuid.NewString(),
		ForkPoint:   runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: uuid.NewString(), Revision: 1},
	}
	for _, tc := range []struct {
		name         string
		plan         runfork.RunForkPlan
		source       semanticview.Source
		construction bool
	}{
		{
			name:   "clean source",
			plan:   basePlan,
			source: selectedDeferredWorkTestSource(nil, nil),
		},
		{
			name: "revision timer history retains native admission responsibility",
			plan: runfork.RunForkPlan{
				SourceRunID: basePlan.SourceRunID,
				ForkPoint:   basePlan.ForkPoint,
				UnsupportedBlockers: []runfork.RunForkUnsupportedBlocker{{
					Code: runfork.RunForkBlockerTimerHistoryUnproven,
				}},
			},
			source: selectedDeferredWorkTestSource(nil, nil),
		},
		{
			name: "workflow timer declaration",
			plan: basePlan,
			source: selectedDeferredWorkTestSource([]runtimecontracts.WorkflowTimerContract{{
				ID: "deadline",
			}}, nil),
		},
		{
			name: "workflow join deadline has retained execution ownership",
			plan: basePlan,
			source: selectedDeferredWorkTestSource(nil, []runtimecontracts.WorkflowJoinPlan{{
				Spec: runtimecontracts.JoinSpec{
					Deadline: &runtimecontracts.JoinDeadlineSpec{After: "1h", From: runtimecontracts.JoinDeadlineFromStageEntry},
				},
			}}),
		},
		{
			name:   "fan-out barrier declaration has retained execution ownership",
			plan:   basePlan,
			source: selectedDeferredWorkTestSource(nil, []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeFanOutDelivery}}),
		},
		{
			name:         "connected declarative creation requires retained construction",
			plan:         basePlan,
			source:       semanticview.Wrap(loadRunForkExecutionFixtureBundle(t, "examples/routing/template-create-minted-key")),
			construction: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			admission, err := admitSelectedContractDeferredWork(tc.plan, tc.source)
			if err != nil {
				t.Fatalf("admitSelectedContractDeferredWork: %v", err)
			}
			if admission.owner != runfork.RunForkSelectedContractDeferredWorkAdmissionOwner {
				t.Fatalf("admission = %#v", admission)
			}
			if admission.dynamicConstructionRequired != tc.construction {
				t.Fatalf("construction requirement=%v want=%v", admission.dynamicConstructionRequired, tc.construction)
			}
			if len(tc.plan.UnsupportedBlockers) != 0 && tc.plan.UnsupportedBlockers[0].Code != runfork.RunForkBlockerTimerHistoryUnproven {
				t.Fatal("owner availability rewrote the native timer evidence blocker")
			}
		})
	}
}

func TestSelectedContractDeferredWorkCannotDropConstructionRequirement(t *testing.T) {
	source := semanticview.Wrap(loadRunForkExecutionFixtureBundle(t, "examples/routing/template-create-minted-key"))
	plan := runfork.RunForkPlan{SourceRunID: uuid.NewString(), ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1}}
	admission, err := admitSelectedContractDeferredWork(plan, source)
	if err != nil || !admission.dynamicConstructionRequired {
		t.Fatalf("compiled creation lost construction requirement: %+v err=%v", admission, err)
	}
	if err := admission.validate(plan.SourceRunID, plan.ForkPoint, source); err != nil {
		t.Fatal(err)
	}
	admission.dynamicConstructionRequired = false
	if err := admission.validate(plan.SourceRunID, plan.ForkPoint, source); err == nil || !strings.Contains(err.Error(), "construction requirement") {
		t.Fatalf("construction requirement disappeared from retained admission: %v", err)
	}
}

func TestSelectedContractDeferredWorkAdmissionBindsDeploymentRevisionWithoutEvent(t *testing.T) {
	plan := runfork.RunForkPlan{
		SourceRunID: uuid.NewString(),
		ForkPoint:   runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7},
	}
	source := selectedDeferredWorkTestSource(nil, nil)
	admission, err := admitSelectedContractDeferredWork(plan, source)
	if err != nil {
		t.Fatalf("admit eventless deployment revision: %v", err)
	}
	if err := admission.validate(plan.SourceRunID, plan.ForkPoint, source); err != nil {
		t.Fatalf("validate exact deployment revision: %v", err)
	}
	for _, point := range []runfork.RunForkPoint{
		{Kind: runfork.RunForkPointDeploymentRevision, Revision: 8},
		{Kind: runfork.RunForkPointEvent, Revision: 7, EventID: uuid.NewString()},
		{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7, EventID: uuid.NewString()},
	} {
		if err := admission.validate(plan.SourceRunID, point, source); err == nil {
			t.Fatalf("contradictory point %+v was admitted", point)
		}
	}
}

func TestSelectedContractFanOutAdmissionRequiresExactElementAndSemanticDigest(t *testing.T) {
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("internal", "runtime", "testdata", "generic-swarm-bundle"))
	source := semanticview.Wrap(bundle)
	compiled, err := selectedContractCompiledFanOutPlans(source)
	if err != nil {
		t.Fatalf("selectedContractCompiledFanOutPlans: %v", err)
	}
	if len(compiled) == 0 {
		t.Fatal("generic fixture has no compiled fan-out plans")
	}
	var selected runtimecontracts.FanOutPlanRef
	for _, ref := range compiled {
		selected = ref
		break
	}
	sourceRef := selected
	sourceRef.BundleHash = "bundle-v2:sha256:" + strings.Repeat("0", 64)
	selectedPlan, ok := source.FanOutPlanForElement(selected.ElementRef)
	if !ok {
		t.Fatal("selected fan-out declaration has no compiled plan")
	}
	frozenSource := selectedPlan.SemanticEvidence()
	frozenSource.Emit.Event = "previous.output"
	sourceRef.SemanticDigest, err = canonicaljson.Hash(frozenSource)
	if err != nil {
		t.Fatal(err)
	}
	fieldSource := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, Field: strings.TrimPrefix(frozenSource.ItemsFrom, "payload.")}
	if strings.HasPrefix(frozenSource.ItemsFrom, "state.") {
		fieldSource.Kind, fieldSource.Field = fanoutobligation.SourceEntityField, strings.TrimPrefix(frozenSource.ItemsFrom, "state.")
	}
	plan := runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{
		selectedFanOutHistoryFixture(t, selectedPlan, sourceRef, fieldSource, frozenSource),
	}}
	refs, err := admitSelectedContractFanOutPlans(plan, source)
	if err != nil {
		t.Fatalf("admit selected fan-out with unchanged semantics: %v", err)
	}
	if len(refs) != 1 || refs[0] != selected {
		t.Fatalf("selected fan-out proof = %#v, want %#v", refs, selected)
	}

	changed := plan
	changed.FanOutObligations = append([]runfork.RunForkFanOutObligation(nil), plan.FanOutObligations...)
	changed.FanOutObligations[0].Intent.Request.Capsule.SourcePlanRef.SemanticDigest = "sha256:" + strings.Repeat("f", 64)
	if _, err := admitSelectedContractFanOutPlans(changed, source); err == nil || !strings.Contains(err.Error(), "contradict the same artifact") {
		t.Fatalf("changed semantic digest error = %v", err)
	}

	missing := plan
	missing.FanOutObligations = append([]runfork.RunForkFanOutObligation(nil), plan.FanOutObligations...)
	missing.FanOutObligations[0].Intent.Request.PlanRef.ElementRef.SemanticPath = "missing." + uuid.NewString()
	missingRequest := &missing.FanOutObligations[0].Intent.Request
	missingRequest.Key.ElementRef = missingRequest.PlanRef.ElementRef
	missingRequest.Capsule.SourceProjection = frozenSource.Clone()
	missingRequest.Capsule.SourceProjection.ElementRef = missingRequest.PlanRef.ElementRef
	missingRequest.PlanRef.SemanticDigest, err = canonicaljson.Hash(missingRequest.Capsule.SourceProjection)
	if err != nil {
		t.Fatal(err)
	}
	missingRequest.Capsule.SourcePlanRef = missingRequest.PlanRef
	if _, err := admitSelectedContractFanOutPlans(missing, source); err == nil || !strings.Contains(err.Error(), "missing pending fan_out declaration") {
		t.Fatalf("missing element error = %v", err)
	}
}

func TestSelectedContractDeferredWorkAdmitsDeploymentHistoryWithoutHandlerPlan(t *testing.T) {
	declaration := durabledata.DeclarationRef{FlowPath: "portfolio", EventName: "portfolio/account.registered"}
	version := durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("a", 64))
	request := fanoutobligation.IntentRequest{
		Key: fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()},
		Deployment: &fanoutobligation.DeploymentOrigin{
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("b", 64), Declaration: declaration,
			VersionID: version, SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("c", 64)),
		},
		Source:      fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion, Declaration: declaration, VersionID: version},
		Cardinality: 0,
	}
	now := time.Now().UTC()
	intent := fanoutobligation.Intent{Request: request, Source: request.Source, Status: fanoutobligation.StatusClosed,
		NextChunkSize: fanoutobligation.InitialChunkSize, CreatedAt: now, UpdatedAt: now}
	plan := runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{{Intent: intent}}}
	source := selectedDeferredWorkTestSource(nil, nil)
	refs, err := admitSelectedContractFanOutPlans(plan, source)
	if err != nil || len(refs) != 0 {
		t.Fatalf("deployment history borrowed handler plan: refs=%v err=%v", refs, err)
	}
	plan.FanOutObligations[0].Intent.Request.Deployment.SchemaDigest = "wrong"
	if _, err := admitSelectedContractFanOutPlans(plan, source); err == nil {
		t.Fatal("malformed deployment history was admitted")
	}
}

func TestSelectedContractFlowInputResolutionDynamicFlowOwnerMatrix(t *testing.T) {
	for _, tc := range []struct {
		mode runtimecontracts.FlowInputResolutionMode
		want bool
	}{
		{mode: runtimecontracts.FlowInputResolutionModeCreate, want: true},
		{mode: runtimecontracts.FlowInputResolutionModeSelect, want: false},
		{mode: runtimecontracts.FlowInputResolutionModeSelectOrCreate, want: true},
		{mode: runtimecontracts.FlowInputResolutionModeReply, want: false},
		{mode: runtimecontracts.FlowInputResolutionModeNone, want: false},
		{mode: runtimecontracts.FlowInputResolutionMode(255), want: true},
	} {
		t.Run(strings.ReplaceAll(runtimecontracts.FlowInputResolutionModeCode(tc.mode), "-", "_"), func(t *testing.T) {
			if got := selectedContractFlowInputResolutionRequiresDynamicFlowOwner(tc.mode); got != tc.want {
				t.Fatalf("selectedContractFlowInputResolutionRequiresDynamicFlowOwner(%q) = %t, want %t", tc.mode, got, tc.want)
			}
		})
	}
}

func TestSelectedContractDeferredWorkAdmissionRejectsSourceDrift(t *testing.T) {
	plan := runfork.RunForkPlan{
		SourceRunID: uuid.NewString(),
		ForkPoint:   runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: uuid.NewString(), Revision: 1},
	}
	admission, err := admitSelectedContractDeferredWork(plan, selectedDeferredWorkTestSource(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	drifted := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "selected-workflow", Version: "v2"},
	})
	if err := admission.validate(plan.SourceRunID, plan.ForkPoint, drifted); err == nil ||
		!strings.Contains(err.Error(), "workflow identity does not match") {
		t.Fatalf("source drift error = %v, want exact workflow identity rejection", err)
	}
}

func selectedDeferredWorkTestSource(timers []runtimecontracts.WorkflowTimerContract, joins []runtimecontracts.WorkflowJoinPlan) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "selected-workflow",
			Version: "v1",
			Timers:  timers,
			Joins:   joins,
		},
	})
}
