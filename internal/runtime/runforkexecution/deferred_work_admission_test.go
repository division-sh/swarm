package runforkexecution

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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
		name       string
		plan       runfork.RunForkPlan
		source     semanticview.Source
		wantCode   string
		capability string
	}{
		{
			name:   "clean source",
			plan:   basePlan,
			source: selectedDeferredWorkTestSource(nil, nil),
		},
		{
			name: "revision timer history",
			plan: runfork.RunForkPlan{
				SourceRunID: basePlan.SourceRunID,
				ForkPoint:   basePlan.ForkPoint,
				UnsupportedBlockers: []runfork.RunForkUnsupportedBlocker{{
					Code: runfork.RunForkBlockerTimerHistoryUnproven,
				}},
			},
			source:     selectedDeferredWorkTestSource(nil, nil),
			wantCode:   runfork.RunForkBlockerTimerHistoryUnproven,
			capability: selectedContractDeferredWorkRevisionTimerHistory,
		},
		{
			name: "workflow timer declaration",
			plan: basePlan,
			source: selectedDeferredWorkTestSource([]runtimecontracts.WorkflowTimerContract{{
				ID: "deadline",
			}}, nil),
			wantCode:   selectedContractDeferredWorkOwnerUnavailable,
			capability: selectedContractDeferredWorkWorkflowTimer,
		},
		{
			name: "workflow join timeout",
			plan: basePlan,
			source: selectedDeferredWorkTestSource(nil, []runtimecontracts.WorkflowJoinPlan{{
				Spec: runtimecontracts.JoinSpec{
					TimeoutFound: true,
					Timeout:      runtimecontracts.JoinTimeoutSpec{After: "1h"},
				},
			}}),
			wantCode:   selectedContractDeferredWorkOwnerUnavailable,
			capability: selectedContractDeferredWorkWorkflowJoinTimeout,
		},
		{
			name:       "fan-out barrier declaration requires deferred execution owner",
			plan:       basePlan,
			source:     selectedDeferredWorkTestSource(nil, []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeFanOutDelivery}}),
			wantCode:   selectedContractDeferredWorkOwnerUnavailable,
			capability: selectedContractDeferredWorkFanOutBarrier,
		},
		{
			name:       "connected declarative dynamic flow creation",
			plan:       basePlan,
			source:     semanticview.Wrap(loadRunForkExecutionFixtureBundle(t, "examples/routing/template-create-minted-key")),
			wantCode:   selectedContractDeferredWorkOwnerUnavailable,
			capability: selectedContractDeferredWorkDynamicFlowCreation,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			admission, err := admitSelectedContractDeferredWork(tc.plan, tc.source)
			if tc.wantCode == "" {
				if err != nil {
					t.Fatalf("admitSelectedContractDeferredWork: %v", err)
				}
				if admission.owner != runfork.RunForkSelectedContractDeferredWorkAdmissionOwner {
					t.Fatalf("admission = %#v", admission)
				}
				return
			}
			failure, ok := runtimefailures.EnvelopeFromError(err)
			if err == nil || !ok || failure.Class != runtimefailures.ClassDependencyUnavailable || failure.Detail.Code != tc.wantCode {
				t.Fatalf("error = %v, failure=%#v, want %s", err, failure, tc.wantCode)
			}
			capabilities, ok := failure.Detail.Attributes["capabilities"].([]string)
			if !ok || !containsSelectedDeferredWorkCapability(capabilities, tc.capability) {
				t.Fatalf("capabilities = %#v, want %q", failure.Detail.Attributes["capabilities"], tc.capability)
			}
		})
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
	plan := runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{{
		Intent: fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{PlanRef: sourceRef}},
	}}}
	refs, err := admitSelectedContractFanOutPlans(plan, source)
	if err != nil {
		t.Fatalf("admit selected fan-out with unchanged semantics: %v", err)
	}
	if len(refs) != 1 || refs[0] != selected {
		t.Fatalf("selected fan-out proof = %#v, want %#v", refs, selected)
	}

	changed := plan
	changed.FanOutObligations = append([]runfork.RunForkFanOutObligation(nil), plan.FanOutObligations...)
	changed.FanOutObligations[0].Intent.Request.PlanRef.SemanticDigest = "sha256:" + strings.Repeat("f", 64)
	if _, err := admitSelectedContractFanOutPlans(changed, source); err == nil || !strings.Contains(err.Error(), "changed semantic digest") {
		t.Fatalf("changed semantic digest error = %v", err)
	}

	missing := plan
	missing.FanOutObligations = append([]runfork.RunForkFanOutObligation(nil), plan.FanOutObligations...)
	missing.FanOutObligations[0].Intent.Request.PlanRef.ElementRef.SemanticPath = "missing." + uuid.NewString()
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
		{mode: runtimecontracts.FlowInputResolutionModeFanIn, want: false},
		{mode: runtimecontracts.FlowInputResolutionModeFanOut, want: false},
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
	drifted := selectedDeferredWorkTestSource(nil, []runtimecontracts.WorkflowJoinPlan{{
		Spec: runtimecontracts.JoinSpec{
			TimeoutFound: true,
			Timeout:      runtimecontracts.JoinTimeoutSpec{After: "1h"},
		},
	}})
	if err := admission.validate(plan.SourceRunID, plan.ForkPoint, drifted); err == nil ||
		!strings.Contains(err.Error(), selectedContractDeferredWorkWorkflowJoinTimeout) {
		t.Fatalf("source drift error = %v, want workflow join capability rejection", err)
	}
}

func TestSelectedContractDeferredWorkAdmissionProductionConsumersStatic(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	activationCalls := map[string]int{}
	imperativeCalls := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && (selector.Sel.Name == "PrepareFlowInstanceActivation" || selector.Sel.Name == "FinalizeCommittedFlowInstanceActivation") {
				activationCalls[name]++
			}
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "ActivateFlowInstance" {
				imperativeCalls++
			}
			ident, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			switch ident.Name {
			case "admitSelectedContractDeferredWork":
				counts[ident.Name]++
			case "BuildSelectedContractExecutionAdmission":
				counts[ident.Name]++
				requireSelectedDeferredWorkCompositeField(t, name, call, "DeferredWorkAdmission")
			case "buildSelectedContractForkLocalRuntimeContainer":
				counts[ident.Name]++
				requireSelectedDeferredWorkCompositeField(t, name, call, "DeferredWorkAdmission")
			}
			return true
		})
	}
	for function, want := range map[string]int{
		"admitSelectedContractDeferredWork":              3, // initial execution, activation gate, recovered finite feed
		"BuildSelectedContractExecutionAdmission":        3, // initial execution, activation gate, recovered finite feed
		"buildSelectedContractForkLocalRuntimeContainer": 2,
	} {
		if got := counts[function]; got != want {
			t.Fatalf("production %s call count = %d, want %d exact admitted entry points", function, got, want)
		}
	}
	if imperativeCalls != 0 {
		t.Fatalf("imperative activation consumers = %d, want none", imperativeCalls)
	}
	if len(activationCalls) != 1 || activationCalls["runtime_container.go"] != 2 {
		t.Fatalf("selected dynamic activation consumers = %#v, want fork-local planner and committed finalizer", activationCalls)
	}
	containerSource, err := os.ReadFile("runtime_container.go")
	if err != nil {
		t.Fatal(err)
	}
	admissionIndex := strings.Index(string(containerSource), "req.DeferredWorkAdmission.validate")
	activationIndex := strings.Index(string(containerSource), "TemplateInstancePlanner:")
	if admissionIndex < 0 || activationIndex < 0 || admissionIndex >= activationIndex {
		t.Fatal("fork-local dynamic activation is not structurally downstream of deferred-work admission")
	}
}

func requireSelectedDeferredWorkCompositeField(t *testing.T, file string, call *ast.CallExpr, field string) {
	t.Helper()
	if len(call.Args) == 0 {
		t.Fatalf("%s call in %s has no request", functionName(call.Fun), file)
	}
	literal, ok := call.Args[len(call.Args)-1].(*ast.CompositeLit)
	if !ok {
		t.Fatalf("%s call in %s does not use an auditable request literal", functionName(call.Fun), file)
	}
	for _, element := range literal.Elts {
		keyValue, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := keyValue.Key.(*ast.Ident)
		if ok && key.Name == field {
			return
		}
	}
	t.Fatalf("%s call in %s does not consume %s", functionName(call.Fun), file, field)
}

func functionName(expr ast.Expr) string {
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return "call"
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

func containsSelectedDeferredWorkCapability(capabilities []string, want string) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}
