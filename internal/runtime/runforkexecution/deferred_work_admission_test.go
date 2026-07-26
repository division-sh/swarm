package runforkexecution

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
)

func TestSelectedContractDeferredWorkAdmissionCapabilityMatrix(t *testing.T) {
	basePlan := store.RunForkPlan{
		SourceRunID: uuid.NewString(),
		ForkPoint:   store.RunForkPoint{EventID: uuid.NewString()},
	}
	for _, tc := range []struct {
		name       string
		plan       store.RunForkPlan
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
			plan: store.RunForkPlan{
				SourceRunID: basePlan.SourceRunID,
				ForkPoint:   basePlan.ForkPoint,
				UnsupportedBlockers: []store.RunForkUnsupportedBlocker{{
					Code: store.RunForkBlockerTimerHistoryUnproven,
				}},
			},
			source:     selectedDeferredWorkTestSource(nil, nil),
			wantCode:   store.RunForkBlockerTimerHistoryUnproven,
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			admission, err := admitSelectedContractDeferredWork(tc.plan, tc.source)
			if tc.wantCode == "" {
				if err != nil {
					t.Fatalf("admitSelectedContractDeferredWork: %v", err)
				}
				if admission.owner != store.RunForkSelectedContractDeferredWorkAdmissionOwner {
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

func TestSelectedContractDeferredWorkAdmissionRejectsSourceDrift(t *testing.T) {
	plan := store.RunForkPlan{
		SourceRunID: uuid.NewString(),
		ForkPoint:   store.RunForkPoint{EventID: uuid.NewString()},
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
	if err := admission.validate(plan.SourceRunID, plan.ForkPoint.EventID, drifted); err == nil ||
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
		"admitSelectedContractDeferredWork":              2,
		"BuildSelectedContractExecutionAdmission":        2,
		"buildSelectedContractForkLocalRuntimeContainer": 2,
	} {
		if got := counts[function]; got != want {
			t.Fatalf("production %s call count = %d, want %d exact admitted entry points", function, got, want)
		}
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
