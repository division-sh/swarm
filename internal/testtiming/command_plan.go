package testtiming

import (
	"fmt"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
)

func commandPlanProblems(plan testplanning.RunPlan) []string {
	if err := plan.BuildContext.Validate(); err != nil {
		return []string{"command evidence requires a bound plan: " + err.Error()}
	}
	if err := plan.Validate(); err != nil {
		return []string{fmt.Sprintf("invalid executable command plan: %v", err)}
	}
	var problems []string
	seen := map[TimingCell]bool{}
	for _, unit := range plan.Units {
		if len(unit.SelectedRoots) == 0 || len(unit.RequiredTests) == 0 {
			problems = append(problems, fmt.Sprintf("executable unit %s requires selected and required test roots", unit.ID))
			continue
		}
		problems = append(problems, unitRootBindingProblems(unit, seen)...)
	}
	return problems
}

func unitRootBindingProblems(unit testplanning.ProofUnit, seen map[TimingCell]bool) []string {
	var problems []string
	backend, _ := testplanning.SoakBackend(unit.Run)
	for _, root := range unit.SelectedRoots {
		if !slices.Contains(unit.Packages, root.Package) || !slices.Contains(unit.TestBearingPackages, root.Package) || root.Name == "" || strings.TrimSpace(root.Name) != root.Name || strings.Contains(root.Name, "/") {
			problems = append(problems, fmt.Sprintf("unit %s has invalid selected root %s.%s", unit.ID, root.Package, root.Name))
		}
		cell := TimingCell{Package: root.Package, Root: root.Name, Backend: backend, Environment: unit.EnvironmentID, Count: unit.CountMode}
		if seen[cell] {
			problems = append(problems, fmt.Sprintf("duplicate executable root cell %s.%s/%s", root.Package, root.Name, backend))
		}
		seen[cell] = true
	}
	for _, pkg := range unit.TestBearingPackages {
		if !slices.Contains(unit.Packages, pkg) {
			problems = append(problems, fmt.Sprintf("unit %s has foreign test-bearing package %s", unit.ID, pkg))
		}
	}
	for _, required := range unit.RequiredTests {
		problems = append(problems, childBindingProblems(required.Name, required.Children)...)
	}
	for name, children := range unit.RequiredChildren {
		problems = append(problems, childBindingProblems(name, children)...)
		problems = append(problems, declaredChildBindingProblems(unit, name, children)...)
	}
	return problems
}

func childBindingProblems(name string, children []string) []string {
	var problems []string
	seen := map[string]bool{}
	for _, child := range children {
		if child == "" || strings.TrimSpace(child) != child || seen[child] {
			problems = append(problems, fmt.Sprintf("root %s has empty, noncanonical or duplicate child %q", name, child))
		}
		seen[child] = true
	}
	return problems
}

func declaredChildBindingProblems(unit testplanning.ProofUnit, name string, children []string) []string {
	var problems []string
	found := false
	for _, required := range unit.RequiredTests {
		if required.Name != name {
			continue
		}
		found = true
		for _, child := range children {
			if !slices.Contains(required.Children, child) {
				problems = append(problems, fmt.Sprintf("unit %s dropped declared child %s/%s", unit.ID, name, child))
			}
		}
	}
	if !found {
		problems = append(problems, fmt.Sprintf("unit %s declares children for unrequired root %s", unit.ID, name))
	}
	return problems
}

func reportSelectionProblems(unit testplanning.ProofUnit, report Report) []string {
	selected := map[testplanning.TestRoot]bool{}
	for _, root := range unit.SelectedRoots {
		selected[root] = true
	}
	var problems []string
	for _, row := range report.Tests {
		name, _, _ := strings.Cut(row.Test, "/")
		if !selected[testplanning.TestRoot{Package: row.Package, Name: name}] {
			problems = append(problems, fmt.Sprintf("unit %s reported root outside its selected manifest: %s.%s", unit.ID, row.Package, name))
		}
	}
	return problems
}
