package main

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestPlanCICoreWithChannelPreservesCanonicalObligations(t *testing.T) {
	dir := t.TempDir()
	policy, weights, packages := productionPlannerInputs(t, dir)
	plans := map[string]testplanning.RunPlan{}
	for _, selection := range []struct{ id, body string }{
		{"core", "CI-Tier: core"},
		{"full", "CI-Tier: full"},
		{"core-channel", "CI-Tier: core\nCI-Units: serveapp-channel"},
	} {
		event := filepath.Join(dir, selection.id+"-event.json")
		body := strings.ReplaceAll(selection.body, "\n", `\n`)
		if err := os.WriteFile(event, []byte(`{"pull_request":{"body":"`+body+`"}}`), 0600); err != nil {
			t.Fatal(err)
		}
		planPath := filepath.Join(dir, selection.id+"-plan.json")
		if err := run(config{
			planCI: true, proofPolicyPath: policy, weightModelPath: weights, packagesPath: packages,
			eventPath: event, event: "pull_request", headSHA: "same-source",
			planPath: planPath, matrixPath: filepath.Join(dir, selection.id+"-matrix.json"), markdownPath: filepath.Join(dir, selection.id+".md"),
		}); err != nil {
			t.Fatalf("%s actual PR-body plan emission: %v", selection.id, err)
		}
		plan, err := readPlan(planPath)
		if err != nil {
			t.Fatal(err)
		}
		plans[selection.id] = plan
	}
	fullChannel, err := plans["full"].Unit("serveapp-channel")
	if err != nil {
		t.Fatal(err)
	}
	actual, err := plans["core-channel"].Unit("serveapp-channel")
	if err != nil {
		t.Fatal(err)
	}
	actual.WeightSeconds = fullChannel.WeightSeconds
	if !reflect.DeepEqual(actual, fullChannel) {
		t.Fatal("PR-body supplement differs from the canonical full channel recipe/obligations")
	}
	union := func(plan testplanning.RunPlan, extra ...testplanning.ProofUnit) map[testplanning.TestRoot][]string {
		roots := map[testplanning.TestRoot][]string{}
		for _, unit := range append(slices.Clone(plan.Units), extra...) {
			for _, required := range unit.RequiredTests {
				children := append(slices.Clone(roots[required.TestRoot]), required.Children...)
				slices.Sort(children)
				roots[required.TestRoot] = slices.Compact(children)
			}
		}
		return roots
	}
	if !reflect.DeepEqual(union(plans["core"], fullChannel), union(plans["core-channel"])) {
		t.Fatal("actual emitted core/channel root and child obligations do not conserve their union")
	}
	if err := testplanning.CheckCurrentCISelection(plans["core-channel"], "CI-Tier: core"); err == nil {
		t.Fatal("removing the supplement accepted its old bound receipt")
	}
	t.Logf("actual PR-body core + channel plan bound: %s", plans["core-channel"].Digest)
}
