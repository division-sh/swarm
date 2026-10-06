package runstart

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func finiteTestSource(t *testing.T, files map[string]string) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	for name, body := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func TestFiniteStartIncludesEagerKeylessChildren(t *testing.T) {
	for _, childFinal := range []bool{false, true} {
		child := "stages: {waiting: {}}\n"
		if childFinal {
			child = "stages: {waiting: {}, done: {final: true}}\n"
		}
		source := finiteTestSource(t, map[string]string{
			"schema.yaml":          "stages: {waiting: {}, done: {final: true}}\n",
			"unrouted/schema.yaml": child,
		})
		err := ValidateFinite(source)
		if childFinal {
			if err != nil {
				t.Fatalf("finite keyless tree rejected: %v", err)
			}
		} else {
			var refusal *FiniteStartError
			if !errors.As(err, &refusal) || refusal.FlowID != "unrouted" || !strings.Contains(refusal.Error(), "use `serve`") {
				t.Fatalf("unrouted eager service was exempted: %v", err)
			}
		}
	}
}

func TestFiniteStartStatelessRootIsNotAnExemptContainer(t *testing.T) {
	source := finiteTestSource(t, map[string]string{
		"schema.yaml":      "name: container\n",
		"leaf/schema.yaml": "stages: {done: {final: true}}\n",
	})
	var refusal *FiniteStartError
	if err := ValidateFinite(source); !errors.As(err, &refusal) || refusal.FlowID != "." {
		t.Fatalf("constructed stateless root was exempted: %v", err)
	}
}

func TestFiniteStartIgnoresUnrelatedDormantTemplate(t *testing.T) {
	source := finiteTestSource(t, map[string]string{
		"schema.yaml":           "stages: {done: {final: true}}\n",
		"dormant/schema.yaml":   "instance: item_id\nstages: {waiting: {}}\n",
		"dormant/entities.yaml": "Item:\n  item_id: {type: text, _unused_reason: dormant constructor}\n",
	})
	if err := ValidateFinite(source); err != nil {
		t.Fatalf("filesystem-only dormant template entered finite closure: %v", err)
	}
}

func TestFiniteStartUnknownCatalogRefuses(t *testing.T) {
	for _, source := range []semanticview.Source{nil, semanticview.Wrap(&contracts.WorkflowContractBundle{})} {
		if err := ValidateFinite(source); err == nil {
			t.Fatal("missing catalog accepted as absence of a service")
		}
	}
}

func TestFiniteStartIncludesConnectedTemplatesAndTheirEagerChildren(t *testing.T) {
	for _, offender := range []string{"worker", "worker/support", "none"} {
		t.Run(offender, func(t *testing.T) {
			workerStages, childStages := "stages: {active: {}, done: {final: true}}\n", "stages: {done: {final: true}}\n"
			if offender == "worker" {
				workerStages = "stages: {active: {}}\n"
			}
			if offender == "worker/support" {
				childStages = "stages: {active: {}}\n"
			}
			source := finiteTestSource(t, map[string]string{
				"schema.yaml":                "stages: {active: {}, done: {final: true}}\npins:\n  inputs: [work.requested]\n  outputs: [work.requested]\nconnect:\n  - {event: work.requested, from: ., to: worker, resolution: create}\n",
				"events.yaml":                "work.requested:\n  worker_id: text\n",
				"worker/schema.yaml":         "instance: worker_id\n" + workerStages + "pins:\n  inputs: [work.requested]\n",
				"worker/entities.yaml":       "Worker:\n  worker_id: {type: text, _unused_reason: constructor identity}\n",
				"worker/nodes.yaml":          "worker:\n  execution_type: system_node\n  event_handlers:\n    work.requested: {}\n",
				"worker/support/schema.yaml": childStages,
			})
			err := ValidateFinite(source)
			if offender == "none" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var refusal *FiniteStartError
			if !errors.As(err, &refusal) || refusal.FlowID != offender {
				t.Fatalf("connected constructor closure missed %s: %v", offender, err)
			}
		})
	}
}

func TestFiniteStartRejectsContradictoryConstructorEvidence(t *testing.T) {
	source := finiteTestSource(t, map[string]string{
		"schema.yaml":       "stages: {done: {final: true}}\n",
		"child/schema.yaml": "stages: {done: {final: true}}\n",
	})
	bundle, _ := semanticview.Bundle(source)
	bundle.FlowTree.Root.Children[0].Parent = nil
	if err := ValidateFinite(source); err == nil || !strings.Contains(err.Error(), "contradicts its admitted constructor") {
		t.Fatalf("corrupt eager construction evidence waived finite admission: %v", err)
	}
}
