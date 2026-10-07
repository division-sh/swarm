package runstart

import (
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func finiteTestSource(t *testing.T, root string) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func TestFiniteStartIncludesEagerKeylessChildren(t *testing.T) {
	for _, childFinal := range []bool{false, true} {
		source := finiteTestSource(t, canonicalrouting.CopyFiniteEagerClosure(t, childFinal))
		err := ValidateFinite(source, nil)
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
	source := finiteTestSource(t, canonicalrouting.CopyFiniteStatelessContainer(t))
	var refusal *FiniteStartError
	if err := ValidateFinite(source, nil); !errors.As(err, &refusal) || refusal.FlowID != "." {
		t.Fatalf("constructed stateless root was exempted: %v", err)
	}
}

func TestFiniteStartIgnoresUnrelatedDormantTemplate(t *testing.T) {
	source := finiteTestSource(t, canonicalrouting.CopyFiniteDormantTemplate(t))
	if err := ValidateFinite(source, nil); err != nil {
		t.Fatalf("filesystem-only dormant template entered finite closure: %v", err)
	}
}

func TestFiniteStartUnknownCatalogRefuses(t *testing.T) {
	for _, source := range []semanticview.Source{nil, semanticview.Wrap(&contracts.WorkflowContractBundle{})} {
		if err := ValidateFinite(source, nil); err == nil {
			t.Fatal("missing catalog accepted as absence of a service")
		}
	}
}

func TestFiniteStartIncludesConnectedTemplatesAndTheirEagerChildren(t *testing.T) {
	for _, tc := range []struct {
		offender string
		variant  canonicalrouting.FiniteClosureOffender
	}{
		{"worker", canonicalrouting.FiniteClosureWorkerService},
		{"worker/support", canonicalrouting.FiniteClosureChildService},
		{"none", canonicalrouting.FiniteClosureEnded},
	} {
		t.Run(tc.offender, func(t *testing.T) {
			source := finiteTestSource(t, canonicalrouting.CopyFiniteConnectedClosure(t, tc.variant))
			err := ValidateFinite(source, nil)
			if tc.offender == "none" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var refusal *FiniteStartError
			if !errors.As(err, &refusal) || refusal.FlowID != tc.offender {
				t.Fatalf("connected constructor closure missed %s: %v", tc.offender, err)
			}
		})
	}
}

func TestFiniteStartRejectsContradictoryConstructorEvidence(t *testing.T) {
	source := finiteTestSource(t, canonicalrouting.CopyFiniteConstructorControl(t))
	bundle, _ := semanticview.Bundle(source)
	bundle.FlowTree.Root.Children[0].Parent = nil
	if err := ValidateFinite(source, nil); err == nil || !strings.Contains(err.Error(), "contradicts its admitted constructor") {
		t.Fatalf("corrupt eager construction evidence waived finite admission: %v", err)
	}
}
