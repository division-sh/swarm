package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Excluded from the collector hash to avoid fingerprint self-reference.
const debtRunScalarCollectorTo = "3db22e07dfc4097879c83e6f0645d8ce8d0e4349e0305c186d5be86f6fce44fc"

func TestPersistenceAuthorityDebtRunScalarCollectorPreservesEveryOccurrenceAndDirection(t *testing.T) {
	site := debtControlSite("call:QueryRow", 3)
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtRequestKeyCollectorTo, Sites: debtControlSet(site)}
	for _, mutation := range []string{"forward", "deletion", "addition", "multiplicity", "identity", "bootstrap", "foreign-base", "foreign-destination", "reverse"} {
		t.Run(mutation, func(t *testing.T) {
			trusted := trusted
			head := trusted
			head.Collector, head.Sites = debtRunScalarCollectorTo, maps.Clone(trusted.Sites)
			base, current := trusted.Collector, head.Collector
			switch mutation {
			case "deletion":
				delete(head.Sites, site.identity())
			case "addition":
				added := debtControlSite("call:Exec", 1)
				head.Sites[added.identity()] = added
			case "multiplicity":
				site := site
				site.Multiplicity++
				head.Sites[site.identity()] = site
			case "identity":
				delete(head.Sites, site.identity())
				site := site
				site.Operation += "foreign"
				head.Sites[site.identity()] = site
			case "bootstrap":
				head.BootstrapSource = strings.Repeat("b", 40)
			case "foreign-base":
				base = strings.Repeat("b", 64)
			case "foreign-destination":
				current, head.Collector = strings.Repeat("c", 64), strings.Repeat("c", 64)
			case "reverse":
				base, current = current, base
				trusted.Collector, head.Collector = base, current
			}
			if got := debtRunScalarCollectorTransition(base, current, trusted, head); got != (mutation == "forward" || mutation == "deletion") {
				t.Fatal("non-finite scalar collector transition accepted/refused:", mutation, got)
			}
		})
	}
}

func TestPersistenceAuthorityDebtRunScalarSourcePinsAndCompositionFailClosed(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if err := debtRunScalarSourcePins(root); err != nil {
		t.Fatal(err)
	}
	if collector, err := debtCollectorDigest(root); err != nil || collector != debtActiveCollectorTo {
		t.Fatalf("run scalar collector=%s expected=%s error=%v", collector, debtActiveCollectorTo, err)
	}
	bodies, err := debtRunScalarBodies(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range bodies {
		if strings.Contains(body.After, "servedEventPublishScalarCount(t, reader,") {
			before, err := debtRunScalarSourcePrevious(root, body.File, body.Function, body.After)
			if err != nil || before != body.Before {
				t.Fatal("exact scalar source inverse failed:", body.Function, err)
			}
			for _, cut := range []string{"reader", "eventID", "runID", "t.Fatal", " != "} {
				if !strings.Contains(body.After, cut) {
					continue
				}
				mutant := strings.Replace(body.After, cut, "foreignScalarCut", 1)
				before, err := debtRunScalarSourcePrevious(root, body.File, body.Function, mutant)
				if err != nil || before != mutant {
					t.Fatal("changed scalar source received normalization credit:", cut, err)
				}
			}
		}
	}
	copy := t.TempDir()
	debtSourceForkCopySourceEvidence(t, root, copy)
	for _, path := range []string{"internal/serveapp/main_runtime_test.go", "internal/serveapp/postgres_three_contracts_test.go", "internal/serveapp/postgres_commit_loss_contract_test.go", "tools/fixture-codemod/pipeline-observations/family145-run-scalar-preimages.json", "tools/fixture-codemod/pipeline-observations/family145-delivery-status-preimages.json"} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	debtServedObservationCopySourceEvidence(t, root, copy)
	if err := debtRunScalarSourcePins(copy); err != nil {
		t.Fatal(err)
	}
	path := "internal/serveapp/main_runtime_test.go"
	raw, err := os.ReadFile(filepath.Join(copy, path))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, path, strings.Replace(string(raw), "storetest.CountPhysicalRunIdentity", "foreignScalarQuery", 1))
	if err := debtRunScalarSourcePins(copy); err == nil {
		t.Fatal("changed canonical run scalar predicate admitted")
	}
}
