package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Excluded from the collector hash to avoid fingerprint self-reference.
const debtDeliveryStatusCollectorTo = "e6e1e0b19a6a9f9710193d07ed508d3c6a69959bbcf73b8636ec602506e41a44"

func TestPersistenceAuthorityDebtDeliveryStatusCollectorPreservesEveryOccurrenceAndDirection(t *testing.T) {
	site := debtControlSite("call:QueryRow", 3)
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtRunScalarCollectorTo, Sites: debtControlSet(site)}
	for _, mutation := range []string{"forward", "deletion", "addition", "multiplicity", "identity", "bootstrap", "foreign-base", "foreign-destination", "reverse"} {
		t.Run(mutation, func(t *testing.T) {
			trusted := trusted
			head := trusted
			head.Collector, head.Sites = debtDeliveryStatusCollectorTo, maps.Clone(trusted.Sites)
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
			if got := debtDeliveryStatusCollectorTransition(base, current, trusted, head); got != (mutation == "forward" || mutation == "deletion") {
				t.Fatal("non-finite delivery status collector transition accepted/refused:", mutation, got)
			}
		})
	}
}

func TestPersistenceAuthorityDebtDeliveryStatusSourcePinsAndCompositionFailClosed(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if err := debtDeliveryStatusSourcePins(root); err != nil {
		t.Fatal(err)
	}
	if collector, err := debtCollectorDigest(root); err != nil || collector != debtActiveCollectorTo {
		t.Fatalf("delivery status collector=%s expected=%s error=%v", collector, debtActiveCollectorTo, err)
	}
	bodies, err := debtDeliveryStatusBodies(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range bodies {
		if debtDeliveryStatusSuccessorCandidate(body.After) {
			before, err := debtDeliveryStatusSourcePrevious(root, body.File, body.Function, body.After)
			if err != nil || before != body.Before {
				t.Fatal("exact delivery status source inverse failed:", body.Function, err)
			}
			for _, cut := range []string{"reader", "eventID", "runID", "t.Fatal", " != ", "25 * time.Millisecond"} {
				if !strings.Contains(body.After, cut) {
					continue
				}
				mutant := strings.Replace(body.After, cut, "foreignStatusCut", 1)
				before, err := debtDeliveryStatusSourcePrevious(root, body.File, body.Function, mutant)
				if err != nil || before != mutant {
					t.Fatal("changed delivery status source received normalization credit:", cut, err)
				}
			}
		}
	}
	copy := t.TempDir()
	debtSourceForkCopySourceEvidence(t, root, copy)
	for _, path := range []string{"internal/serveapp/main_runtime_test.go", "tools/fixture-codemod/pipeline-observations/family145-delivery-status-preimages.json", "tools/fixture-codemod/pipeline-observations/family145-event-name-preimages.json"} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	debtServedObservationCopySourceEvidence(t, root, copy)
	if err := debtDeliveryStatusSourcePins(copy); err != nil {
		t.Fatal(err)
	}
	path := "internal/serveapp/main_runtime_test.go"
	raw, err := os.ReadFile(filepath.Join(copy, path))
	if err != nil {
		t.Fatal(err)
	}
	var counter string
	for _, body := range bodies {
		if body.Function == "servedEventPublishDeliveryStatusCount" {
			counter = body.After
		}
	}
	if counter == "" || strings.Count(string(raw), counter) != 1 {
		t.Fatal("missing exact canonical status counter for hostile mutation")
	}
	mutant := strings.Replace(counter, "storetest.ReadServedDeliveryStatusCount", "foreignStatusQuery", 1)
	debtWriteModuleSource(t, copy, path, strings.Replace(string(raw), counter, mutant, 1))
	if err := debtDeliveryStatusSourcePins(copy); err == nil {
		t.Fatal("changed canonical status query admitted")
	}
}
