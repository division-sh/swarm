package commgraph

import "testing"

func TestProducerEventsForRole_AliasesAndSort(t *testing.T) {
	events := ProducerEventsForRole("Head_of_Product")
	if len(events) == 0 {
		t.Fatal("expected events for head_of_product alias")
	}
	found := false
	for _, evt := range events {
		if evt == "product_report" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected product_report in %v", events)
	}
}

func TestCanonicalRole_Aliases(t *testing.T) {
	if got := CanonicalRole("Head_of_Growth"); got != "vp-growth" {
		t.Fatalf("expected vp-growth, got %q", got)
	}
	if got := CanonicalRole(" CTO "); got != "cto-agent" {
		t.Fatalf("expected cto-agent, got %q", got)
	}
}

func TestHasProducerForPattern(t *testing.T) {
	if !HasProducerForPattern("opco.*") {
		t.Fatal("expected wildcard opco.* to match known producers")
	}
	if !HasProducerForPattern("spec.validation_requested") {
		t.Fatal("expected exact producer match")
	}
	if HasProducerForPattern("unknown.event.never") {
		t.Fatal("did not expect unknown producer pattern to pass")
	}
}

func TestProducerRoles_SortedAndNonEmpty(t *testing.T) {
	roles := ProducerRoles()
	if len(roles) == 0 {
		t.Fatal("expected producer roles")
	}
	for i := 1; i < len(roles); i++ {
		if roles[i-1] > roles[i] {
			t.Fatalf("expected sorted roles, got %q before %q", roles[i-1], roles[i])
		}
	}
	found := false
	for _, role := range roles {
		if role == "empire-coordinator" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected empire-coordinator in producer roles: %v", roles)
	}
}
