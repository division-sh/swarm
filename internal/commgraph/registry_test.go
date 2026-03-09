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

func TestRuntimeAndHumanEventClassifications(t *testing.T) {
	runtimeSet := map[string]struct{}{}
	for _, evt := range RuntimeEvents() {
		runtimeSet[evt] = struct{}{}
	}
	humanSet := map[string]struct{}{}
	for _, evt := range HumanEvents() {
		humanSet[evt] = struct{}{}
	}

	for _, evt := range []string{
		"brand.requested",
		"cto.spec_review_requested",
		"founder_input.response",
		"human_task.requested",
		"scan.completed",
		"spec.revision_requested",
		"user_onboarded",
		"vertical.discovered",
		"vertical.killed",
		"vertical.shortlisted",
		"devops.health_check_failed",
	} {
		if _, ok := runtimeSet[evt]; !ok {
			t.Fatalf("expected runtime event %q to be classified as runtime-emitted", evt)
		}
	}

	for _, evt := range []string{
		"system.directive",
		"board.directive",
		"board.chat",
		"template.publish_requested",
		"template.migration_approved",
		"spend.approved",
		"spend.rejected",
		"vertical.approved",
		"vertical.needs_more_data",
		"human_task.completed",
		"opco.teardown_requested",
	} {
		if _, ok := humanSet[evt]; !ok {
			t.Fatalf("expected human event %q to be classified as human-emitted", evt)
		}
	}

	for _, evt := range []string{"vertical.resumed", "opco.routing_updated", "inbound.whatsapp_message", "inbound.email", "human_task.assigned"} {
		if _, ok := runtimeSet[evt]; ok {
			t.Fatalf("did not expect %q in runtime-emitted list", evt)
		}
	}
	for _, evt := range []string{"vertical.killed", "founder_input.response", "opco.escalation_response"} {
		if _, ok := humanSet[evt]; ok {
			t.Fatalf("did not expect %q in human-emitted list", evt)
		}
	}
}

func TestProducerEventsForRoleIncludesActorGatewayAndCoordinatorResume(t *testing.T) {
	assertHas := func(role, eventType string) {
		events := ProducerEventsForRole(role)
		for _, evt := range events {
			if evt == eventType {
				return
			}
		}
		t.Fatalf("expected %q to produce %q, got %v", role, eventType, events)
	}
	assertHas("empire-coordinator", "vertical.resumed")
	assertHas("inbound-gateway", "inbound.whatsapp_message")
	assertHas("inbound-gateway", "inbound.email")
	assertHas("dashboard", "human_task.assigned")
	assertHas("actor-agent", "opco.routing_updated")
}

func TestMessageAuthorities_DeriveTemplateHierarchy(t *testing.T) {
	assertRecipient := func(sender, recipient string) {
		t.Helper()
		sender = CanonicalRole(sender)
		recipient = CanonicalRole(recipient)
		for _, rule := range MessageAuthorities() {
			if CanonicalRole(rule.SenderRole) != sender {
				continue
			}
			for _, candidate := range rule.RecipientRoles {
				if CanonicalRole(candidate) == recipient {
					return
				}
			}
		}
		t.Fatalf("expected %q to be allowed to message %q", sender, recipient)
	}

	assertRecipient("opco-ceo", "vp-product")
	assertRecipient("vp-product", "opco-ceo")
	assertRecipient("cto-agent", "backend-agent")
	assertRecipient("backend-agent", "cto-agent")
	assertRecipient("opco-ceo", "backend-agent")
	assertRecipient("backend-agent", "opco-ceo")
}

func TestMessageAuthorities_KeepManualPeerExceptions(t *testing.T) {
	for _, rule := range MessageAuthorities() {
		if CanonicalRole(rule.SenderRole) != "chief-of-staff" {
			continue
		}
		recipients := map[string]struct{}{}
		for _, recipient := range rule.RecipientRoles {
			recipients[CanonicalRole(recipient)] = struct{}{}
		}
		for _, expected := range []string{"vp-product", "vp-growth", "opco-ceo"} {
			if _, ok := recipients[expected]; !ok {
				t.Fatalf("expected chief-of-staff to keep recipient %q; got %v", expected, rule.RecipientRoles)
			}
		}
		return
	}
	t.Fatal("expected chief-of-staff message authority rule")
}
