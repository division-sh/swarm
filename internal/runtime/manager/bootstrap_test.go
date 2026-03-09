package manager

import (
	"strings"
	"testing"
)

func TestDefaultOpCoRoutesBootstrapSeededCounts(t *testing.T) {
	routes := DefaultOpCoRoutes("v1")
	if len(routes) != 20 {
		t.Fatalf("expected 20 default routes (all bootstrap), got %d", len(routes))
	}
	bootstrap := 0
	seeded := 0
	deployToCTO := false
	deployToDevOps := false
	for _, r := range routes {
		switch r.Source {
		case "bootstrap":
			bootstrap++
		case "seeded":
			seeded++
		}
		if r.EventPattern == "deploy_requested" && strings.HasPrefix(r.SubscriberID, "cto-agent-") {
			deployToCTO = true
		}
		if r.EventPattern == "deploy_requested" && strings.HasPrefix(r.SubscriberID, "devops-agent-") {
			deployToDevOps = true
		}
	}
	if bootstrap != 20 {
		t.Fatalf("expected 20 bootstrap routes, got %d", bootstrap)
	}
	if seeded != 0 {
		t.Fatalf("expected 0 seeded routes, got %d", seeded)
	}
	if !deployToCTO || !deployToDevOps {
		t.Fatalf("expected deploy_requested bootstrap routes to CTO and DevOps, got cto=%v devops=%v", deployToCTO, deployToDevOps)
	}
}

func TestDefaultOpCoRosterCEOIncludesCrossDomainReport(t *testing.T) {
	roster := DefaultOpCoRoster("v1")
	var found bool
	var ceo PersistedAgent
	for _, agent := range roster {
		if agent.Config.Role == "opco-ceo" {
			ceo = agent
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected opco-ceo in default roster")
	}
	has := false
	for _, sub := range ceo.Config.Subscriptions {
		if strings.TrimSpace(sub) == "cross_domain_report" {
			has = true
			break
		}
	}
	if !has {
		t.Fatalf("expected opco-ceo subscriptions to include cross_domain_report, got %v", ceo.Config.Subscriptions)
	}
}
