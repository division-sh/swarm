package manager

import (
	"fmt"

	"empireai/internal/models"
)

func DefaultOpCoRoster(verticalID string) []PersistedAgent {
	mk := func(role, typ, parent string, subs ...string) PersistedAgent {
		return PersistedAgent{
			Config: models.AgentConfig{
				ID:            OpCoAgentID(role, verticalID),
				Type:          typ,
				Role:          role,
				Mode:          "operating",
				VerticalID:    verticalID,
				ParentAgent:   parent,
				Subscriptions: subs,
			},
			ParentAgentID:   parent,
			CoordinatorID:   OpCoAgentID("opco-ceo", verticalID),
			Status:          "active",
			HiredBy:         "agent-manager",
			TemplateVersion: "2.2.1",
		}
	}

	ceo := OpCoAgentID("opco-ceo", verticalID)
	vpProduct := OpCoAgentID("vp-product", verticalID)
	vpGrowth := OpCoAgentID("vp-growth", verticalID)
	cto := OpCoAgentID("cto-agent", verticalID)

	return []PersistedAgent{
		mk("opco-ceo", "operating", "", "opco.spinup_requested", "product_report", "growth_report", "cross_domain_report", "product_escalation", "growth_escalation", "spend_request", "spend.approved", "spend.rejected", "cto.architecture_directive", "founder_input.response", "opco.escalation_response", "launch_ready"),
		mk("chief-of-staff", "operating", ceo, "product_report", "growth_report", "feature_deployed", "churn_risk", "build_complete", "prelaunch_ready", "support_critical", "channel_blocked", "mandate_updated"),
		mk("vp-product", "operating", ceo, "build_complete", "build_blocked", "product_escalation", "support_digest", "support_critical", "build_progress", "churn_risk", "spend_needed", "mandate_updated", "market_feedback", "feature_deployed"),
		mk("vp-growth", "operating", ceo, "outreach_digest", "channel_blocked", "user_onboarded", "prelaunch_ready", "spend_needed", "mandate_updated", "channel_update", "market_signals"),
		mk("cto-agent", "operating", vpProduct),
		mk("pm-agent", "operating", vpProduct),
		mk("support-agent", "operating", vpProduct),
		mk("marketing-agent", "operating", vpGrowth),
		mk("tech-writer", "operating", cto),
		mk("backend-agent", "operating", cto),
		mk("frontend-agent", "operating", cto),
		mk("qa-agent", "operating", cto),
		mk("devops-agent", "operating", cto),
	}
}

func DefaultOpCoRoutes(verticalID string) []PersistedRoutingRule {
	ceo := OpCoAgentID("opco-ceo", verticalID)
	cto := OpCoAgentID("cto-agent", verticalID)
	pm := OpCoAgentID("pm-agent", verticalID)
	backend := OpCoAgentID("backend-agent", verticalID)
	frontend := OpCoAgentID("frontend-agent", verticalID)
	devops := OpCoAgentID("devops-agent", verticalID)
	marketing := OpCoAgentID("marketing-agent", verticalID)
	support := OpCoAgentID("support-agent", verticalID)

	bootstrap := func(pattern, sub string) PersistedRoutingRule {
		return PersistedRoutingRule{
			VerticalID:       verticalID,
			EventPattern:     pattern,
			SubscriberID:     sub,
			InstalledBy:      ceo,
			Reason:           "bootstrap",
			Status:           "active",
			Source:           "bootstrap",
			BootstrapVersion: 1,
		}
	}
	return []PersistedRoutingRule{
		bootstrap("product_spec_ready", cto),
		bootstrap("cto.tech_spec_review_requested", OpCoAgentID("tech-writer", verticalID)),
		bootstrap("technical_spec_ready", cto),
		bootstrap("technical_spec_ready", backend),
		bootstrap("technical_spec_ready", frontend),
		bootstrap("build_progress", cto),
		bootstrap("build_blocked", cto),
		bootstrap("deploy_requested", cto),
		bootstrap("deploy_requested", devops),
		bootstrap("qa.validation_passed", cto),
		bootstrap("qa.validation_failed", cto),
		bootstrap("devops.infra_change_needed", "holding-devops"),
		bootstrap("bug_reported", cto),
		bootstrap("feature_request", pm),
		bootstrap("feature_request", cto),
		bootstrap("cycle_limit_reached", cto),
		bootstrap("inbound.whatsapp_message", support),
		bootstrap("inbound.email", support),
		bootstrap("feature_deployed", marketing),
		bootstrap("bug_fix_deployed", support),
	}
}

func OpCoAgentID(role, verticalID string) string {
	return fmt.Sprintf("%s-%s", role, verticalID)
}

func RouteRuleKey(verticalID, eventPattern, subscriberID string) string {
	return verticalID + "|" + eventPattern + "|" + subscriberID
}
