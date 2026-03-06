package commgraph

import (
	"path"
	"sort"
	"strings"
)

const (
	RuntimeProducerID = "sys:runtime"
	HumanProducerID   = "sys:human-board"
	MailboxNodeID     = "sys:mailbox"
)

type MessageAuthority struct {
	SenderRole     string
	RecipientRoles []string
	Scope          string // holding | opco | any
}

type MailboxRoundTrip struct {
	SenderRole     string
	MailboxType    string
	DecisionEvents []string
	ReturnToRole   string // "requesting-agent" uses sender as fallback in graph projections.
	Timeout        string
}

var runtimeEmittedEvents = []string{
	"system.started",
	"timer.portfolio_digest",
	"timer.marginal_review",
	"timer.infra_health_check",
	"campaign.completed",
	"vertical.discovered",
	"vertical.shortlisted",
	"vertical.killed",
	"validation.started",
	"validation.more_data_needed",
	"validation.package_ready",
	"brand.requested",
	"brand.revision_needed",
	"spec.revision_requested",
	"cto.spec_review_requested",
	"dedup.ambiguous",
	"synthesis.needed",
	"scan.completed",
	"market_research.scan_assigned",
	"trend_research.scan_assigned",
	"scanner.google_maps.scan_assigned",
	"scanner.instagram.scan_assigned",
	"scanner.reviews.scan_assigned",
	"scanner.directories.scan_assigned",
	"scanner.yelp.scan_assigned",
	"mailbox.item_decided",
	"opco.ceo_ready",
	"opco.teardown_complete",
	"user_onboarded",
	"human_task.requested",
	"human_task.expired",
	"founder_input.response",
	"devops.health_check_failed",
	"ops.agent_panic",
	"ops.agent_failed",
	"spec.contradiction_detected",
	"budget.threshold_crossed",
	"cycle_limit_reached",
}

var humanEmittedEvents = []string{
	"vertical.approved",
	"vertical.needs_more_data",
	"system.directive",
	"template.publish_requested",
	"template.migration_approved",
	"spend.approved",
	"spend.rejected",
	"review.product_spec_feedback",
	"review.deploy_feedback",
	"board.directive",
	"board.chat",
	"opco.teardown_requested",
	"human_task.completed",
}

var agentProducerEvents = map[string][]string{
	"empire-coordinator": {
		"scan.requested",
		"opco.spinup_requested",
		"template.migration_planned",
		"template.migration_completed",
		"template.migration_failed",
		"vertical.health_warning",
		"vertical.resumed",
		"portfolio.digest_compiled",
		"budget.warning",
		"budget.throttle",
		"budget.emergency",
		"budget.resumed",
		"human_task.approved",
		"human_task.rejected",
		"human_task.deferred",
		"scoring.contest_resolved",
		"opco.escalation_response",
	},
	"factory-cto": {
		"template.version_published",
		"cto.spec_approved",
		"cto.spec_revision_needed",
		"cto.spec_vetoed",
		"cto.architecture_directive",
		"cto.extraction_recommended",
		"cto.pattern_detected",
		"cto.tech_spec_feedback",
		"spec.validation_requested",
	},
	"holding-devops": {
		"devops.deploy_complete",
		"devops.deploy_failed",
		"devops.rollback_complete",
		"devops.rollback_failed",
		"devops.capacity_warning",
		"devops.infra_change_needed",
		"devops.ssl_provisioned",
		"devops.health_check_failed",
	},
	"operations-analyst": {
		"analyst.bootstrap_upgrade_proposal",
		"analyst.prompt_refinement_proposal",
		"analyst.anti_pattern_advisory",
	},
	"spec-auditor": {
		"spec.validation_passed",
		"spec.validation_failed",
	},
	"discovery-coordinator": {
		"dedup.resolved",
		"synthesis.resolved",
	},
	"market-research-agent": {
		"category.assessed",
		"market_research.scan_complete",
	},
	"trend-research-agent": {
		"trend.identified",
		"trend_research.scan_complete",
	},
	"scanner-agent": {
		"source.scraped",
		"scanner.google_maps.scan_complete",
		"scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete",
		"scanner.directories.scan_complete",
		"scanner.yelp.scan_complete",
	},
	"analysis-agent": {
		"score.dimension_complete",
		"vertical.derived",
	},
	"scoring-node": {
		"scoring.requested",
		"scoring.contested",
		"vertical.discovered",
		"vertical.scored",
		"vertical.shortlisted",
		"vertical.marginal",
		"vertical.rejected",
		"pipeline.dead_letter",
	},
	"validation-coordinator": {
		"vertical.ready_for_review",
	},
	"business-research-agent": {
		"research.completed",
		"research.vertical_rejected",
		"spec.requested",
		"spec.approved",
		"spec.revision_needed",
		"spec_review.requested",
	},
	"lightweight-spec-agent": {
		"spec.draft_ready",
	},
	"spec-reviewer": {
		"spec_review.passed",
		"spec_review.issues_found",
	},
	"pre-brand-agent": {
		"brand.candidates_ready",
	},
	"opco-ceo": {
		"opco.ceo_report",
		"opco.launched",
		"opco.escalation",
		"opco.spend_request",
		"opco.deploy_review",
		"opco.founder_input",
		"opco.steady_state_reached",
		"mandate_updated",
	},
	"chief-of-staff": {
		"cross_domain_report",
	},
	"vp-product": {
		"product_report",
		"product_escalation",
		"opco.product_spec_review",
	},
	"vp-growth": {
		"growth_report",
		"growth_escalation",
	},
	"cto-agent": {
		"build_complete",
		"build_blocked",
		"build_progress",
		"feature_deployed",
		"launch_ready",
		"deploy_requested",
		"bug_fix_deployed",
		"cycle_reset",
		"cto.tech_spec_review_requested",
		"spec.validation_requested",
	},
	"pm-agent": {
		"product_spec_ready",
	},
	"tech-writer": {
		"technical_spec_ready",
	},
	"qa-agent": {
		"qa.validation_passed",
		"qa.validation_failed",
	},
	"devops-agent": {
		"devops.deploy_requested",
		"devops.rollback_requested",
	},
	"support-agent": {
		"bug_reported",
		"feature_request",
		"support_digest",
		"support_critical",
		"churn_risk",
		"market_feedback",
	},
	"marketing-agent": {
		"prelaunch_ready",
		"outreach_digest",
		"channel_blocked",
		"channel_update",
		"spend_needed",
		"spend_request",
		"market_signals",
	},
	"inbound-gateway": {
		"inbound.whatsapp_message",
		"inbound.email",
	},
	"dashboard": {
		"human_task.assigned",
		"runtime.reset",
	},
	"actor-agent": {
		"opco.routing_updated",
	},
}

var roleAliases = map[string]string{
	"head-of-product": "vp-product",
	"head-of-growth":  "vp-growth",
	"cto":             "cto-agent",
	"opco-devops":     "devops-agent",
}

var messageAuthorityRegistry = []MessageAuthority{
	// Holding level.
	{SenderRole: "empire-coordinator", RecipientRoles: []string{"factory-cto", "holding-devops", "operations-analyst", "discovery-coordinator", "validation-coordinator", "spec-auditor", "opco-ceo"}, Scope: "any"},
	{SenderRole: "factory-cto", RecipientRoles: []string{"validation-coordinator", "operations-analyst", "cto-agent"}, Scope: "any"},
	{SenderRole: "operations-analyst", RecipientRoles: []string{"empire-coordinator", "factory-cto"}, Scope: "holding"},

	// OpCo level.
	{SenderRole: "opco-ceo", RecipientRoles: []string{"chief-of-staff", "vp-product", "vp-growth", "pm-agent", "cto-agent", "support-agent", "marketing-agent", "tech-writer", "backend-agent", "frontend-agent", "qa-agent", "devops-agent"}, Scope: "opco"},
	{SenderRole: "chief-of-staff", RecipientRoles: []string{"vp-product", "vp-growth", "opco-ceo"}, Scope: "opco"},
	{SenderRole: "vp-product", RecipientRoles: []string{"pm-agent", "cto-agent", "support-agent"}, Scope: "opco"},
	{SenderRole: "vp-growth", RecipientRoles: []string{"marketing-agent"}, Scope: "opco"},
	{SenderRole: "cto-agent", RecipientRoles: []string{"tech-writer", "backend-agent", "frontend-agent", "qa-agent", "devops-agent"}, Scope: "opco"},
}

var mailboxRoundTrips = []MailboxRoundTrip{
	{SenderRole: "validation-coordinator", MailboxType: "vertical_approval", DecisionEvents: []string{"vertical.approved", "vertical.killed", "vertical.needs_more_data"}, ReturnToRole: "empire-coordinator"},
	{SenderRole: "opco-ceo", MailboxType: "spend_request", DecisionEvents: []string{"spend.approved", "spend.rejected"}, ReturnToRole: "opco-ceo"},
	{SenderRole: "vp-product", MailboxType: "product_spec_review", DecisionEvents: []string{"review.product_spec_feedback"}, ReturnToRole: "vp-product", Timeout: "48h auto-proceed"},
	{SenderRole: "opco-ceo", MailboxType: "deploy_review", DecisionEvents: []string{"review.deploy_feedback"}, ReturnToRole: "opco-ceo", Timeout: "48h auto-proceed"},
	{SenderRole: "opco-ceo", MailboxType: "founder_input", DecisionEvents: []string{"founder_input.response"}, ReturnToRole: "opco-ceo", Timeout: "48h use CEO recommendation"},
	{SenderRole: "opco-ceo", MailboxType: "escalation", DecisionEvents: []string{"opco.escalation_response"}, ReturnToRole: "opco-ceo"},
	{SenderRole: "empire-coordinator", MailboxType: "template_migration", DecisionEvents: []string{"template.migration_approved"}, ReturnToRole: "empire-coordinator"},
	{SenderRole: "holding-devops", MailboxType: "capacity_warning", DecisionEvents: []string{"spend.approved", "spend.rejected"}, ReturnToRole: "holding-devops"},
	{SenderRole: "empire-coordinator", MailboxType: "health_warning", DecisionEvents: []string{"vertical.killed"}, ReturnToRole: "empire-coordinator"},
	{SenderRole: "empire-coordinator", MailboxType: "human_task", DecisionEvents: []string{"human_task.completed"}, ReturnToRole: "requesting-agent", Timeout: "auto_expire_hours"},
}

func RuntimeEvents() []string {
	return append([]string(nil), runtimeEmittedEvents...)
}

func HumanEvents() []string {
	return append([]string(nil), humanEmittedEvents...)
}

func MessageAuthorities() []MessageAuthority {
	out := make([]MessageAuthority, len(messageAuthorityRegistry))
	copy(out, messageAuthorityRegistry)
	return out
}

func MailboxRoundTrips() []MailboxRoundTrip {
	out := make([]MailboxRoundTrip, len(mailboxRoundTrips))
	copy(out, mailboxRoundTrips)
	return out
}

func CanonicalRole(role string) string {
	return canonicalRole(role)
}

func ProducerEventsForRole(role string) []string {
	key := canonicalRole(role)
	if key == "" {
		return nil
	}
	events := agentProducerEvents[key]
	if len(events) == 0 {
		return nil
	}
	out := make([]string, 0, len(events))
	seen := make(map[string]struct{}, len(events))
	for _, evt := range events {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		if _, ok := seen[evt]; ok {
			continue
		}
		seen[evt] = struct{}{}
		out = append(out, evt)
	}
	sort.Strings(out)
	return out
}

func ProducerRoles() []string {
	out := make([]string, 0, len(agentProducerEvents))
	for role := range agentProducerEvents {
		role = strings.TrimSpace(role)
		if role == "" {
			continue
		}
		out = append(out, role)
	}
	sort.Strings(out)
	return out
}

func KnownProducedEvents() map[string]struct{} {
	out := make(map[string]struct{}, 256)
	for _, evt := range runtimeEmittedEvents {
		if v := strings.TrimSpace(evt); v != "" {
			out[v] = struct{}{}
		}
	}
	for _, evt := range humanEmittedEvents {
		if v := strings.TrimSpace(evt); v != "" {
			out[v] = struct{}{}
		}
	}
	for _, events := range agentProducerEvents {
		for _, evt := range events {
			if v := strings.TrimSpace(evt); v != "" {
				out[v] = struct{}{}
			}
		}
	}
	return out
}

func HasProducerForPattern(pattern string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	known := KnownProducedEvents()
	if _, ok := known[pattern]; ok {
		return true
	}
	for evt := range known {
		if routeMatches(pattern, evt) {
			return true
		}
	}
	return false
}

func canonicalRole(role string) string {
	role = strings.TrimSpace(strings.ToLower(role))
	role = strings.ReplaceAll(role, "_", "-")
	role = strings.Join(strings.Fields(role), "-")
	if alias, ok := roleAliases[role]; ok {
		return alias
	}
	return role
}

func routeMatches(pattern, eventType string) bool {
	switch {
	case pattern == "", pattern == "*":
		return true
	default:
		if strings.Contains(pattern, "*") {
			if ok, err := path.Match(pattern, eventType); err == nil && ok {
				return true
			}
		}
		if strings.HasSuffix(pattern, "*") {
			return strings.HasPrefix(eventType, strings.TrimSuffix(pattern, "*"))
		}
		return pattern == eventType
	}
}
