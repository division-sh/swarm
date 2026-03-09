package empire

import "empireai/internal/commgraph"

type policy struct{}

func New() commgraph.Policy {
	return policy{}
}

func (policy) MessageAuthorities() []commgraph.MessageAuthority {
	return []commgraph.MessageAuthority{
		{SenderRole: "empire-coordinator", RecipientRoles: []string{"factory-cto", "holding-devops", "operations-analyst", "discovery-coordinator", "validation-coordinator", "spec-auditor", "opco-ceo"}, Scope: "any"},
		{SenderRole: "factory-cto", RecipientRoles: []string{"validation-coordinator", "operations-analyst", "cto-agent"}, Scope: "any"},
		{SenderRole: "operations-analyst", RecipientRoles: []string{"empire-coordinator", "factory-cto"}, Scope: "holding"},
		{SenderRole: "chief-of-staff", RecipientRoles: []string{"vp-product", "vp-growth", "opco-ceo"}, Scope: "opco"},
	}
}

func (policy) MailboxRoundTrips() []commgraph.MailboxRoundTrip {
	return []commgraph.MailboxRoundTrip{
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
}
