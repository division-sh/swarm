package commgraph

type genericTestPolicy struct{}

func NewGenericTestPolicy() Policy {
	return genericTestPolicy{}
}

func (genericTestPolicy) MessageAuthorities() []MessageAuthority {
	return []MessageAuthority{
		{SenderRole: "coordinator", RecipientRoles: []string{"reviewer", "worker"}, Scope: "any"},
		{SenderRole: "empire-coordinator", RecipientRoles: []string{"reviewer", "worker", "opco-ceo", "chief-of-staff", "vp-product", "vp-growth", "cto-agent", "validation-coordinator"}, Scope: "any"},
		{SenderRole: "reviewer", RecipientRoles: []string{"coordinator"}, Scope: "local"},
	}
}

func (genericTestPolicy) MailboxRoundTrips() []MailboxRoundTrip {
	return []MailboxRoundTrip{
		{SenderRole: "reviewer", MailboxType: "generic_review", DecisionEvents: []string{"review.approved", "review.rejected"}, ReturnToRole: "coordinator"},
		{SenderRole: "empire-coordinator", MailboxType: "generic_review", DecisionEvents: []string{"review.approved", "review.rejected"}, ReturnToRole: "empire-coordinator"},
	}
}

func (genericTestPolicy) HumanTaskDecisionRoles() []string {
	return []string{"coordinator", "empire-coordinator", "validation-coordinator"}
}

func (genericTestPolicy) RoutingAuthorities() []RoutingAuthority {
	return []RoutingAuthority{
		{ActorRole: "coordinator"},
		{ActorRole: "empire-coordinator"},
		{ActorRole: "opco-ceo"},
		{ActorRole: "chief-of-staff", AllowedStatuses: []string{"proposed"}},
		{ActorRole: "vp-product", AllowedTargetRoles: []string{"backend-agent", "frontend-agent", "qa-agent", "pm-agent", "tech-writer"}},
		{ActorRole: "vp-growth", AllowedTargetRoles: []string{"marketing-agent", "support-agent"}},
		{ActorRole: "cto-agent", AllowedTargetRoles: []string{"backend-agent", "frontend-agent", "qa-agent", "devops-agent", "tech-writer"}},
	}
}

func (genericTestPolicy) ManagementAuthorities() []ManagementAuthority {
	return []ManagementAuthority{
		{ActorRole: "coordinator", AllowCrossVertical: true},
		{ActorRole: "empire-coordinator", AllowCrossVertical: true},
		{ActorRole: "opco-ceo", AllowCrossVertical: false},
		{ActorRole: "vp-product", AllowedTargetRoles: []string{"backend-agent", "frontend-agent", "qa-agent", "pm-agent", "tech-writer"}},
		{ActorRole: "vp-growth", AllowedTargetRoles: []string{"marketing-agent", "support-agent"}},
		{ActorRole: "cto-agent", AllowedTargetRoles: []string{"backend-agent", "frontend-agent", "qa-agent", "devops-agent", "tech-writer"}},
	}
}

func (genericTestPolicy) MailboxSendRoles() []string {
	return []string{
		"coordinator",
		"reviewer",
		"empire-coordinator",
		"opco-ceo",
		"validation-coordinator",
		"vp-growth",
		"support-agent",
		"marketing-agent",
	}
}
