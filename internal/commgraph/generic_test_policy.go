package commgraph

type genericTestPolicy struct{}

func NewGenericTestPolicy() Policy {
	return genericTestPolicy{}
}

func (genericTestPolicy) MessageAuthorities() []MessageAuthority {
	return []MessageAuthority{
		{SenderRole: "control-plane", RecipientRoles: []string{"reviewer", "worker"}, Scope: "any"},
		{SenderRole: "reviewer", RecipientRoles: []string{"control-plane"}, Scope: "local"},
	}
}

func (genericTestPolicy) MailboxRoundTrips() []MailboxRoundTrip {
	return []MailboxRoundTrip{
		{SenderRole: "reviewer", MailboxType: "generic_review", DecisionEvents: []string{"review.approved", "review.rejected"}, ReturnToRole: "control-plane"},
	}
}

func (genericTestPolicy) HumanTaskDecisionRoles() []string {
	return []string{"control-plane", "reviewer"}
}

func (genericTestPolicy) RoutingAuthorities() []RoutingAuthority {
	return []RoutingAuthority{
		{ActorRole: "control-plane"},
		{ActorRole: "reviewer", AllowedStatuses: []string{"proposed"}},
	}
}

func (genericTestPolicy) ManagementAuthorities() []ManagementAuthority {
	return []ManagementAuthority{
		{ActorRole: "control-plane", AllowCrossEntity: true},
		{ActorRole: "reviewer", AllowedTargetRoles: []string{"worker"}, AllowCrossEntity: false},
	}
}

func (genericTestPolicy) MailboxSendRoles() []string {
	return []string{
		"control-plane",
		"reviewer",
	}
}
