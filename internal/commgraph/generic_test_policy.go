package commgraph

type genericTestPolicy struct{}

func NewGenericTestPolicy() Policy {
	return genericTestPolicy{}
}

func (genericTestPolicy) MessageAuthorities() []MessageAuthority {
	return []MessageAuthority{
		{SenderRole: "coordinator", RecipientRoles: []string{"reviewer", "worker"}, Scope: "any"},
		{SenderRole: "reviewer", RecipientRoles: []string{"coordinator"}, Scope: "local"},
	}
}

func (genericTestPolicy) MailboxRoundTrips() []MailboxRoundTrip {
	return []MailboxRoundTrip{
		{SenderRole: "reviewer", MailboxType: "generic_review", DecisionEvents: []string{"review.approved", "review.rejected"}, ReturnToRole: "coordinator"},
	}
}

func (genericTestPolicy) HumanTaskDecisionRoles() []string {
	return []string{"coordinator", "reviewer"}
}

func (genericTestPolicy) RoutingAuthorities() []RoutingAuthority {
	return []RoutingAuthority{
		{ActorRole: "coordinator"},
		{ActorRole: "reviewer", AllowedStatuses: []string{"proposed"}},
	}
}

func (genericTestPolicy) ManagementAuthorities() []ManagementAuthority {
	return []ManagementAuthority{
		{ActorRole: "coordinator", AllowCrossVertical: true},
		{ActorRole: "reviewer", AllowedTargetRoles: []string{"worker"}, AllowCrossVertical: false},
	}
}

func (genericTestPolicy) MailboxSendRoles() []string {
	return []string{
		"coordinator",
		"reviewer",
	}
}
