package commgraph

type Policy interface {
	MessageAuthorities() []MessageAuthority
	MailboxRoundTrips() []MailboxRoundTrip
	HumanTaskDecisionRoles() []string
	RoutingAuthorities() []RoutingAuthority
	ManagementAuthorities() []ManagementAuthority
	MailboxSendRoles() []string
}

var defaultPolicyFactory func() Policy

func SetDefaultPolicyFactory(factory func() Policy) {
	defaultPolicyFactory = factory
}

func defaultPolicyOrNil() Policy {
	if defaultPolicyFactory == nil {
		return nil
	}
	return defaultPolicyFactory()
}
