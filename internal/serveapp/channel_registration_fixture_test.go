package serveapp

import "github.com/division-sh/swarm/internal/cliapp"

type registrationFixture struct {
	cliapp.ChannelRegistrationFactory
	open cliapp.ChannelRegistrationConstructor
}

func (f registrationFixture) Select(provider string) (cliapp.ChannelRegistrationBinding, bool) {
	binding, found := f.ChannelRegistrationFactory.Select(provider)
	if found {
		binding.Open = f.open
	}
	return binding, found
}

func fixtureRegistration(open cliapp.ChannelRegistrationConstructor) cliapp.ChannelRegistrationFactory {
	return registrationFixture{ChannelRegistrationFactory: ChannelRegistrations(), open: open}
}
