package packs

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func qualifyChannelBodyReferences(root yamlsource.Value, manifest *ChannelManifest) error {
	fields, err := channelFields(root)
	if err != nil {
		return err
	}
	operations, err := channelFields(fields["operations"])
	if err != nil {
		return err
	}
	for _, name := range sortedChannelNames(operations) {
		binding := manifest.Operations[name]
		binding.Tool, err = qualifyChannelMemberField(operations[name], "tool", manifest.Provider)
		if err != nil {
			return err
		}
		manifest.Operations[name] = binding
	}
	events, err := channelFields(fields["events"])
	if err != nil {
		return err
	}
	for _, name := range sortedChannelNames(events) {
		field, err := events[name].Lookup("event")
		if err != nil {
			return err
		}
		binding := manifest.Events[name]
		binding.Event, err = eventidentity.QualifyProviderEvent(manifest.Provider, binding.Event)
		if err != nil {
			return channelError(field.Value, err.Error())
		}
		manifest.Events[name] = binding
	}
	if manifest.Onboarding != nil {
		if err := qualifyChannelOnboardingRoles(fields["onboarding"], manifest.Provider, manifest.Onboarding); err != nil {
			return err
		}
	}
	if manifest.Registration != nil {
		return qualifyChannelRegistrationReferences(fields["registration"], manifest.Provider, manifest.Registration)
	}
	return nil
}

func qualifyChannelMemberField(parent yamlsource.Value, field, provider string) (string, error) {
	value, err := parent.Lookup(field)
	if err != nil {
		return "", err
	}
	return qualifyChannelMember(value.Value, provider)
}

func qualifyChannelMember(value yamlsource.Value, provider string) (string, error) {
	local, err := channelText(value)
	if err != nil {
		return "", err
	}
	qualified, err := runtimecontracts.QualifyProviderLocalName(provider, local)
	if err != nil {
		return "", channelError(value, err.Error())
	}
	return qualified, nil
}

func qualifyChannelOnboardingRoles(source yamlsource.Value, provider string, profile *ChannelOnboardingProfile) error {
	for _, role := range []struct {
		field string
		value *string
	}{
		{"provider_credential", &profile.ProviderCredentialRole},
		{"signing_credential", &profile.SigningCredentialRole},
	} {
		field, err := source.Lookup(role.field)
		if err != nil {
			return err
		}
		if field.Presence == yamlsource.PresenceMissing {
			continue
		}
		if *role.value, err = qualifyChannelMember(field.Value, provider); err != nil {
			return err
		}
	}
	return nil
}

func qualifyChannelRegistrationReferences(source yamlsource.Value, provider string, profile *ChannelRegistrationProfile) error {
	fields, err := channelFields(source)
	if err != nil {
		return err
	}
	slot, err := channelFields(fields["slot"])
	if err != nil {
		return err
	}
	for _, operation := range []struct {
		source yamlsource.Value
		value  *ChannelRegistrationOperation
	}{
		{slot["identify"], &profile.Slot.Identify},
		{fields["apply"], &profile.Apply},
		{fields["readback"], &profile.Readback},
	} {
		operation.value.Tool, err = qualifyChannelMemberField(operation.source, "tool", provider)
		if err != nil {
			return err
		}
	}
	credentials, err := channelFields(fields["credentials"])
	if err != nil {
		return err
	}
	roles, err := credentials["provider"].Sequence()
	if err != nil {
		return err
	}
	for i, role := range roles {
		profile.Credentials.Provider[i], err = qualifyChannelMember(role, provider)
		if err != nil {
			return err
		}
	}
	profile.Credentials.Signing, err = qualifyChannelMember(credentials["signing"], provider)
	return err
}
