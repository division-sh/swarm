package packs

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func (m ChannelManifest) SourceValue() yamlsource.Value { return m.source }

func parseChannelManifestAt(body []byte, file string) (ChannelManifest, error) {
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return ChannelManifest{}, err
	}
	root := snapshot.Document(file).Root()
	if err := root.ValidateExpansion(); err != nil {
		return ChannelManifest{}, err
	}
	f, err := channelFields(root, "provider", "opaque_types", "operations", "events", "registration", "onboarding", "native_inbox")
	if err != nil {
		return ChannelManifest{}, err
	}
	out := ChannelManifest{source: root, OpaqueTypes: map[string]runtimecontracts.ToolInputSchema{}, Operations: map[string]ChannelOperationBinding{}, Events: map[string]ChannelEventBinding{}}
	if out.Provider, err = channelRequiredText(root, f, "provider"); err != nil {
		return out, err
	}
	if err := admitChannelBindings(root, f, &out); err != nil {
		return out, err
	}
	if v, ok := f["native_inbox"]; ok {
		profile, e := admitChannelNativeInbox(v)
		if e != nil {
			return out, e
		}
		out.NativeInbox = &profile
	}
	if v, ok := f["registration"]; ok {
		profile, e := admitChannelRegistration(v)
		if e != nil {
			return out, e
		}
		out.Registration = &profile
	}
	if v, ok := f["onboarding"]; ok {
		profile, e := admitChannelOnboarding(v)
		if e != nil {
			return out, e
		}
		out.Onboarding = &profile
	}
	return out, nil
}

func admitChannelNativeInbox(value yamlsource.Value) (NativeInboxProfile, error) {
	f, err := channelFields(value, "kind", "client_languages", "direct_launcher_read", "default_launcher_read", "commands_launcher", "inherited_launcher")
	var out NativeInboxProfile
	if err != nil {
		return out, err
	}
	for _, name := range []string{"kind", "direct_launcher_read", "default_launcher_read", "commands_launcher", "inherited_launcher"} {
		text, e := channelRequiredText(value, f, name)
		if e != nil {
			return out, e
		}
		switch name {
		case "kind":
			out.Kind = text
		case "direct_launcher_read":
			out.DirectLauncherRead = text
		case "default_launcher_read":
			out.DefaultLauncherRead = text
		case "commands_launcher":
			out.CommandsLauncher = text
		case "inherited_launcher":
			out.InheritedLauncher = text
		}
	}
	languages, err := channelRequired(value, f, "client_languages")
	if err != nil {
		return out, err
	}
	rows, err := languages.Sequence()
	if err != nil {
		return out, err
	}
	for _, row := range rows {
		text, e := channelText(row)
		if e != nil {
			return out, e
		}
		out.ClientLanguages = append(out.ClientLanguages, text)
	}
	return out, nil
}

func admitChannelOperation(value yamlsource.Value) (ChannelRegistrationOperation, error) {
	f, err := channelFields(value, "tool", "input", "output")
	var out ChannelRegistrationOperation
	if err != nil {
		return out, err
	}
	if out.Tool, err = channelRequiredText(value, f, "tool"); err != nil {
		return out, err
	}
	for _, name := range []string{"input", "output"} {
		if v, ok := f[name]; ok {
			mappings, e := admitChannelMappings(v, name == "input")
			if e != nil {
				return out, e
			}
			if name == "input" {
				out.Input = mappings
			} else {
				out.Output = mappings
			}
		}
	}
	return out, nil
}

func admitChannelMappings(value yamlsource.Value, allowEach bool) (map[string]ChannelMapping, error) {
	f, err := channelFields(value)
	if err != nil {
		return nil, err
	}
	out := make(map[string]ChannelMapping, len(f))
	for _, name := range sortedChannelNames(f) {
		v := f[name]
		if v.Presence() == yamlsource.PresenceScalar {
			text, e := channelText(v)
			if e != nil {
				return nil, e
			}
			out[name] = ChannelMapping{From: strings.TrimSpace(text)}
			continue
		}
		if !allowEach {
			return nil, channelError(v, "channel mapping must be a source path; each is supported only in operation input")
		}
		members, e := channelFields(v, "each", "item")
		if e != nil {
			return nil, e
		}
		text, e := channelRequiredText(v, members, "each")
		if e != nil {
			return nil, e
		}
		item, e := channelRequired(v, members, "item")
		if e != nil {
			return nil, e
		}
		rows, e := item.Sequence()
		if e != nil {
			return nil, e
		}
		if len(rows) == 0 {
			return nil, channelError(item, "channel each mapping requires nonempty item")
		}
		mapping := ChannelMapping{Each: strings.TrimSpace(text), Item: make([]map[string]ChannelMapping, len(rows))}
		for i, row := range rows {
			if mapping.Item[i], e = admitChannelMappings(row, false); e != nil {
				return nil, e
			}
			if len(mapping.Item[i]) == 0 {
				return nil, channelError(row, "channel item requires fields")
			}
		}
		out[name] = mapping
	}
	return out, nil
}

func admitChannelRegistration(value yamlsource.Value) (ChannelRegistrationProfile, error) {
	f, err := channelFields(value, "slot", "credentials", "apply", "readback")
	var out ChannelRegistrationProfile
	if err != nil {
		return out, err
	}
	slot, err := channelRequired(value, f, "slot")
	if err != nil {
		return out, err
	}
	s, err := channelFields(slot, "namespace", "identify")
	if err != nil {
		return out, err
	}
	if out.Slot.Namespace, err = channelRequiredText(slot, s, "namespace"); err != nil {
		return out, err
	}
	identify, err := channelRequired(slot, s, "identify")
	if err != nil {
		return out, err
	}
	if out.Slot.Identify, err = admitChannelOperation(identify); err != nil {
		return out, err
	}
	credentials, err := channelRequired(value, f, "credentials")
	if err != nil {
		return out, err
	}
	c, err := channelFields(credentials, "provider", "signing")
	if err != nil {
		return out, err
	}
	provider, err := channelRequired(credentials, c, "provider")
	if err != nil {
		return out, err
	}
	rows, err := provider.Sequence()
	if err != nil {
		return out, err
	}
	if len(rows) == 0 {
		return out, channelError(provider, "provider credentials are required")
	}
	for _, row := range rows {
		text, e := channelText(row)
		if e != nil {
			return out, e
		}
		out.Credentials.Provider = append(out.Credentials.Provider, text)
	}
	if out.Credentials.Signing, err = channelRequiredText(credentials, c, "signing"); err != nil {
		return out, err
	}
	for _, name := range []string{"apply", "readback"} {
		v, e := channelRequired(value, f, name)
		if e != nil {
			return out, e
		}
		op, e := admitChannelOperation(v)
		if e != nil {
			return out, e
		}
		if name == "apply" {
			out.Apply = op
		} else {
			out.Readback = op
		}
	}
	return out, nil
}

func admitChannelOnboarding(value yamlsource.Value) (ChannelOnboardingProfile, error) {
	f, err := channelFields(value, "activation", "ceremony", "provider_credential", "confirmation", "signing_credential", "connection_health", "learned_destination")
	var out ChannelOnboardingProfile
	if err != nil {
		return out, err
	}
	for _, name := range []string{"activation", "ceremony", "provider_credential", "confirmation"} {
		text, e := channelRequiredText(value, f, name)
		if e != nil {
			return out, e
		}
		switch name {
		case "activation":
			out.Activation = text
		case "ceremony":
			out.Ceremony = text
		case "provider_credential":
			out.ProviderCredentialRole = text
		case "confirmation":
			out.Confirmation = text
		}
	}
	required, forbidden := "signing_credential", "connection_health"
	if out.Activation == string(ChannelActivationSessionConnection) {
		required, forbidden = forbidden, required
	}
	if v, ok := f[forbidden]; ok {
		return out, channelError(v, "field "+forbidden+" is forbidden for "+out.Activation)
	}
	text, err := channelRequiredText(value, f, required)
	if err != nil {
		return out, err
	}
	if required == "signing_credential" {
		out.SigningCredentialRole = text
	} else {
		out.ConnectionHealth = text
	}
	destination, err := channelRequired(value, f, "learned_destination")
	if err != nil {
		return out, err
	}
	out.LearnedDestination, err = admitChannelMappings(destination, false)
	if err != nil {
		return out, err
	}
	if len(out.LearnedDestination) == 0 {
		return out, channelError(destination, "learned_destination requires a nonempty mapping")
	}
	return out, nil
}

func channelFields(value yamlsource.Value, allowed ...string) (map[string]yamlsource.Value, error) {
	entries, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]yamlsource.Value, len(entries))
	for _, entry := range entries {
		if entry.KeyTag != "!!str" || strings.TrimSpace(entry.Name) == "" {
			return nil, channelError(entry.Value, "field name must be nonempty text")
		}
		if _, ok := out[entry.Name]; ok {
			return nil, channelError(entry.Value, "duplicate field "+entry.Name)
		}
		if len(allowed) > 0 {
			known := false
			for _, name := range allowed {
				known = known || entry.Name == name
			}
			if !known {
				return nil, channelError(entry.Value, "field "+entry.Name+" not found in channel declaration; mappings use a scalar source or each/item")
			}
		}
		out[entry.Name] = entry.Value
	}
	return out, nil
}

func channelText(value yamlsource.Value) (string, error) {
	s, err := value.Scalar()
	if err != nil || s.Tag != "!!str" || strings.TrimSpace(s.Value) == "" {
		return "", channelError(value, "want nonempty text")
	}
	return s.Value, nil
}

func channelRequired(parent yamlsource.Value, fields map[string]yamlsource.Value, name string) (yamlsource.Value, error) {
	if v, ok := fields[name]; ok {
		return v, nil
	}
	missing, err := parent.Lookup(name)
	if err != nil {
		return yamlsource.Value{}, err
	}
	return missing.Value, fmt.Errorf("%s required field is missing from mapping at %s", missing.Value.SemanticPath(), parent.Location())
}

func channelRequiredText(parent yamlsource.Value, fields map[string]yamlsource.Value, name string) (string, error) {
	v, err := channelRequired(parent, fields, name)
	if err != nil {
		return "", err
	}
	return channelText(v)
}

func channelTextMap(value yamlsource.Value) (map[string]string, error) {
	f, err := channelFields(value)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(f))
	for _, name := range sortedChannelNames(f) {
		if out[name], err = channelText(f[name]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func sortedChannelNames(fields map[string]yamlsource.Value) []string {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func channelError(value yamlsource.Value, message string) error {
	return fmt.Errorf("%s at %s: %s", value.SemanticPath(), value.Location(), message)
}

func admitChannelBindings(root yamlsource.Value, f map[string]yamlsource.Value, out *ChannelManifest) error {
	var err error
	for _, group := range []string{"opaque_types", "operations", "events"} {
		v, e := channelRequired(root, f, group)
		if e != nil {
			return e
		}
		members, e := channelFields(v)
		if e != nil {
			return e
		}
		if len(members) == 0 {
			return channelError(v, "requires a nonempty mapping")
		}
		for _, id := range sortedChannelNames(members) {
			value := members[id]
			switch group {
			case "opaque_types":
				out.OpaqueTypes[id], err = runtimecontracts.AdmitToolInputSchemaValue(value)
			case "operations":
				var op ChannelRegistrationOperation
				op, err = admitChannelOperation(value)
				out.Operations[id] = ChannelOperationBinding{Tool: op.Tool, Input: op.Input, Output: op.Output}
			case "events":
				var fields map[string]yamlsource.Value
				fields, err = channelFields(value, "event", "fields")
				var event ChannelEventBinding
				if err == nil {
					event.Event, err = channelRequiredText(value, fields, "event")
				}
				var fieldsValue yamlsource.Value
				if err == nil {
					fieldsValue, err = channelRequired(value, fields, "fields")
				}
				if err == nil {
					event.Fields, err = channelTextMap(fieldsValue)
				}
				out.Events[id] = event
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}
