package cliapp

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestConfiguredChannelPackDrivesAvailableAndOutboundReadinessSurfaces(t *testing.T) {
	repo := RepoRoot()
	cfg := &config.Config{
		ProviderTriggers: config.ProviderTriggersConfig{Packs: config.ProviderTriggerPacksConfig{
			PlatformDirs: []string{"packs/provider-triggers/telegram"},
		}},
		Channels: config.ChannelsConfig{
			Packs: config.ChannelPacksConfig{PlatformDirs: []string{"packs/channels/telegram"}},
			Bindings: map[string]config.ChannelBindingConfig{
				"ops": {Pack: "provider.telegram.hitl_channel", Destination: "-100123"},
			},
		},
	}
	cfgResult := RuntimeConfigLoadResult{Config: cfg, KeyOrigins: map[string]unifiedConfigKeyOrigin{}}
	triggers, err := LoadConfiguredProviderTriggerPacks(repo, cfgResult)
	if err != nil {
		t.Fatalf("LoadConfiguredProviderTriggerPacks: %v", err)
	}
	spec, err := loadChannelPlatformSpecDocument(filepath.Join(repo, defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("loadChannelPlatformSpecDocument: %v", err)
	}

	withoutCredential, err := LoadConfiguredChannelPacks(context.Background(), repo, cfgResult, spec, triggers.Catalog, nil, nil)
	if err != nil {
		t.Fatalf("LoadConfiguredChannelPacks without credential: %v", err)
	}
	if len(withoutCredential.Plans) != 1 || len(withoutCredential.Bindings) != 1 {
		t.Fatalf("channel load = %#v, want one plan and one binding", withoutCredential)
	}
	structural, err := withoutCredential.Plans[0].CapabilitySubject()
	if err != nil || structural.Kind != packs.SubjectChannelPack || structural.Status != packs.StatusAvailable {
		t.Fatalf("structural subject = %#v, err=%v", structural, err)
	}
	outbound, err := withoutCredential.Bindings[0].CapabilitySubject()
	if err != nil || outbound.Kind != packs.SubjectChannelOutbound || outbound.Status != packs.StatusNotReady {
		t.Fatalf("outbound subject without credential = %#v, err=%v", outbound, err)
	}

	credentials := channelTestCredentialStore{"telegram_bot_token": "secret"}
	ready, err := LoadConfiguredChannelPacks(context.Background(), repo, cfgResult, spec, triggers.Catalog, credentials, nil)
	if err != nil {
		t.Fatalf("LoadConfiguredChannelPacks with credential: %v", err)
	}
	outbound, err = ready.Bindings[0].CapabilitySubject()
	if err != nil || outbound.Status != packs.StatusReady {
		t.Fatalf("outbound subject with credential = %#v, err=%v", outbound, err)
	}

	report := LocalPreflightReport{}
	appendChannelCapabilitySubjects(&report, ready)
	if len(report.CapabilitySubjects) != 2 {
		t.Fatalf("preflight channel subjects = %#v, want structural and outbound", report.CapabilitySubjects)
	}

	connectors := providerconnectors.DefaultPackRegistry().PackDescriptors()
	for index := range connectors {
		if connectors[index].Identity.ID() != "provider.telegram.connector" {
			continue
		}
		for toolID, scope := range map[string]string{"telegram.send_interactive": "deliver", "telegram.edit_message": "edit"} {
			tool := connectors[index].Tools[toolID]
			tool, err = tool.WithManagedCredential(runtimecontracts.ManagedCredentialRef{Key: "shared-channel-auth", Scopes: []string{scope}})
			if err != nil {
				t.Fatalf("derive conflicting connector tool %q: %v", toolID, err)
			}
			connectors[index].Tools[toolID] = tool
		}
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatalf("NewInterfaceRegistry: %v", err)
	}
	conflicting, err := packs.CompileChannelInventory(registry, ready.Loaded, triggers.Catalog.PackDescriptors(), connectors)
	if err != nil {
		t.Fatalf("CompileChannelInventory: %v", err)
	}
	if _, err := compileChannelBindings(context.Background(), cfg, conflicting, nil, nil); err == nil {
		t.Fatal("incompatible same-key channel credential requirements were accepted")
	}
}

func TestConfiguredChannelRegistrationRequiresOneExactBindingDeclaration(t *testing.T) {
	repo := RepoRoot()
	base := &config.Config{
		ProviderTriggers: config.ProviderTriggersConfig{Packs: config.ProviderTriggerPacksConfig{
			PlatformDirs: []string{"packs/provider-triggers/telegram"},
		}},
		Channels: config.ChannelsConfig{
			Packs: config.ChannelPacksConfig{PlatformDirs: []string{"packs/channels/telegram"}},
			Bindings: map[string]config.ChannelBindingConfig{
				"hitl": {
					Pack: "provider.telegram.hitl_channel", Destination: "-100123",
					Credentials: map[string]string{"telegram_bot_token": "telegram_hitl_bot"},
					Register:    "ingress:support:telegram:telegram",
				},
			},
		},
	}
	load := func(t *testing.T, cfg *config.Config) (ChannelPackLoad, error) {
		t.Helper()
		result := RuntimeConfigLoadResult{Config: cfg, KeyOrigins: map[string]unifiedConfigKeyOrigin{}}
		triggers, err := LoadConfiguredProviderTriggerPacks(repo, result)
		if err != nil {
			t.Fatalf("LoadConfiguredProviderTriggerPacks: %v", err)
		}
		spec, err := loadChannelPlatformSpecDocument(filepath.Join(repo, defaultPlatformSpecPath))
		if err != nil {
			t.Fatalf("loadChannelPlatformSpecDocument: %v", err)
		}
		return LoadConfiguredChannelPacks(context.Background(), repo, result, spec, triggers.Catalog, channelTestCredentialStore{"telegram_hitl_bot": "secret"}, nil)
	}

	loaded, err := load(t, base)
	if err != nil {
		t.Fatalf("LoadConfiguredChannelPacks: %v", err)
	}
	if len(loaded.Bindings) != 1 || loaded.Bindings[0].RegistrationTarget() != "ingress:support:telegram:telegram" {
		t.Fatalf("registration binding = %#v", loaded.Bindings)
	}
	if got := loaded.Bindings[0].CredentialStoreKeys()["telegram_bot_token"]; got != "telegram_hitl_bot" {
		t.Fatalf("credential mapping = %q", got)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*config.ChannelBindingConfig)
	}{
		{name: "missing explicit credentials", mutate: func(binding *config.ChannelBindingConfig) { binding.Credentials = nil }},
		{name: "malformed target", mutate: func(binding *config.ChannelBindingConfig) { binding.Register = "support:telegram" }},
		{name: "provider mismatch", mutate: func(binding *config.ChannelBindingConfig) { binding.Register = "ingress:support:telegram:slack" }},
		{name: "unknown credential role", mutate: func(binding *config.ChannelBindingConfig) { binding.Credentials["unused"] = "value" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := base.Channels.Bindings["hitl"]
			binding.Credentials = map[string]string{"telegram_bot_token": "telegram_hitl_bot"}
			tc.mutate(&binding)
			candidate := *base
			candidate.Channels = base.Channels
			candidate.Channels.Bindings = map[string]config.ChannelBindingConfig{"hitl": binding}
			if _, err := load(t, &candidate); err == nil {
				t.Fatal("invalid registration binding was accepted")
			}
		})
	}
}

type channelTestCredentialStore map[string]string

var _ runtimecredentials.Store = channelTestCredentialStore{}

func (s channelTestCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	value, ok := s[key]
	return value, ok, nil
}

func (s channelTestCredentialStore) Set(_ context.Context, key, value string) error {
	s[key] = value
	return nil
}

func (s channelTestCredentialStore) List(_ context.Context) ([]string, error) {
	keys := make([]string, 0, len(s))
	for key := range s {
		keys = append(keys, key)
	}
	return keys, nil
}

func (s channelTestCredentialStore) Delete(_ context.Context, key string) error {
	delete(s, key)
	return nil
}
