package cliapp

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func TestConfiguredChannelRegistrationRequiresOneExactBindingDeclaration(t *testing.T) {
	repo := RepoRoot()
	base := &config.Config{
		Channels: config.ChannelsConfig{
			Bindings: map[string]config.ChannelBindingConfig{
				"hitl": {
					Pack: "telegram", Destination: "-100123",
					Credentials: map[string]string{"telegram.telegram_bot_token": "telegram_hitl_bot"},
					Register:    "ingress:support:telegram",
				},
			},
		},
	}
	load := func(t *testing.T, cfg *config.Config) (ChannelPackLoad, error) {
		t.Helper()
		result := RuntimeConfigLoadResult{Config: cfg}
		spec, err := loadChannelPlatformSpecDocument(filepath.Join(repo, defaultPlatformSpecPath))
		if err != nil {
			t.Fatalf("loadChannelPlatformSpecDocument: %v", err)
		}
		projection, err := packadmission.Admit(packfixture.EmbeddedInventory(t), spec)
		if err != nil {
			t.Fatalf("admit embedded packs: %v", err)
		}
		return LoadConfiguredChannelPacks(context.Background(), result, projection, channelTestCredentialStore{"telegram_hitl_bot": "secret"}, nil)
	}

	loaded, err := load(t, base)
	if err != nil {
		t.Fatalf("LoadConfiguredChannelPacks: %v", err)
	}
	if len(loaded.Bindings) != 1 || loaded.Bindings[0].RegistrationTarget() != "ingress:support:telegram" {
		t.Fatalf("registration binding = %#v", loaded.Bindings)
	}
	if got := loaded.Bindings[0].CredentialStoreKeys()["telegram.telegram_bot_token"]; got != "telegram_hitl_bot" {
		t.Fatalf("credential mapping = %q", got)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*config.ChannelBindingConfig)
	}{
		{name: "missing explicit credentials", mutate: func(binding *config.ChannelBindingConfig) { binding.Credentials = nil }},
		{name: "malformed target", mutate: func(binding *config.ChannelBindingConfig) { binding.Register = "support:telegram" }},
		{name: "provider mismatch", mutate: func(binding *config.ChannelBindingConfig) { binding.Register = "ingress:support:slack" }},
		{name: "unknown credential role", mutate: func(binding *config.ChannelBindingConfig) { binding.Credentials["unused"] = "value" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := base.Channels.Bindings["hitl"]
			binding.Credentials = map[string]string{"telegram.telegram_bot_token": "telegram_hitl_bot"}
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
