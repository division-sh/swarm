package cliapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func TestChannelPlatformSpecExplicitLoaderRefusesInvalidSources(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
	}{
		{"missing", "", "read platform spec:"},
		{"malformed", "platform: [unfinished\n", "unmarshal platform spec:"},
		{"draft", "platform: {name: draft}\n", "platform.version missing or duplicated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "explicit-spec.yaml")
			if tc.name != "missing" {
				if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := loadChannelPlatformSpecDocument(path); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("explicit %s spec error = %v, want %q", tc.name, err, tc.want)
			}
		})
	}
}

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
	credentials := channelTestCredentialStore{"telegram_hitl_bot": "secret"}
	load := func(t *testing.T, cfg *config.Config, static runtimecredentials.Store) (ChannelPackLoad, error) {
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
		return LoadConfiguredChannelPacks(context.Background(), result, projection, static, nil)
	}

	loaded, err := load(t, base, credentials)
	if err != nil {
		t.Fatalf("LoadConfiguredChannelPacks: %v", err)
	}
	if len(loaded.Bindings) != 1 || loaded.Bindings[0].RegistrationTarget() != "ingress:support:telegram" {
		t.Fatalf("registration binding = %#v", loaded.Bindings)
	}
	if got := loaded.Bindings[0].CredentialStoreKeys()["telegram.telegram_bot_token"]; got != "telegram_hitl_bot" {
		t.Fatalf("credential mapping = %q", got)
	}
	t.Run("source_projects_the_mapped_native_observation", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "channel-credentials.json")
		file, err := runtimecredentials.NewFileStore(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Set(t.Context(), "exact.channel.key", "private-channel-token"); err != nil {
			t.Fatal(err)
		}
		binding := base.Channels.Bindings["hitl"]
		binding.Credentials = map[string]string{"telegram.telegram_bot_token": "exact.channel.key"}
		mappedConfig := *base
		mappedConfig.Channels.Bindings = map[string]config.ChannelBindingConfig{"hitl": binding}
		mapped, err := load(t, &mappedConfig, file)
		if err != nil {
			t.Fatal(err)
		}
		facts := mapped.CredentialSources()
		if len(facts) == 0 {
			t.Fatal("mapped native channel observation lost source provenance")
		}
		for _, fact := range facts {
			if fact.Key != "exact.channel.key" || fact.Reference != "file:"+path {
				t.Fatalf("channel invented a logical key or a different source: %+v", fact)
			}
		}
	})

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
			if _, err := load(t, &candidate, credentials); err == nil {
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
