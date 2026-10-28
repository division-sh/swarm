package cliapp

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
	for _, plan := range loaded.Plans {
		if subject, err := plan.CapabilitySubject(); err != nil || subject.ID != plan.ChannelIdentity().ID() {
			t.Fatalf("standalone installed channel identity changed: %+v error=%v", subject, err)
		}
		if plan.MatchesCapturedHolder(".") {
			t.Fatal("standalone channel acquired a captured holder")
		}
	}
	t.Run("captured_projection_retains_scope_and_configured_binding", func(t *testing.T) {
		root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
		if err := os.MkdirAll(filepath.Join(root, "other"), 0o700); err != nil {
			t.Fatal(err)
		}
		writeRuntimeConfigText(t, filepath.Join(root, "other/manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\ncapabilities:\n  can:\n    emit_events: [raw]\n  cannot: [execute code]\n")
		writeRuntimeConfigText(t, filepath.Join(root, "other/trigger.yaml"), "transport: webhook\ndelivery_id: {json_path: $.id, required: true}\nevent_name: {literal: raw}\nack: {mode: durable_before_dispatch}\n")
		_, bundle, err := NewSwarmWorkflowModule(repo, root)
		if err != nil {
			t.Fatal(err)
		}
		captured, err := LoadBundlePackRuntime(t.Context(), RuntimeConfigLoadResult{Config: base}, bundle, credentials, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(captured.Channels.Plans) != 1 || len(captured.Channels.Bindings) != 1 {
			t.Fatalf("captured projection/configured binding multiplicity: %+v", captured.Channels)
		}
		if subject, err := captured.Channels.Plans[0].CapabilitySubject(); err != nil || subject.ID != "telegram/channel.yaml" || subject.Provider != "telegram" || captured.Channels.Plans[0].ChannelIdentity().ID() != "telegram" {
			t.Fatalf("captured channel lost its exact BODY member/provider distinction: %+v error=%v", subject, err)
		}
		if !captured.Channels.Plans[0].MatchesCapturedHolder(".") {
			t.Fatal("captured channel lost its exact holding flow")
		}
		for _, foreign := range []string{"", " .", "./", "child", "telegram-agent"} {
			if captured.Channels.Plans[0].MatchesCapturedHolder(foreign) {
				t.Fatalf("captured holder normalized foreign flow %q", foreign)
			}
		}
		before, err := loaded.Bindings[0].Generation()
		if err != nil {
			t.Fatal(err)
		}
		after, err := captured.Channels.Bindings[0].Generation()
		if err != nil || !before.Equal(after) {
			t.Fatalf("captured projection replaced configured outbound binding: %v", err)
		}
		rooted, err := bundle.RootedSource()
		if err != nil {
			t.Fatal(err)
		}
		providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
		if err != nil {
			t.Fatal(err)
		}
		scope, err := providers.Scope(".")
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := scope.TriggerCatalog()
		if err != nil {
			t.Fatal(err)
		}
		plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: filepath.Base(root), Provider: "telegram", SigningSecret: "telegram.webhook_signing_secret"})
		if err != nil {
			t.Fatal(err)
		}
		body := []byte(`{"update_id":101,"callback_query":{"id":"query-101","from":{"id":41},"message":{"message_id":7,"chat":{"id":42,"type":"private"}},"data":"opaque-action-token"}}`)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		delivery, err := plan.Accept(providertriggers.Request{Target: providertriggers.Target{WebhookSecret: "secret"}, Headers: http.Header{"X-Telegram-Bot-Api-Secret-Token": {"secret"}}, Body: body, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		matches := 0
		for _, output := range delivery.Events {
			for _, projection := range captured.Channels.Plans {
				if _, matched, err := projection.ProjectActionFact(string(output.Name), output.Authorization, output.Payload); err != nil {
					t.Fatal(err)
				} else if matched {
					matches++
				}
			}
		}
		if matches != 1 {
			t.Fatalf("exact captured multi-provider authorization matches=%d, want 1", matches)
		}
	})
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
