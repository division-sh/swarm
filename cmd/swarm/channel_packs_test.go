package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packs"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestConfiguredChannelPackDrivesAvailableAndOutboundReadinessSurfaces(t *testing.T) {
	repo := repoRoot()
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
	cfgResult := runtimeConfigLoadResult{Config: cfg, KeyOrigins: map[string]unifiedConfigKeyOrigin{}}
	triggers, err := loadConfiguredProviderTriggerPacks(repo, cfgResult)
	if err != nil {
		t.Fatalf("loadConfiguredProviderTriggerPacks: %v", err)
	}
	spec, err := loadServePlatformSpecDocument(filepath.Join(repo, defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("loadServePlatformSpecDocument: %v", err)
	}

	withoutCredential, err := loadConfiguredChannelPacks(context.Background(), repo, cfgResult, spec, triggers.Catalog, nil, nil)
	if err != nil {
		t.Fatalf("loadConfiguredChannelPacks without credential: %v", err)
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
	ready, err := loadConfiguredChannelPacks(context.Background(), repo, cfgResult, spec, triggers.Catalog, credentials, nil)
	if err != nil {
		t.Fatalf("loadConfiguredChannelPacks with credential: %v", err)
	}
	outbound, err = ready.Bindings[0].CapabilitySubject()
	if err != nil || outbound.Status != packs.StatusReady {
		t.Fatalf("outbound subject with credential = %#v, err=%v", outbound, err)
	}

	report := localPreflightReport{}
	appendChannelCapabilitySubjects(&report, ready)
	if len(report.CapabilitySubjects) != 2 {
		t.Fatalf("preflight channel subjects = %#v, want structural and outbound", report.CapabilitySubjects)
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
