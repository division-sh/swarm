package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providertriggers"
)

type providerTriggerPackLoad struct {
	Registry     *providertriggers.Registry
	Loaded       []providertriggers.LoadedPack
	PlatformDirs []string
	ExternalDirs []string
}

func loadConfiguredProviderTriggerPacks(repo string, cfgResult runtimeConfigLoadResult) (providerTriggerPackLoad, error) {
	if cfgResult.Config == nil {
		return providerTriggerPackLoad{}, fmt.Errorf("runtime config is required")
	}
	configuredPlatformDirs := cfgResult.Config.ProviderTriggers.Packs.PlatformDirs
	if len(configuredPlatformDirs) == 0 {
		return providerTriggerPackLoad{}, fmt.Errorf("provider_triggers.packs.platform_dirs is required and must declare the complete first-party platform pack inventory (%s); add this key to elevated operator configuration", requiredProviderTriggerPlatformPackIDs())
	}
	runningVersion, err := platform.PlatformVersion()
	if err != nil {
		return providerTriggerPackLoad{}, err
	}
	platformDirs := resolveProviderTriggerPackDirs(repo, providerTriggerPackConfigOrigin(cfgResult, "provider_triggers.packs.platform_dirs"), configuredPlatformDirs)
	externalDirs := resolveProviderTriggerPackDirs(repo, providerTriggerPackConfigOrigin(cfgResult, "provider_triggers.packs.external_dirs"), cfgResult.Config.ProviderTriggers.Packs.ExternalDirs)
	registry, loaded, err := providertriggers.NewRegistryFromRequiredPlatformPackDirs(runningVersion, platformDirs, externalDirs)
	if err != nil {
		return providerTriggerPackLoad{}, err
	}
	return providerTriggerPackLoad{
		Registry:     registry,
		Loaded:       loaded,
		PlatformDirs: platformDirs,
		ExternalDirs: externalDirs,
	}, nil
}

func requiredProviderTriggerPlatformPackIDs() string {
	identities := providertriggers.RequiredPlatformPackIdentities()
	ids := make([]string, 0, len(identities))
	for _, identity := range identities {
		ids = append(ids, strings.TrimSpace(identity.ID))
	}
	return strings.Join(ids, ", ")
}

func providerTriggerPackConfigOrigin(cfgResult runtimeConfigLoadResult, key string) string {
	if origin, ok := cfgResult.KeyOrigins[strings.TrimSpace(key)]; ok {
		return strings.TrimSpace(origin.Path)
	}
	return ""
}

func resolveProviderTriggerPackDirs(repo, configPath string, dirs []string) []string {
	if len(dirs) == 0 {
		return nil
	}
	base := strings.TrimSpace(repo)
	if configPath = strings.TrimSpace(configPath); configPath != "" {
		base = filepath.Dir(configPath)
	}
	out := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" || filepath.IsAbs(dir) {
			out = append(out, dir)
			continue
		}
		out = append(out, filepath.Join(base, dir))
	}
	return out
}

func appendProviderTriggerCapabilitySubjects(report *localPreflightReport, loaded []providertriggers.LoadedPack) {
	if report == nil || len(loaded) == 0 {
		return
	}
	subjects := make([]packs.Subject, 0, len(loaded))
	for _, pack := range loaded {
		subject, err := pack.CapabilitySubject()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "provider_trigger_surface_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix provider trigger pack capability declarations")
			return
		}
		subjects = append(subjects, subject)
	}
	report.addCapabilitySubjects(subjects)
}
