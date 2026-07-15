package cliapp

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type ProviderTriggerPackLoad struct {
	Catalog      *providertriggers.CatalogSnapshot
	Loaded       []providertriggers.LoadedPack
	PlatformDirs []string
	ExternalDirs []string
}

func (l ProviderTriggerPackLoad) Reload() (*providertriggers.CatalogSnapshot, error) {
	runningVersion, err := platform.PlatformVersion()
	if err != nil {
		return nil, err
	}
	catalog, _, err := providertriggers.NewCatalogSnapshotFromPackDirs(runningVersion, l.PlatformDirs, l.ExternalDirs)
	return catalog, err
}

func LoadConfiguredProviderTriggerPacks(repo string, cfgResult RuntimeConfigLoadResult) (ProviderTriggerPackLoad, error) {
	if cfgResult.Config == nil {
		return ProviderTriggerPackLoad{}, fmt.Errorf("runtime config is required")
	}
	configuredPlatformDirs := cfgResult.Config.ProviderTriggers.Packs.PlatformDirs
	runningVersion, err := platform.PlatformVersion()
	if err != nil {
		return ProviderTriggerPackLoad{}, err
	}
	platformDirs := resolveProviderTriggerPackDirs(repo, providerTriggerPackConfigOrigin(cfgResult, "provider_triggers.packs.platform_dirs"), configuredPlatformDirs)
	externalDirs := resolveProviderTriggerPackDirs(repo, providerTriggerPackConfigOrigin(cfgResult, "provider_triggers.packs.external_dirs"), cfgResult.Config.ProviderTriggers.Packs.ExternalDirs)
	catalog, loaded, err := providertriggers.NewCatalogSnapshotFromPackDirs(runningVersion, platformDirs, externalDirs)
	if err != nil {
		return ProviderTriggerPackLoad{}, err
	}
	return ProviderTriggerPackLoad{
		Catalog:      catalog,
		Loaded:       loaded,
		PlatformDirs: platformDirs,
		ExternalDirs: externalDirs,
	}, nil
}

func providerTriggerPackConfigOrigin(cfgResult RuntimeConfigLoadResult, key string) string {
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

func appendProviderTriggerCapabilitySubjects(report *LocalPreflightReport, loaded []providertriggers.LoadedPack) {
	if report == nil || len(loaded) == 0 {
		return
	}
	subjects := make([]packs.Subject, 0, len(loaded))
	for _, pack := range loaded {
		subject, err := pack.CapabilitySubject()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "provider_trigger_surface_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix provider trigger pack capability declarations")
			return
		}
		subjects = append(subjects, subject)
	}
	report.addCapabilitySubjects(subjects)
}

func appendEffectiveProviderTriggerCapabilitySubjects(report *LocalPreflightReport, source semanticview.Source, catalog *providertriggers.CatalogSnapshot) {
	if report == nil || source == nil || catalog == nil {
		return
	}
	subjects, err := runtime.EffectiveStandingIngressCapabilitySubjects(source, catalog)
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_trigger_target_admission_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the standing ingress provider admission declaration or configured trigger packs")
		return
	}
	report.addCapabilitySubjects(subjects)
}
