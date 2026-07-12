package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type providerTriggerPackLoad struct {
	Catalog      *providertriggers.CatalogSnapshot
	Loaded       []providertriggers.LoadedPack
	PlatformDirs []string
	ExternalDirs []string
}

func (l providerTriggerPackLoad) Reload() (*providertriggers.CatalogSnapshot, error) {
	runningVersion, err := platform.PlatformVersion()
	if err != nil {
		return nil, err
	}
	catalog, _, err := providertriggers.NewCatalogSnapshotFromRequiredPlatformPackDirs(runningVersion, l.PlatformDirs, l.ExternalDirs)
	return catalog, err
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
	catalog, loaded, err := providertriggers.NewCatalogSnapshotFromRequiredPlatformPackDirs(runningVersion, platformDirs, externalDirs)
	if err != nil {
		return providerTriggerPackLoad{}, err
	}
	return providerTriggerPackLoad{
		Catalog:      catalog,
		Loaded:       loaded,
		PlatformDirs: platformDirs,
		ExternalDirs: externalDirs,
	}, nil
}

// loadVerifyProviderTriggerPacks keeps offline verification self-contained by
// using the fixed release inventory when no elevated platform override exists.
// Both sources pass through the identical required-inventory verifier.
func loadVerifyProviderTriggerPacks(repo string, cfgResult runtimeConfigLoadResult, requireCatalog bool) (providerTriggerPackLoad, error) {
	if cfgResult.Config == nil {
		return providerTriggerPackLoad{}, fmt.Errorf("runtime config is required")
	}
	if len(cfgResult.Config.ProviderTriggers.Packs.PlatformDirs) > 0 {
		return loadConfiguredProviderTriggerPacks(repo, cfgResult)
	}
	runningVersion, err := platform.PlatformVersion()
	if err != nil {
		return providerTriggerPackLoad{}, err
	}
	platformDirs := make([]string, 0, len(providertriggers.RequiredPlatformPackIdentities()))
	present := 0
	for _, identity := range providertriggers.RequiredPlatformPackIdentities() {
		dir := filepath.Join(repo, "packs", "provider-triggers", identity.Provider)
		platformDirs = append(platformDirs, dir)
		if _, err := os.Stat(filepath.Join(dir, packs.EnvelopeFileName)); err == nil {
			present++
		} else if !os.IsNotExist(err) {
			return providerTriggerPackLoad{}, fmt.Errorf("inspect default provider trigger pack %q: %w", dir, err)
		}
	}
	externalDirs := resolveProviderTriggerPackDirs(repo, providerTriggerPackConfigOrigin(cfgResult, "provider_triggers.packs.external_dirs"), cfgResult.Config.ProviderTriggers.Packs.ExternalDirs)
	if present == 0 && len(externalDirs) == 0 && !requireCatalog {
		catalog, err := providertriggers.NewCatalogSnapshot()
		if err != nil {
			return providerTriggerPackLoad{}, err
		}
		return providerTriggerPackLoad{Catalog: catalog}, nil
	}
	catalog, loaded, err := providertriggers.NewCatalogSnapshotFromRequiredPlatformPackDirs(runningVersion, platformDirs, externalDirs)
	if err != nil {
		return providerTriggerPackLoad{}, err
	}
	return providerTriggerPackLoad{
		Catalog: catalog, Loaded: loaded, PlatformDirs: platformDirs, ExternalDirs: externalDirs,
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

func appendEffectiveProviderTriggerCapabilitySubjects(report *localPreflightReport, source semanticview.Source, catalog *providertriggers.CatalogSnapshot) {
	if report == nil || source == nil || catalog == nil {
		return
	}
	subjects, err := runtime.EffectiveStandingIngressCapabilitySubjects(source, catalog)
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_trigger_target_admission_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix the standing ingress provider admission declaration or configured trigger packs")
		return
	}
	report.addCapabilitySubjects(subjects)
}
