package cliapp

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type ProviderTriggerPackLoad struct {
	Catalog   *providertriggers.CatalogSnapshot
	Loaded    []providertriggers.LoadedPack
	Inventory *packartifact.EffectivePackInventory
}

func LoadConfiguredPlatformPackBase(repo string, cfgResult RuntimeConfigLoadResult) (*packartifact.PlatformPackInventory, error) {
	if cfgResult.Config == nil {
		return nil, fmt.Errorf("runtime config is required")
	}
	runningVersion, err := platform.PlatformVersion()
	if err != nil {
		return nil, err
	}
	embedded, err := packartifact.LoadEmbeddedPlatformPackInventory(runningVersion)
	if err != nil {
		return nil, err
	}
	configured := cfgResult.Config.Platform.Packs.PlatformDirs
	if len(configured) == 0 {
		return embedded, nil
	}
	dirs := resolvePlatformPackDirs(repo, packConfigOrigin(cfgResult, "platform.packs.platform_dirs"), configured)
	return packartifact.LoadDevelopmentPlatformPackInventory(runningVersion, dirs, embedded)
}

func packConfigOrigin(cfgResult RuntimeConfigLoadResult, key string) string {
	if origin, ok := cfgResult.KeyOrigins[strings.TrimSpace(key)]; ok {
		return strings.TrimSpace(origin.Path)
	}
	return ""
}

func resolvePlatformPackDirs(repo, configPath string, dirs []string) []string {
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

func appendEffectiveProviderTriggerCapabilitySubjects(ctx context.Context, report *LocalPreflightReport, source semanticview.Source, catalog *providertriggers.CatalogSnapshot, providerCredentials runtimecredentials.Store) {
	if report == nil || source == nil || catalog == nil {
		return
	}
	var owner *runtimecredentials.SnapshotOwner
	if providerCredentials != nil {
		var err error
		owner, err = runtimecredentials.NewSnapshotOwner(providerCredentials)
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "provider_trigger_credential_store_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix deployment provider credential storage")
			return
		}
	}
	subjects, err := runtime.EffectiveStandingIngressCapabilitySubjects(ctx, source, catalog, owner)
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_trigger_target_admission_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the standing ingress provider admission declaration or configured trigger packs")
		return
	}
	report.addCapabilitySubjects(subjects)
}
