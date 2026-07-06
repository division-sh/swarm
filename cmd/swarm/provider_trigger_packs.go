package main

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providertriggers"
)

type providerTriggerPackLoad struct {
	Registry     *providertriggers.Registry
	Loaded       []providertriggers.LoadedPack
	ExternalDirs []string
}

func loadConfiguredProviderTriggerPacks(repo string, cfgResult runtimeConfigLoadResult) (providerTriggerPackLoad, error) {
	if cfgResult.Config == nil {
		return providerTriggerPackLoad{}, fmt.Errorf("runtime config is required")
	}
	runningVersion, err := platform.PlatformVersion()
	if err != nil {
		return providerTriggerPackLoad{}, err
	}
	dirs := resolveProviderTriggerPackDirs(repo, cfgResult.Path, cfgResult.Config.ProviderTriggers.Packs.ExternalDirs)
	registry, loaded, err := providertriggers.NewRegistryWithExternalPackDirs(runningVersion, dirs...)
	if err != nil {
		return providerTriggerPackLoad{}, err
	}
	return providerTriggerPackLoad{
		Registry:     registry,
		Loaded:       loaded,
		ExternalDirs: dirs,
	}, nil
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

func appendProviderTriggerPackSurfaceFindings(report *localPreflightReport, loaded []providertriggers.LoadedPack) {
	if report == nil || len(loaded) == 0 {
		return
	}
	sort.SliceStable(loaded, func(i, j int) bool {
		return strings.TrimSpace(loaded[i].Manifest.Provider) < strings.TrimSpace(loaded[j].Manifest.Provider)
	})
	for _, pack := range loaded {
		provider := providertriggers.NormalizeProviderName(pack.Manifest.Provider)
		if provider == "" {
			provider = strings.TrimSpace(pack.Envelope.ID)
		}
		report.add(localPreflightProviderPackPrerequisite, "provider_trigger_pack_"+provider, localPreflightSeverityInfo, localPreflightStatusOK, providerTriggerPackSurfaceMessage(pack), "")
	}
}

func providerTriggerPackSurfaceMessage(pack providertriggers.LoadedPack) string {
	surface := pack.CapabilitySurface(nil)
	return fmt.Sprintf("provider trigger pack %s source=%s CAN %s CANNOT %s requires %s",
		strings.TrimSpace(pack.Envelope.ID),
		strings.TrimSpace(pack.Source),
		strings.Join(surface.Can, "; "),
		strings.Join(surface.Cannot, "; "),
		formatProviderTriggerPackRequirements(surface.Requires),
	)
}

func formatProviderTriggerPackRequirements(requirements []packs.RequirementStatus) string {
	if len(requirements) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(requirements))
	for _, requirement := range requirements {
		status := "UNBOUND"
		if requirement.Bound {
			status = "BOUND"
		}
		parts = append(parts, strings.TrimSpace(requirement.Name)+"="+status)
	}
	return strings.Join(parts, "; ")
}
