package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
)

type channelPackLoad struct {
	Loaded       []packs.LoadedChannelPack
	Plans        []packs.SatisfactionPlan
	Bindings     []packs.OutboundBindingPlan
	PlatformDirs []string
	ExternalDirs []string
	PlatformSpec runtimecontracts.PlatformSpecDocument
}

func loadConfiguredChannelPacks(ctx context.Context, repo string, cfgResult runtimeConfigLoadResult, platformSpec runtimecontracts.PlatformSpecDocument, triggerCatalog *providertriggers.CatalogSnapshot, staticCredentials runtimecredentials.Store, managedCredentials runtimemanagedcredentials.Store) (channelPackLoad, error) {
	if cfgResult.Config == nil {
		return channelPackLoad{}, fmt.Errorf("runtime config is required")
	}
	if triggerCatalog == nil {
		return channelPackLoad{}, fmt.Errorf("provider trigger catalog is required for channel satisfaction")
	}
	runningVersion, err := platform.PlatformVersion()
	if err != nil {
		return channelPackLoad{}, err
	}
	platformDirs := resolveProviderTriggerPackDirs(repo, providerTriggerPackConfigOrigin(cfgResult, "channels.packs.platform_dirs"), cfgResult.Config.Channels.Packs.PlatformDirs)
	externalDirs := resolveProviderTriggerPackDirs(repo, providerTriggerPackConfigOrigin(cfgResult, "channels.packs.external_dirs"), cfgResult.Config.Channels.Packs.ExternalDirs)
	platformPacks, err := packs.LoadChannelPackDirs(runningVersion, "platform", platformDirs...)
	if err != nil {
		return channelPackLoad{}, err
	}
	externalPacks, err := packs.LoadChannelPackDirs(runningVersion, "external", externalDirs...)
	if err != nil {
		return channelPackLoad{}, err
	}
	loaded := append(append([]packs.LoadedChannelPack(nil), platformPacks...), externalPacks...)
	registry, err := packs.NewInterfaceRegistry(platformSpec)
	if err != nil {
		return channelPackLoad{}, err
	}
	plans, err := packs.CompileChannelInventory(registry, loaded, triggerCatalog.PackDescriptors(), providerconnectors.DefaultPackRegistry().PackDescriptors())
	if err != nil {
		return channelPackLoad{}, err
	}
	bindings, err := compileChannelBindings(ctx, cfgResult.Config, plans, staticCredentials, managedCredentials)
	if err != nil {
		return channelPackLoad{}, err
	}
	return channelPackLoad{
		Loaded: loaded, Plans: plans, Bindings: bindings,
		PlatformDirs: platformDirs, ExternalDirs: externalDirs, PlatformSpec: platformSpec,
	}, nil
}

func compileChannelBindings(ctx context.Context, cfg *config.Config, plans []packs.SatisfactionPlan, staticCredentials runtimecredentials.Store, managedCredentials runtimemanagedcredentials.Store) ([]packs.OutboundBindingPlan, error) {
	byID := make(map[string]packs.SatisfactionPlan, len(plans))
	for _, plan := range plans {
		byID[plan.Channel.ID] = plan
	}
	ids := make([]string, 0, len(cfg.Channels.Bindings))
	for id := range cfg.Channels.Bindings {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	bindings := make([]packs.OutboundBindingPlan, 0, len(ids))
	for _, id := range ids {
		declared := cfg.Channels.Bindings[id]
		plan, ok := byID[strings.TrimSpace(declared.Pack)]
		if !ok {
			return nil, fmt.Errorf("channels.bindings.%s references unavailable channel pack %q", id, declared.Pack)
		}
		requirements := []packs.Requirement{}
		seen := map[string]struct{}{}
		for _, operation := range plan.Operations {
			resolved, err := providerconnectors.RequirementsForTool(ctx, operation.Tool, operation.ToolSchema, providerconnectors.CapabilityOptions{
				StaticCredentials: staticCredentials, ManagedCredentials: managedCredentials,
			})
			if err != nil {
				return nil, fmt.Errorf("channels.bindings.%s connector requirements: %w", id, err)
			}
			for _, requirement := range resolved {
				key := requirement.Kind + "\x00" + requirement.Name
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				requirements = append(requirements, requirement)
			}
		}
		binding, err := packs.NewOutboundBindingPlan(id, plan, declared.Destination, requirements)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func appendChannelCapabilitySubjects(report *localPreflightReport, load channelPackLoad) {
	if report == nil {
		return
	}
	subjects := make([]packs.Subject, 0, len(load.Plans)+len(load.Bindings))
	for _, plan := range load.Plans {
		subject, err := plan.CapabilitySubject()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "channel_pack_surface_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix the selected channel pack")
			return
		}
		subjects = append(subjects, subject)
	}
	for _, binding := range load.Bindings {
		subject, err := binding.CapabilitySubject()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "channel_outbound_surface_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix the outbound channel binding or connector credentials")
			return
		}
		subjects = append(subjects, subject)
	}
	report.addCapabilitySubjects(subjects)
}
