package cliapp

import (
	"context"
	"fmt"
	"reflect"
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
	"github.com/division-sh/swarm/internal/yamlsource"
)

type ChannelPackLoad struct {
	Loaded       []packs.LoadedChannelPack
	Plans        []packs.SatisfactionPlan
	Bindings     []packs.OutboundBindingPlan
	PlatformDirs []string
	ExternalDirs []string
	PlatformSpec runtimecontracts.PlatformSpecDocument
}

func LoadConfiguredChannelPacks(ctx context.Context, repo string, cfgResult RuntimeConfigLoadResult, platformSpec runtimecontracts.PlatformSpecDocument, triggerCatalog *providertriggers.CatalogSnapshot, staticCredentials runtimecredentials.Store, managedCredentials runtimemanagedcredentials.Store) (ChannelPackLoad, error) {
	if cfgResult.Config == nil {
		return ChannelPackLoad{}, fmt.Errorf("runtime config is required")
	}
	if triggerCatalog == nil {
		return ChannelPackLoad{}, fmt.Errorf("provider trigger catalog is required for channel satisfaction")
	}
	runningVersion, err := platform.PlatformVersion()
	if err != nil {
		return ChannelPackLoad{}, err
	}
	platformDirs := resolveProviderTriggerPackDirs(repo, providerTriggerPackConfigOrigin(cfgResult, "channels.packs.platform_dirs"), cfgResult.Config.Channels.Packs.PlatformDirs)
	externalDirs := resolveProviderTriggerPackDirs(repo, providerTriggerPackConfigOrigin(cfgResult, "channels.packs.external_dirs"), cfgResult.Config.Channels.Packs.ExternalDirs)
	platformPacks, err := packs.LoadChannelPackDirs(runningVersion, "platform", platformDirs...)
	if err != nil {
		return ChannelPackLoad{}, err
	}
	externalPacks, err := packs.LoadChannelPackDirs(runningVersion, "external", externalDirs...)
	if err != nil {
		return ChannelPackLoad{}, err
	}
	loaded := append(append([]packs.LoadedChannelPack(nil), platformPacks...), externalPacks...)
	registry, err := packs.NewInterfaceRegistry(platformSpec)
	if err != nil {
		return ChannelPackLoad{}, err
	}
	plans, err := packs.CompileChannelInventory(registry, loaded, triggerCatalog.PackDescriptors(), providerconnectors.DefaultPackRegistry().PackDescriptors())
	if err != nil {
		return ChannelPackLoad{}, err
	}
	bindings, err := compileChannelBindings(ctx, cfgResult.Config, plans, staticCredentials, managedCredentials)
	if err != nil {
		return ChannelPackLoad{}, err
	}
	return ChannelPackLoad{
		Loaded: loaded, Plans: plans, Bindings: bindings,
		PlatformDirs: platformDirs, ExternalDirs: externalDirs, PlatformSpec: platformSpec,
	}, nil
}

func loadChannelPlatformSpecDocument(platformSpecPath string) (runtimecontracts.PlatformSpecDocument, error) {
	platformSpecPath = strings.TrimSpace(platformSpecPath)
	if platformSpecPath == "" {
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("platform spec path is required")
	}
	source, err := yamlsource.LoadFile(platformSpecPath)
	if err != nil {
		if cause, ok := yamlsource.ParseCause(err); ok {
			return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("unmarshal platform spec: %w", cause)
		}
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("read platform spec: %w", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := source.Decode(&spec); err != nil {
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("unmarshal platform spec: %w", err)
	}
	return spec, nil
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
		requirementsByKey := map[string]packs.Requirement{}
		requirementOwner := map[string]string{}
		for _, operationName := range sortedChannelOperationNames(plan.Operations) {
			operation := plan.Operations[operationName]
			resolved, err := providerconnectors.RequirementsForTool(ctx, operation.Tool, operation.ToolSchema, providerconnectors.CapabilityOptions{
				StaticCredentials: staticCredentials, ManagedCredentials: managedCredentials,
			})
			if err != nil {
				return nil, fmt.Errorf("channels.bindings.%s connector requirements: %w", id, err)
			}
			for _, requirement := range resolved {
				key := requirement.Kind + "\x00" + requirement.Name
				if existing, exists := requirementsByKey[key]; exists {
					if !reflect.DeepEqual(existing, requirement) {
						return nil, fmt.Errorf("channels.bindings.%s operations %q and %q require incompatible %s %q descriptors", id, requirementOwner[key], operationName, requirement.Kind, requirement.Name)
					}
					continue
				}
				requirementsByKey[key] = requirement
				requirementOwner[key] = operationName
			}
		}
		requirementKeys := make([]string, 0, len(requirementsByKey))
		for key := range requirementsByKey {
			requirementKeys = append(requirementKeys, key)
		}
		sort.Strings(requirementKeys)
		requirements := make([]packs.Requirement, 0, len(requirementKeys))
		for _, key := range requirementKeys {
			requirements = append(requirements, requirementsByKey[key])
		}
		binding, err := packs.NewOutboundBindingPlan(id, plan, declared.Destination, requirements)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func sortedChannelOperationNames(operations map[string]packs.CompiledChannelOperation) []string {
	names := make([]string, 0, len(operations))
	for name := range operations {
		names = append(names, strings.TrimSpace(name))
	}
	sort.Strings(names)
	return names
}

func appendChannelCapabilitySubjects(report *LocalPreflightReport, load ChannelPackLoad) {
	if report == nil {
		return
	}
	subjects := make([]packs.Subject, 0, len(load.Plans)+len(load.Bindings))
	for _, plan := range load.Plans {
		subject, err := plan.CapabilitySubject()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "channel_pack_surface_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the selected channel pack")
			return
		}
		subjects = append(subjects, subject)
	}
	for _, binding := range load.Bindings {
		subject, err := binding.CapabilitySubject()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "channel_outbound_surface_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the outbound channel binding or connector credentials")
			return
		}
		subjects = append(subjects, subject)
	}
	report.addCapabilitySubjects(subjects)
}
