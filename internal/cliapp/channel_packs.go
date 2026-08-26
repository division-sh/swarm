package cliapp

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/yamlsource"
)

type ChannelPackLoad struct {
	Loaded   []packs.LoadedChannelPack
	Plans    []packs.SatisfactionPlan
	Bindings []packs.OutboundBindingPlan
}

func LoadConfiguredChannelPacks(ctx context.Context, cfgResult RuntimeConfigLoadResult, projection packadmission.Projection, staticCredentials runtimecredentials.Store, managedCredentials runtimemanagedcredentials.Store) (ChannelPackLoad, error) {
	if cfgResult.Config == nil {
		return ChannelPackLoad{}, fmt.Errorf("runtime config is required")
	}
	if projection.EffectivePackInventoryDigest() == "" || projection.ProviderTriggers == nil || projection.ProviderConnectors == nil {
		return ChannelPackLoad{}, fmt.Errorf("admitted pack projection is required for channel satisfaction")
	}
	bindings, err := compileChannelBindings(ctx, cfgResult.Config, projection.ChannelPlans, staticCredentials, managedCredentials)
	if err != nil {
		return ChannelPackLoad{}, err
	}
	return ChannelPackLoad{
		Loaded: projection.LoadedChannelPacks, Plans: projection.ChannelPlans, Bindings: bindings,
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
		byID[plan.ChannelIdentity().ID()] = plan
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
		credentialKeys := normalizeChannelCredentialMap(declared.Credentials)
		if err := validateChannelBindingRegistration(id, declared, plan, credentialKeys); err != nil {
			return nil, err
		}
		credentialStore := mappedChannelCredentialStore{Store: staticCredentials, keys: credentialKeys}
		requirementsByKey := map[string]packs.Requirement{}
		requirementOwner := map[string]string{}
		for _, operationName := range plan.OperationNames() {
			connectorToolID, tool, err := plan.ConnectorOperation(operationName)
			if err != nil {
				return nil, fmt.Errorf("channels.bindings.%s connector operation: %w", id, err)
			}
			resolved, err := providerconnectors.RequirementsForTool(ctx, connectorToolID, tool, providerconnectors.CapabilityOptions{
				StaticCredentials: credentialStore, ManagedCredentials: managedCredentials,
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
		binding, err := packs.NewOutboundBindingPlanWithRegistration(id, plan, declared.Destination, requirements, credentialKeys, declared.Register)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func normalizeChannelCredentialMap(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for logical, rawKey := range input {
		out[strings.TrimSpace(logical)] = strings.TrimSpace(rawKey)
	}
	return out
}

func validateChannelBindingRegistration(id string, declared config.ChannelBindingConfig, plan packs.SatisfactionPlan, credentialKeys map[string]string) error {
	expected := map[string]struct{}{}
	for _, operationName := range plan.OperationNames() {
		_, tool, err := plan.ConnectorOperation(operationName)
		if err != nil {
			return err
		}
		for _, logical := range tool.Credentials() {
			expected[logical] = struct{}{}
		}
	}
	register := strings.TrimSpace(declared.Register)
	registration, hasRegistration := plan.Registration()
	if register != "" {
		if !hasRegistration {
			return fmt.Errorf("channels.bindings.%s.register requires channel pack %q to declare registration", id, plan.ChannelIdentity().ID())
		}
		target, err := packs.ParseChannelRegistrationTarget(register)
		if err != nil {
			return fmt.Errorf("channels.bindings.%s.register: %w", id, err)
		}
		if target.Provider != registration.Provider() {
			return fmt.Errorf("channels.bindings.%s.register provider %q conflicts with channel registration provider %q", id, target.Provider, registration.Provider())
		}
		for _, logical := range registration.ProviderCredentials() {
			expected[logical] = struct{}{}
		}
		if len(credentialKeys) == 0 {
			return fmt.Errorf("channels.bindings.%s.register requires an explicit complete credentials map", id)
		}
	}
	for logical := range credentialKeys {
		if _, ok := expected[logical]; !ok {
			return fmt.Errorf("channels.bindings.%s.credentials.%s is not required by the selected channel plan", id, logical)
		}
	}
	if register != "" {
		missing := make([]string, 0)
		for logical := range expected {
			if strings.TrimSpace(credentialKeys[logical]) == "" {
				missing = append(missing, logical)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			return fmt.Errorf("channels.bindings.%s.credentials is missing required mappings: %s", id, strings.Join(missing, ", "))
		}
	}
	return nil
}

type mappedChannelCredentialStore struct {
	runtimecredentials.Store
	keys map[string]string
}

func (s mappedChannelCredentialStore) mapped(key string) string {
	key = strings.TrimSpace(key)
	if mapped := s.keys[key]; mapped != "" {
		return mapped
	}
	return key
}

func (s mappedChannelCredentialStore) Get(ctx context.Context, key string) (string, bool, error) {
	if s.Store == nil {
		return "", false, nil
	}
	return s.Store.Get(ctx, s.mapped(key))
}

func (s mappedChannelCredentialStore) Inspect(ctx context.Context, key string) (runtimecredentials.Metadata, error) {
	mapped := s.mapped(key)
	if inspector, ok := s.Store.(runtimecredentials.Inspector); ok {
		metadata, err := inspector.Inspect(ctx, mapped)
		metadata.Key = strings.TrimSpace(key)
		return metadata, err
	}
	_, present, err := s.Get(ctx, key)
	return runtimecredentials.Metadata{Key: strings.TrimSpace(key), Present: present}, err
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
	publication, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication(load.Bindings)
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "channel_outbound_surface_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the outbound channel binding or connector credentials")
		return
	}
	for _, binding := range publication.Bindings() {
		subject, err := binding.CapabilitySubject()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "channel_outbound_surface_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the outbound channel binding or connector credentials")
			return
		}
		subjects = append(subjects, subject)
	}
	report.addCapabilitySubjects(subjects)
}
