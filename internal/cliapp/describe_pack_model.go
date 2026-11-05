package cliapp

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/manifesthash"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

// PackDescription is the authoring projection shared by human and machine views.
// Components retain their exact declaration scope; installed is not effective.
type PackDescription struct {
	ID         string                     `json:"id"`
	Inventory  packInventoryReadback      `json:"inventory"`
	Components []PackComponentDescription `json:"components"`
	Project    PackProjectDescription     `json:"project"`
	Fragments  []PackConsumptionFragment  `json:"fragments"`
}

type PackComponentDescription struct {
	Scope        string                  `json:"scope"`
	Directory    string                  `json:"directory"`
	Source       string                  `json:"source"`
	Version      string                  `json:"version"`
	ManifestHash string                  `json:"manifest_hash"`
	Receive      *PackReceiveDescription `json:"receive,omitempty"`
	Tools        []PackToolDescription   `json:"tools"`
}

type PackReceiveDescription struct {
	Identity       providertriggers.PackIdentity        `json:"identity"`
	SourcePath     string                               `json:"source_path"`
	Transport      packs.ChannelTransport               `json:"transport"`
	Signature      providertriggers.SignatureInspection `json:"signature"`
	RequiresSecret bool                                 `json:"requires_secret"`
	Secrets        []string                             `json:"secrets"`
	Events         []PackEventDescription               `json:"events"`
}

type PackEventDescription struct {
	packs.TriggerEventDescriptor
	AuthorSubject PackAuthorSubject `json:"author_subject"`
}

type PackAuthorSubject struct {
	Type  string `json:"type,omitempty"`
	Field string `json:"field,omitempty"`
}

type PackToolDescription struct {
	ID           string                           `json:"id"`
	Description  string                           `json:"description"`
	Identity     providertriggers.PackIdentity    `json:"identity"`
	SourcePath   string                           `json:"source_path"`
	Category     string                           `json:"category"`
	Handler      string                           `json:"handler"`
	Permission   string                           `json:"permission,omitempty"`
	Input        runtimecontracts.ToolInputSchema `json:"input"`
	Output       runtimecontracts.ToolInputSchema `json:"output"`
	Credentials  []string                         `json:"credentials"`
	Managed      string                           `json:"managed_credential,omitempty"`
	Capabilities []packs.Capability               `json:"capabilities"`
	Guarantees   []packs.Guarantee                `json:"guarantees"`
}

type PackProjectDescription struct {
	Evaluated bool                 `json:"evaluated"`
	Subjects  []packs.Subject      `json:"subjects"`
	Rows      []PackRequirementRow `json:"rows"`
}

type PackRequirementRow struct {
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	Target      string `json:"target"`
	Scope       string `json:"scope,omitempty"`
	Status      string `json:"status"`
	Remediation string `json:"remediation"`
}

type PackConsumptionFragment struct {
	Scope                string `json:"scope"`
	File                 string `json:"file"`
	Purpose              string `json:"purpose"`
	RequiresSubstitution bool   `json:"requires_substitution"`
	YAML                 string `json:"yaml"`
}

func buildPackDescription(ctx context.Context, loaded describePackContext, provider string) (PackDescription, error) {
	result := PackDescription{ID: provider, Inventory: packInventoryReadbackFromInventory(loaded.projection.Inventory),
		Components: []PackComponentDescription{}, Fragments: []PackConsumptionFragment{}}
	if entry, present := loaded.projection.Inventory.Lookup(provider); present {
		component := packDescriptionComponent(entry.Provider(), entry.Source())
		if trigger, found := loaded.projection.ProviderTriggers.EntryByProvider(provider); found {
			receive, err := packReceiveDescription(trigger, loaded.projection.ProviderTriggers)
			if err != nil {
				return result, err
			}
			component.Receive = &receive
		}
		for _, tool := range loaded.projection.ProviderConnectors.Inventory() {
			if tool.Provider == provider {
				row, err := packToolDescription(tool.ToolID, tool.Tool, tool.Pack.Identity)
				if err != nil {
					return result, err
				}
				component.Tools = append(component.Tools, row)
			}
		}
		result.Components = append(result.Components, component)
	}
	if loaded.bundle != nil {
		if err := appendCapturedPackComponents(&result, loaded.bundle); err != nil {
			return result, err
		}
	}
	if len(result.Components) == 0 {
		var candidates []string
		for _, entry := range result.Inventory.Packs {
			candidates = append(candidates, entry.ID)
		}
		return result, fmt.Errorf("provider %q is not selected; candidates: %s; searched selected pack paths: %s; run `swarm describe pack`",
			provider, strings.Join(candidates, ", "), strings.Join(packDescriptionSearchPaths(result.Inventory), ", "))
	}
	project, err := describePackProject(ctx, loaded, provider)
	if err != nil {
		return result, err
	}
	result.Project = project
	completePackDescriptionRequirements(&result)
	for _, component := range result.Components {
		fragments, err := packConsumptionFragments(provider, component)
		if err != nil {
			return result, err
		}
		result.Fragments = append(result.Fragments, fragments...)
	}
	return result, nil
}

func packDescriptionComponent(provider packartifact.AdmittedProvider, source string) PackComponentDescription {
	return PackComponentDescription{Scope: provider.DeclaringFlow(), Directory: provider.Directory(), Source: source,
		Version: provider.Metadata().Version, ManifestHash: manifesthash.FromBytes(provider.ManifestBody()).String(), Tools: []PackToolDescription{}}
}

func appendCapturedPackComponents(result *PackDescription, bundle *runtimecontracts.WorkflowContractBundle) error {
	providers, err := packadmission.ProviderSourceForBundle(bundle)
	if err != nil {
		return err
	}
	for _, flow := range providers.FlowPaths() {
		scope, err := providers.Scope(flow)
		if err != nil {
			return err
		}
		for _, definition := range scope.HeldProviders() {
			if definition.Provider().Key() != result.ID {
				continue
			}
			component := packDescriptionComponent(definition.Provider(), "captured")
			if catalog, present := definition.TriggerCatalog(); present {
				entry, _ := catalog.EntryByProvider(result.ID)
				receive, err := packReceiveDescription(entry, catalog)
				if err != nil {
					return err
				}
				component.Receive = &receive
			}
			if connector, present := definition.ConnectorDescriptor(); present {
				for _, name := range sortedPackToolNames(connector.Tools) {
					row, err := packToolDescription(name, connector.Tools[name], connector.Identity)
					if err != nil {
						return err
					}
					component.Tools = append(component.Tools, row)
				}
			}
			result.Components = append(result.Components, component)
		}
	}
	return nil
}

func packReceiveDescription(entry providertriggers.CatalogEntry, catalog *providertriggers.CatalogSnapshot) (PackReceiveDescription, error) {
	signature, err := entry.Manifest.InspectSignature()
	if err != nil {
		return PackReceiveDescription{}, err
	}
	out := PackReceiveDescription{Identity: entry.Identity, SourcePath: entry.SourcePath,
		Transport: entry.Manifest.Transport(), Signature: signature, RequiresSecret: entry.Manifest.RequiresSecret(),
		Secrets: providertriggers.DerivedRequires(entry.Manifest).Secrets, Events: []PackEventDescription{}}
	outputs := entry.Manifest.OutputManifest()
	subjects, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		return out, err
	}
	for _, subject := range subjects {
		if subject.Provider != entry.Manifest.Provider() {
			continue
		}
		for _, event := range subject.TriggerEvents {
			row := PackEventDescription{TriggerEventDescriptor: event}
			sort.Slice(row.Fields, func(i, j int) bool { return row.Fields[i].Name < row.Fields[j].Name })
			for _, output := range outputs {
				if output.Kind == providertriggers.OutputKindNormalized && output.Event == event.Event {
					row.AuthorSubject = PackAuthorSubject{Type: output.AuthorSubject.Type, Field: output.AuthorSubject.Field}
				}
			}
			out.Events = append(out.Events, row)
		}
	}
	return out, nil
}

func packToolDescription(name string, tool runtimecontracts.ToolSchemaEntry, identity packs.PackIdentity) (PackToolDescription, error) {
	out := PackToolDescription{ID: name, Description: tool.Description(), Category: tool.Category().String(), Handler: tool.Handler().String(),
		Permission: tool.Permission().String(), Input: tool.InputSchema(), Output: tool.OutputSchema(), Credentials: tool.Credentials(),
		Identity:   providertriggers.PackIdentity{ID: identity.ID(), Version: identity.Version(), ManifestHash: identity.ManifestHash(), Provenance: identity.Source().Provenance()},
		SourcePath: identity.Source().Path(), Capabilities: []packs.Capability{}, Guarantees: []packs.Guarantee{}}
	if managed, present := tool.ManagedCredentialExecution(); present {
		out.Managed = managed.Key()
	}
	subject, err := (providerconnectors.InstalledTool{ToolID: name, Tool: tool,
		Pack: providerconnectors.LoadedPack{Identity: identity, Directory: identity.Source().Path()}}).CapabilitySubject()
	if err != nil {
		return out, err
	}
	out.Capabilities, out.Guarantees = subject.Capabilities, subject.Guarantees
	return out, nil
}

func sortedPackToolNames(tools map[string]runtimecontracts.ToolSchemaEntry) []string {
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func packDescriptionSearchPaths(inventory packInventoryReadback) []string {
	paths := append([]string(nil), inventory.BaseDirectories...)
	if len(paths) == 0 {
		for _, entry := range inventory.Packs {
			paths = append(paths, entry.Directory)
		}
	}
	return paths
}

func selectPackDescriptionTool(description PackDescription, name string) (PackDescription, error) {
	found := false
	for index := range description.Components {
		selected := []PackToolDescription{}
		for _, tool := range description.Components[index].Tools {
			if tool.ID == name {
				selected, found = append(selected, tool), true
			}
		}
		description.Components[index].Tools = selected
	}
	if !found {
		return description, fmt.Errorf("tool %q is not declared by provider %q; use an exact canonical ID from `swarm describe pack %s`", name, description.ID, description.ID)
	}
	return description, nil
}

func describePackProject(ctx context.Context, loaded describePackContext, provider string) (PackProjectDescription, error) {
	result := PackProjectDescription{Subjects: []packs.Subject{}, Rows: []PackRequirementRow{}}
	if loaded.bundle == nil || !loaded.config.DeploymentConfigured() {
		return result, nil
	}
	source, _, err := admitStructuralSource(loaded.config, loaded.bundle)
	if err != nil {
		return result, err
	}
	credentials, err := BuildProviderCredentialStore()
	if err != nil {
		return result, err
	}
	owner, err := runtimecredentials.NewSnapshotOwner(credentials)
	if err != nil {
		return result, err
	}
	triggers, err := runtime.EffectiveStandingIngressCapabilitySubjects(ctx, source, loaded.projection.ProviderTriggers, owner)
	if err != nil {
		return result, err
	}
	managed, err := BuildManagedCredentialStore()
	if err != nil {
		return result, err
	}
	connectors, err := providerconnectors.CapabilitySubjects(ctx, source, providerconnectors.CapabilityOptions{
		Registry: loaded.projection.ProviderConnectors, IncludeInstalled: true, StaticCredentials: credentials, ManagedCredentials: managed})
	if err != nil {
		return result, err
	}
	result.Evaluated = true
	for _, subject := range append(triggers, connectors...) {
		if subject.Provider != provider {
			continue
		}
		result.Subjects = append(result.Subjects, subject)
		for _, requirement := range subject.Requirements {
			exit := requirement.Remediation
			if exit == "" {
				exit = "no requirement action; runtime readiness is not evaluated"
			}
			result.Rows = append(result.Rows, PackRequirementRow{Kind: requirement.Kind, Name: requirement.Name, Target: subject.ID,
				Scope: requirement.Scope, Status: requirement.Status, Remediation: exit})
		}
	}
	return result, nil
}

func completePackDescriptionRequirements(description *PackDescription) {
	hasTarget := false
	for _, subject := range description.Project.Subjects {
		hasTarget = hasTarget || subject.Kind == packs.SubjectProviderTrigger
	}
	seen := map[string]bool{}
	for _, component := range description.Components {
		if component.Receive != nil && description.Project.Evaluated && !hasTarget {
			key := "target:" + component.Scope
			if !seen[key] {
				description.Project.Rows = append(description.Project.Rows, PackRequirementRow{Kind: "target", Name: description.ID,
					Scope: component.Scope, Status: "not declared", Remediation: "add the receive block from `swarm describe pack " + description.ID + " --consume`"})
				seen[key] = true
			}
		}
		if description.Project.Evaluated {
			continue
		}
		add := func(kind, name string) {
			key := kind + ":" + name + ":" + component.Scope
			if seen[key] {
				return
			}
			seen[key] = true
			description.Project.Rows = append(description.Project.Rows, PackRequirementRow{Kind: kind, Name: name, Scope: component.Scope,
				Status: "not evaluated", Remediation: "run inside a configured project to check bindings"})
		}
		if component.Receive != nil && component.Receive.RequiresSecret {
			for _, name := range component.Receive.Secrets {
				add("secret", name)
			}
		}
		for _, tool := range component.Tools {
			for _, name := range tool.Credentials {
				add("secret", name)
			}
			if tool.Managed != "" {
				add("managed_credential", tool.Managed)
			}
		}
	}
}
