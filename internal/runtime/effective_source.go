package runtime

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const admittedEffectiveSourceProjectionVersion = "admitted-effective-source-projection/v1"

type EffectiveSourceProjectionRequest struct {
	Source                 semanticview.Source
	WorkflowModule         runtimepipeline.WorkflowModule
	SourceArtifactFact     runtimecorrelation.SourceArtifactFact
	ProviderTriggerCatalog *providertriggers.CatalogSnapshot
	ChannelPlans           []packs.SatisfactionPlan
}

// AdmittedEffectiveSourceProjection is the single owner of the composed
// executable source and its behavior-bearing identity.
type AdmittedEffectiveSourceProjection struct {
	source   semanticview.Source
	module   runtimepipeline.WorkflowModule
	identity scenarioexecution.EffectiveSourceIdentity
}

func (p AdmittedEffectiveSourceProjection) Source() semanticview.Source { return p.source }
func (p AdmittedEffectiveSourceProjection) WorkflowModule() runtimepipeline.WorkflowModule {
	return p.module
}
func (p AdmittedEffectiveSourceProjection) Identity() scenarioexecution.EffectiveSourceIdentity {
	return p.identity
}

func AdmitEffectiveSourceProjection(request EffectiveSourceProjectionRequest) (AdmittedEffectiveSourceProjection, error) {
	if err := request.SourceArtifactFact.Validate(); err != nil {
		return AdmittedEffectiveSourceProjection{}, fmt.Errorf("effective source bundle fact: %w", err)
	}
	source := request.Source
	if request.WorkflowModule != nil {
		if source != nil && source != request.WorkflowModule.SemanticSource() {
			return AdmittedEffectiveSourceProjection{}, fmt.Errorf("effective source request must not provide competing source and workflow module owners")
		}
		source = request.WorkflowModule.SemanticSource()
	}
	if source == nil {
		return AdmittedEffectiveSourceProjection{}, fmt.Errorf("effective semantic source is required")
	}

	connectorRegistry, triggerCatalog, err := packProjectionsForEffectiveSource(source, request.ProviderTriggerCatalog)
	if err != nil {
		return AdmittedEffectiveSourceProjection{}, err
	}
	source, err = providerconnectors.SourceWithConnectorPackImports(source, connectorRegistry)
	if err != nil {
		return AdmittedEffectiveSourceProjection{}, fmt.Errorf("provider connector pack import failed: %w", err)
	}
	source, err = SourceWithProviderTriggerEvents(source, triggerCatalog)
	if err != nil {
		return AdmittedEffectiveSourceProjection{}, fmt.Errorf("provider trigger event import failed: %w", err)
	}
	identityValue, err := effectiveSourceIdentityValue(source, request.SourceArtifactFact, request.ChannelPlans)
	if err != nil {
		return AdmittedEffectiveSourceProjection{}, err
	}
	digest, err := canonicaljson.Hash(identityValue)
	if err != nil {
		return AdmittedEffectiveSourceProjection{}, fmt.Errorf("hash effective source projection: %w", err)
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(request.SourceArtifactFact, digest)
	if err != nil {
		return AdmittedEffectiveSourceProjection{}, err
	}

	module := request.WorkflowModule
	if module != nil {
		module = connectorPackWorkflowModule{WorkflowModule: module, source: source}
	}
	return AdmittedEffectiveSourceProjection{source: source, module: module, identity: identity}, nil
}

func packProjectionsForEffectiveSource(source semanticview.Source, suppliedTriggers *providertriggers.CatalogSnapshot) (*providerconnectors.PackRegistry, *providertriggers.CatalogSnapshot, error) {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || bundle.PackInventory == nil {
		return nil, suppliedTriggers, nil
	}
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		return nil, nil, fmt.Errorf("load admitted pack projection for effective source: %w", err)
	}
	connectors := projection.ProviderConnectors
	triggers := projection.ProviderTriggers
	if suppliedTriggers != nil && !suppliedTriggers.Generation().Equal(triggers.Generation()) {
		return nil, nil, fmt.Errorf("supplied provider trigger catalog generation %s contradicts bundle effective inventory generation %s", suppliedTriggers.Generation().Diagnostic(), triggers.Generation().Diagnostic())
	}
	return connectors, triggers, nil
}

func effectiveSourceIdentityValue(source semanticview.Source, sourceFact runtimecorrelation.SourceArtifactFact, channelPlans []packs.SatisfactionPlan) (map[string]any, error) {
	bundleHash := sourceFact.BundleHash()
	inputs := semanticview.BuildAuthoredEventEndpointCensus(source).InputPins()
	inputValues := make([]map[string]any, 0, len(inputs))
	for _, endpoint := range inputs {
		resolution := semanticview.ResolveEventSchema(source, endpoint.FlowID, endpoint.Event.EventKey())
		value := map[string]any{
			"id": endpoint.ID, "flow_id": endpoint.FlowID, "pin": endpoint.PinName,
			"event": endpoint.Event.EventKey(), "schema_available": resolution.HasSchema,
		}
		if resolution.HasSchema {
			unresolvedTypes := append([]string(nil), resolution.UnresolvedTypes...)
			sort.Strings(unresolvedTypes)
			value["event"] = resolution.EventKey
			value["schema"] = runtimeeventschema.CanonicalAcceptanceSchema(resolution.Schema.Schema)
			value["unresolved_types"] = unresolvedTypes
		}
		inputValues = append(inputValues, value)
	}
	sort.Slice(inputValues, func(left, right int) bool {
		return inputValues[left]["id"].(string) < inputValues[right]["id"].(string)
	})

	capabilities := source.SemanticCapabilities()
	connectorValues := make([]map[string]any, 0)
	for _, toolID := range sortedToolIDs(source.ToolEntries()) {
		tool := source.ToolEntries()[toolID]
		if tool.Category() != runtimecontracts.ToolCategoryProviderConnector {
			continue
		}
		toolValue, err := tool.CanonicalValue()
		if err != nil {
			return nil, fmt.Errorf("effective connector tool %q: %w", toolID, err)
		}
		value := map[string]any{"tool_id": toolID, "tool": toolValue}
		importSource, imported := capabilities.ConnectorImportSource(toolID)
		if imported {
			packProvenance, ok := capabilities.ConnectorPackProvenance(toolID)
			if !ok || !packProvenance.Valid() {
				return nil, fmt.Errorf("effective connector tool %q is missing valid pack provenance", toolID)
			}
			value["import_source"] = importSource.URI()
			value["pack"] = map[string]any{
				"id": packProvenance.PackID, "version": packProvenance.Version,
				"manifest_hash": packProvenance.ManifestHash, "source": packProvenance.Source,
			}
		}
		if generation, generated := capabilities.ConnectorGeneration(toolID); generated {
			if !imported {
				return nil, fmt.Errorf("effective connector tool %q has generation provenance without an import source", toolID)
			}
			if !generation.Valid() {
				return nil, fmt.Errorf("effective connector tool %q has invalid generation provenance", toolID)
			}
			permissions := generation.Permissions()
			permissionValues := make([]map[string]any, 0, len(permissions))
			for _, permission := range permissions {
				permissionValues = append(permissionValues, map[string]any{"id": permission.ID(), "note": permission.Note()})
			}
			sort.Slice(permissionValues, func(left, right int) bool {
				return permissionValues[left]["id"].(string) < permissionValues[right]["id"].(string)
			})
			value["generation"] = map[string]any{
				"generator_version": generation.GeneratorVersion(), "source_path": generation.SourcePath(),
				"source_sha256": generation.SourceSHA256(), "profile_path": generation.ProfilePath(),
				"profile_sha256": generation.ProfileSHA256(), "manifest_sha256": generation.ManifestSHA256(),
				"operation_id": generation.OperationID(), "permissions": permissionValues,
				"fixture_id": generation.FixtureID(), "fixture_status": generation.FixtureStatus(),
				"review_status": generation.ReviewStatus(),
			}
		}
		connectorValues = append(connectorValues, value)
	}

	triggerValue := any(nil)
	if generation, _, applied := capabilities.ProviderTriggerEvents(); applied {
		if !generation.Valid() {
			return nil, fmt.Errorf("effective provider-trigger source has invalid catalog generation")
		}
		provenance := capabilities.ProviderTriggerEventProvenance()
		provenanceValues := make([]map[string]any, 0, len(provenance))
		for _, item := range provenance {
			if !item.Valid() {
				return nil, fmt.Errorf("effective provider-trigger event %q has invalid provenance", item.Event)
			}
			scopes := append([]string(nil), item.FlowScopes...)
			sort.Strings(scopes)
			provenanceValues = append(provenanceValues, map[string]any{
				"provider": item.Provider, "event": item.Event, "kind": item.Kind,
				"pack_id": item.PackID, "pack_version": item.PackVersion,
				"manifest_hash": item.ManifestHash, "source_provenance": item.SourceProvenance,
				"generation": item.Generation.Diagnostic(), "project_scopes": scopes,
			})
		}
		sort.Slice(provenanceValues, func(left, right int) bool {
			leftKey := provenanceValues[left]["provider"].(string) + "\x00" + provenanceValues[left]["event"].(string)
			rightKey := provenanceValues[right]["provider"].(string) + "\x00" + provenanceValues[right]["event"].(string)
			return leftKey < rightKey
		})
		triggerValue = map[string]any{"catalog_generation": generation.Diagnostic(), "events": provenanceValues}
	}

	channelValues := make([]map[string]any, 0, len(channelPlans))
	for _, plan := range channelPlans {
		generation, err := plan.Generation()
		if err != nil {
			return nil, fmt.Errorf("effective channel plan %q: %w", plan.ChannelIdentity().ID(), err)
		}
		channelValues = append(channelValues, map[string]any{
			"kind": "satisfaction", "id": plan.ChannelIdentity().ID(), "generation": generation.Diagnostic(),
		})
	}
	sort.Slice(channelValues, func(left, right int) bool {
		return channelValues[left]["kind"].(string)+"\x00"+channelValues[left]["id"].(string) <
			channelValues[right]["kind"].(string)+"\x00"+channelValues[right]["id"].(string)
	})

	return map[string]any{
		"version":             admittedEffectiveSourceProjectionVersion,
		"base_source":         map[string]any{"bundle_hash": bundleHash},
		"public_inputs":       inputValues,
		"provider_connectors": connectorValues,
		"provider_triggers":   triggerValue,
		"channels":            channelValues,
	}, nil
}

func sortedToolIDs(tools map[string]runtimecontracts.ToolSchemaEntry) []string {
	ids := make([]string, 0, len(tools))
	for rawID := range tools {
		id := strings.TrimSpace(rawID)
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
