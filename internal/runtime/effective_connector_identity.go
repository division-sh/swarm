package runtime

import (
	"fmt"
	"sort"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func effectiveConnectorIdentityValues(source semanticview.Source) ([]map[string]any, error) {
	values := make([]map[string]any, 0)
	for _, scope := range source.FlowScopes() {
		for _, name := range sortedToolIDs(scope.Tools) {
			tool := scope.Tools[name]
			if tool.Category() != runtimecontracts.ToolCategoryProviderConnector {
				continue
			}
			value, err := effectiveConnectorIdentityValue(source.SemanticCapabilities(), scope.ID, name, tool)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}
	sort.Slice(values, func(i, j int) bool {
		left := values[i]["flow_id"].(string) + "\x00" + values[i]["tool_id"].(string)
		right := values[j]["flow_id"].(string) + "\x00" + values[j]["tool_id"].(string)
		return left < right
	})
	return values, nil
}

func effectiveConnectorIdentityValue(capabilities semanticview.Capabilities, flowID, toolID string, tool runtimecontracts.ToolSchemaEntry) (map[string]any, error) {
	toolValue, err := tool.CanonicalValue()
	if err != nil {
		return nil, fmt.Errorf("effective connector tool %q in flow %q: %w", toolID, flowID, err)
	}
	value := map[string]any{"flow_id": flowID, "tool_id": toolID, "tool": toolValue}
	importSource, imported := capabilities.ConnectorImportSource(flowID, toolID)
	if imported {
		identity, ok := capabilities.ConnectorPackProvenance(flowID, toolID)
		if !ok || !identity.Valid() {
			return nil, fmt.Errorf("effective connector tool %q in flow %q is missing valid source identity", toolID, flowID)
		}
		value["import_source"] = importSource.URI()
		value["pack"] = map[string]any{
			"id": identity.PackID, "version": identity.Version,
			"manifest_hash": identity.ManifestHash, "source": identity.Source,
		}
	}
	if generation, generated := capabilities.ConnectorGeneration(flowID, toolID); generated {
		if !imported || !generation.Valid() {
			return nil, fmt.Errorf("effective connector tool %q in flow %q has invalid or unbound generation evidence", toolID, flowID)
		}
		value["generation"] = effectiveConnectorGenerationValue(generation)
	}
	return value, nil
}

func effectiveConnectorGenerationValue(generation semanticview.ConnectorGenerationSurface) map[string]any {
	permissions := generation.Permissions()
	values := make([]map[string]any, 0, len(permissions))
	for _, permission := range permissions {
		values = append(values, map[string]any{"id": permission.ID(), "note": permission.Note()})
	}
	sort.Slice(values, func(i, j int) bool { return values[i]["id"].(string) < values[j]["id"].(string) })
	return map[string]any{
		"generator_version": generation.GeneratorVersion(), "source_path": generation.SourcePath(),
		"source_sha256": generation.SourceSHA256(), "profile_path": generation.ProfilePath(),
		"profile_sha256": generation.ProfileSHA256(), "manifest_sha256": generation.ManifestSHA256(),
		"operation_id": generation.OperationID(), "permissions": values,
		"fixture_id": generation.FixtureID(), "fixture_status": generation.FixtureStatus(),
		"review_status": generation.ReviewStatus(),
	}
}
