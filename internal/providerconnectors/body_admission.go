package providerconnectors

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func AdmitBody(body packartifact.ProviderBody) (ConnectorManifest, error) {
	if !body.Valid() || body.Role() != packmodel.BodyConnector {
		return ConnectorManifest{}, fmt.Errorf("provider connector admission requires an admitted connector.yaml BODY")
	}
	root := body.SourceValue()
	fields, err := connectorFields(root, "tools", "generation")
	if err != nil {
		return ConnectorManifest{}, err
	}
	tools, err := runtimecontracts.AdmitProviderToolDeclarations(body)
	if err != nil {
		return ConnectorManifest{}, err
	}
	out := ConnectorManifest{source: root, Provider: body.ProviderKey(), Tools: tools}
	if value, present := fields["generation"]; present {
		generation, err := admitGenerationEvidence(value)
		if err != nil {
			return ConnectorManifest{}, err
		}
		for i := range generation.Operations {
			operation := &generation.Operations[i]
			qualified := body.ProviderKey() + "." + operation.ToolID
			if _, present := tools[qualified]; !present {
				return ConnectorManifest{}, connectorError(value, fmt.Sprintf("generation tool_id %q must reference a declared short tool name", operation.ToolID))
			}
			operation.ToolID = qualified
		}
		if err := generation.Validate(out.Provider, out.Tools); err != nil {
			return ConnectorManifest{}, connectorError(value, err.Error())
		}
		out.Generation = &generation
	}
	if err := out.Validate(); err != nil {
		return ConnectorManifest{}, connectorError(root, err.Error())
	}
	return out, nil
}
