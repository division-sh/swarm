package providerconnectors

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func unmarshalToolTestYAML(body []byte, target any) error {
	switch out := target.(type) {
	case *ConnectorManifest:
		value, err := ParseConnectorManifest(body)
		*out = value
		return err
	case *runtimecontracts.ToolInputSchema:
		source, err := yamlsource.Load(body)
		if err != nil {
			return err
		}
		*out, err = runtimecontracts.AdmitToolInputSchemaValue(source.Document("schema-fixture.yaml").Root())
		return err
	default:
		return yaml.Unmarshal(body, target)
	}
}
