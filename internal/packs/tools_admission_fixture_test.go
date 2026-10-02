package packs_test

import (
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func unmarshalToolTestYAML(body []byte, target any) error {
	switch out := target.(type) {
	case *runtimecontracts.PlatformSpecDocument:
		value, err := runtimecontracts.ParsePlatformSpecDocument(body, "platform-spec.yaml")
		*out = value
		return err
	case *packs.ChannelManifest:
		value, err := packs.ParseChannelManifest(body)
		*out = value
		return err
	case *providerconnectors.ConnectorManifest:
		value, err := providerconnectors.ParseConnectorManifest(body)
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
