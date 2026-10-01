package providertriggers

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func unmarshalToolTestYAML(body []byte, target *runtimecontracts.ToolInputSchema) error {
	source, err := yamlsource.Load(body)
	if err != nil {
		return err
	}
	*target, err = runtimecontracts.AdmitToolInputSchemaValue(source.Document("schema-fixture.yaml").Root())
	return err
}
