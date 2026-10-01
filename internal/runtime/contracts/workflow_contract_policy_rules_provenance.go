package contracts

import (
	"fmt"
	"strconv"
	"strings"
)

func populatePolicyRulesProvenance(bundle *WorkflowContractBundle, builder *effectiveProvenanceBuilder) error {
	if bundle.SourceArtifact == nil {
		return nil
	}
	for _, view := range bundle.FlowViews() {
		for _, declaration := range []struct{ file, family string }{{view.Paths.PolicyFile, "policy"}, {view.Paths.RulesFile, "rules"}} {
			if declaration.file == "" {
				continue
			}
			document, ok := bundle.SourceArtifact.YAML(declaration.file)
			if !ok {
				return fmt.Errorf("missing admitted source for %s", declaration.file)
			}
			root := document.Root()
			if err := root.ValidateExpansion(); err != nil {
				return err
			}
			fields, err := root.Mapping()
			if err != nil {
				return err
			}
			for _, field := range fields {
				prefix := declaration.family + "[" + strconv.Quote(view.Paths.FlowPath+":"+field.Name) + "]"
				sources := map[string]EffectiveValueProvenance{}
				sources[""] = authoredSourceProvenance(field.Value)
				if err := collectNodeValueProvenance(field.Value, "", sources, nil); err != nil {
					return err
				}
				for path, source := range sources {
					if path == "" {
						builder.set(prefix, source)
					} else if strings.HasPrefix(path, "[") {
						builder.set(prefix+path, source)
					} else {
						builder.set(prefix+"."+path, source)
					}
				}
			}
		}
	}
	return nil
}
