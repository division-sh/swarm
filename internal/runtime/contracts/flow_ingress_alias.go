package contracts

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

type compiledFlowIngressAlias struct {
	value      string
	provenance EffectiveValueProvenance
}

// FlowIngressAlias is an immutable declaration fact, not deployment enablement.
func (b *WorkflowContractBundle) FlowIngressAlias(flowID string) (string, bool) {
	if b == nil {
		return "", false
	}
	fact, present := b.Semantics.flowIngressAliases[flowID]
	return fact.value, present
}

func ValidateIngressAlias(alias string) error {
	if alias == "" {
		return fmt.Errorf("ingress alias must be non-empty")
	}
	for i, char := range []byte(alias) {
		alphaNumeric := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9'
		if !alphaNumeric && !(i > 0 && (char == '.' || char == '_' || char == '-')) {
			return fmt.Errorf("ingress alias %q must be one URL-safe path segment matching [A-Za-z0-9][A-Za-z0-9._-]*", alias)
		}
	}
	return nil
}

func compileFlowIngressAliases(bundle *WorkflowContractBundle) (map[string]compiledFlowIngressAlias, error) {
	out := map[string]compiledFlowIngressAlias{}
	for _, view := range bundle.FlowViews() {
		if view.Schema.Ingress == nil {
			continue
		}
		flowID := view.Paths.FlowPath
		alias := view.Schema.Ingress.Alias
		provenance, authored := view.Schema.admissionProvenance["ingress.alias"]
		provenance = cloneEffectiveValueProvenance(provenance)
		if alias == "" {
			if authored {
				return nil, fmt.Errorf("%s: %w", view.Paths.SchemaFile, ValidateIngressAlias(alias))
			}
			var err error
			alias, err = deriveRootedIngressAlias(bundle.RootSchema, flowID)
			if err != nil {
				return nil, fmt.Errorf("%s ingress.alias: %w", view.Paths.SchemaFile, err)
			}
			provenance = EffectiveValueProvenance{
				Origin: EffectiveValueOriginDerived, RuleID: "flow.rooted_ingress_alias",
				InputPaths: []string{`schemas["."].name`, "schemas[" + strconv.Quote(flowID) + "].declaration"},
				SourceFile: view.Paths.SchemaFile,
			}
		} else {
			provenance.InputPaths = qualifyEffectiveEventInputPaths("schemas["+strconv.Quote(flowID)+"]", provenance.InputPaths)
		}
		if err := ValidateIngressAlias(alias); err != nil {
			return nil, fmt.Errorf("%s: %w", view.Paths.SchemaFile, err)
		}
		out[flowID] = compiledFlowIngressAlias{value: alias, provenance: provenance}
	}
	return out, nil
}

func deriveRootedIngressAlias(root *FlowSchemaDocument, flowID string) (string, error) {
	if root == nil || root.Name == "" {
		return "", fmt.Errorf("default ingress alias requires the selected root's flow name")
	}
	if err := ValidateIngressAlias(root.Name); err != nil {
		return "", err
	}
	if strings.Contains(root.Name, ".") {
		return "", fmt.Errorf("default ingress alias requires a single-segment root flow name, got %q", root.Name)
	}
	if flowID == "." {
		return root.Name, nil
	}
	for _, segment := range strings.Split(flowID, "/") {
		if err := sourceartifact.ValidateFlowSegment(segment); err != nil {
			return "", err
		}
	}
	return root.Name + "." + strings.ReplaceAll(flowID, "/", "."), nil
}
