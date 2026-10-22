package contracts

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/packmodel"
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
	if bundle.SourceArtifact == nil {
		return out, nil
	}
	holders := map[string]bool{}
	for _, provider := range bundle.SourceArtifact.ProviderDirectories() {
		if _, trigger := provider.Body(packmodel.BodyTrigger); trigger {
			holders[provider.FlowPath()] = true
		}
	}
	if len(holders) == 0 {
		return out, nil
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return nil, err
	}
	for _, view := range bundle.FlowViews() {
		flowID := view.Paths.FlowPath
		if !holders[flowID] {
			continue
		}
		alias, err := deriveRootedIngressAlias(rooted.Admission(), flowID)
		if err != nil {
			return nil, fmt.Errorf("%s webhook alias: %w", view.Paths.SchemaFile, err)
		}
		if err := ValidateIngressAlias(alias); err != nil {
			return nil, fmt.Errorf("%s: %w", view.Paths.SchemaFile, err)
		}
		out[flowID] = compiledFlowIngressAlias{value: alias, provenance: EffectiveValueProvenance{
			Origin: EffectiveValueOriginDerived, RuleID: "flow.rooted_ingress_alias",
			InputPaths: []string{"source.root_directory", "schemas[" + strconv.Quote(flowID) + "].declaration"},
			SourceFile: view.Paths.SchemaFile,
		}}
	}
	return out, nil
}

func deriveRootedIngressAlias(root sourceartifact.RootAdmission, flowID string) (string, error) {
	if err := root.Validate(); err != nil {
		return "", err
	}
	if flowID == "." {
		return root.RootDirectory(), nil
	}
	for _, segment := range strings.Split(flowID, "/") {
		if err := sourceartifact.ValidateFlowSegment(segment); err != nil {
			return "", err
		}
	}
	return root.RootDirectory() + "." + strings.ReplaceAll(flowID, "/", "."), nil
}
