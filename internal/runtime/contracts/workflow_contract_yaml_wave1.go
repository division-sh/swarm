package contracts

import (
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var builtinWave1ScalarTypes = map[string]struct{}{
	"text":      {},
	"integer":   {},
	"numeric":   {},
	"boolean":   {},
	"timestamp": {},
	"uuid":      {},
}

var projectPackageDocumentFields = map[string]struct{}{
	"name":             {},
	"version":          {},
	"platform_version": {},
	"author":           {},
	"description":      {},
	"keywords":         {},
	"license":          {},
	"repository":       {},
	"extra":            {},
	"requires":         {},
	"flows":            {},
	"packages":         {},
	"children":         {},
	"subpackages":      {},
	"connect":          {},
	"connector_packs":  {},
	"handoffs":         {},
}

var projectFlowRefFields = map[string]struct{}{
	"id":         {},
	"flow":       {},
	"namespace":  {},
	"mode":       {},
	"activation": {},
	"ingress":    {},
	"bind":       {},
}

var projectFlowIngressFields = map[string]struct{}{
	"alias":     {},
	"providers": {},
}

var projectFlowIngressProviderFields = map[string]struct{}{
	"provider":       {},
	"signing_secret": {},
	"admission":      {},
}

var projectFlowIngressAdmissionFields = map[string]struct{}{
	"kind":           {},
	"pack":           {},
	"acknowledge":    {},
	"authentication": {},
	"event":          {},
	"delivery_id":    {},
	"payload":        {},
}

var projectFlowIngressAdmissionPackFields = map[string]struct{}{"id": {}}

var projectFlowIngressAuthenticationFields = map[string]struct{}{
	"kind": {}, "header": {}, "prefix": {}, "encoding": {},
}

var projectFlowIngressDeliveryIDFields = map[string]struct{}{
	"source": {}, "header": {}, "json_path": {},
}

func (p *ProjectPackageDocument) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	if hasYAMLMappingKey(node, "entity_schema") {
		return fmt.Errorf("RETIRED: package.yaml entity_schema is no longer supported; migrate to entities.yaml")
	}
	if err := validateProjectPackageDocumentFields(node); err != nil {
		return err
	}
	var aux struct {
		Name            string               `yaml:"name"`
		Version         string               `yaml:"version"`
		PlatformVersion string               `yaml:"platform_version"`
		Author          string               `yaml:"author"`
		Description     string               `yaml:"description"`
		Requires        FlowPackageRequires  `yaml:"requires"`
		Flows           []ProjectFlowRef     `yaml:"flows"`
		Packages        []ProjectPackageRef  `yaml:"packages"`
		Children        []ProjectPackageRef  `yaml:"children"`
		Subpackages     []ProjectPackageRef  `yaml:"subpackages"`
		Connect         []FlowPackageConnect `yaml:"connect"`
		ConnectorPacks  ConnectorPackImports `yaml:"connector_packs"`
		Handoffs        []ProjectHandoff     `yaml:"handoffs"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	keywords, err := decodePackageKeywordsYAML(yamlMappingValue(node, "keywords"))
	if err != nil {
		return err
	}
	license, err := decodePackageLicenseYAML(yamlMappingValue(node, "license"))
	if err != nil {
		return err
	}
	repository, err := decodePackageRepositoryYAML(yamlMappingValue(node, "repository"))
	if err != nil {
		return err
	}
	extra, err := decodePackageExtraYAML(yamlMappingValue(node, "extra"))
	if err != nil {
		return err
	}
	*p = ProjectPackageDocument{
		Name:            aux.Name,
		Version:         aux.Version,
		PlatformVersion: aux.PlatformVersion,
		Author:          aux.Author,
		Description:     aux.Description,
		Keywords:        keywords,
		License:         license,
		Repository:      repository,
		Extra:           extra,
		Requires:        aux.Requires.normalized(),
		Flows:           append([]ProjectFlowRef(nil), aux.Flows...),
		Packages:        append([]ProjectPackageRef(nil), aux.Packages...),
		Children:        append([]ProjectPackageRef(nil), aux.Children...),
		Subpackages:     append([]ProjectPackageRef(nil), aux.Subpackages...),
		Connect:         cloneFlowPackageConnects(aux.Connect),
		ConnectorPacks:  aux.ConnectorPacks.normalized(),
		Handoffs:        append([]ProjectHandoff(nil), aux.Handoffs...),
	}
	return nil
}

func validateProjectPackageDocumentFields(node *yaml.Node) error {
	if node == nil || node.Kind == 0 {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return NewPackageDocumentMappingDiagnostic(nil)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := projectPackageDocumentFields[key]; !ok {
			return NewUndefinedFieldDiagnostic("package.yaml", key, projectPackageDocumentFields)
		}
	}
	return nil
}

func (f *ProjectFlowRef) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	if err := validateKnownMappingFields(node, "ProjectFlowRef package flow entry", projectFlowRefFields); err != nil {
		return err
	}
	type rawProjectFlowRef ProjectFlowRef
	var out rawProjectFlowRef
	if err := node.Decode(&out); err != nil {
		return err
	}
	*f = ProjectFlowRef(out)
	return nil
}

func (i *ProjectFlowIngress) UnmarshalYAML(node *yaml.Node) error {
	if i == nil {
		return nil
	}
	if err := validateKnownMappingFields(node, "package flow ingress", projectFlowIngressFields); err != nil {
		return err
	}
	type rawProjectFlowIngress ProjectFlowIngress
	var out rawProjectFlowIngress
	if err := node.Decode(&out); err != nil {
		return err
	}
	*i = ProjectFlowIngress(out)
	return nil
}

func (p *ProjectFlowIngressProvider) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	if err := validateKnownMappingFields(node, "package flow ingress provider", projectFlowIngressProviderFields); err != nil {
		return err
	}
	type rawProjectFlowIngressProvider ProjectFlowIngressProvider
	var out rawProjectFlowIngressProvider
	if err := node.Decode(&out); err != nil {
		return err
	}
	*p = ProjectFlowIngressProvider(out)
	return nil
}

func (a *ProjectFlowIngressAdmission) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	if err := validateKnownMappingFields(node, "package flow ingress admission", projectFlowIngressAdmissionFields); err != nil {
		return err
	}
	type raw ProjectFlowIngressAdmission
	var out raw
	if err := node.Decode(&out); err != nil {
		return err
	}
	*a = ProjectFlowIngressAdmission(out)
	return nil
}

func (p *ProjectFlowIngressAdmissionPack) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	if err := validateKnownMappingFields(node, "package flow ingress admission pack", projectFlowIngressAdmissionPackFields); err != nil {
		return err
	}
	type raw ProjectFlowIngressAdmissionPack
	var out raw
	if err := node.Decode(&out); err != nil {
		return err
	}
	*p = ProjectFlowIngressAdmissionPack(out)
	return nil
}

func (a *ProjectFlowIngressAuthentication) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	if err := validateKnownMappingFields(node, "package flow ingress admission authentication", projectFlowIngressAuthenticationFields); err != nil {
		return err
	}
	type raw ProjectFlowIngressAuthentication
	var out raw
	if err := node.Decode(&out); err != nil {
		return err
	}
	*a = ProjectFlowIngressAuthentication(out)
	return nil
}

func (d *ProjectFlowIngressDeliveryID) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if err := validateKnownMappingFields(node, "package flow ingress admission delivery_id", projectFlowIngressDeliveryIDFields); err != nil {
		return err
	}
	type raw ProjectFlowIngressDeliveryID
	var out raw
	if err := node.Decode(&out); err != nil {
		return err
	}
	*d = ProjectFlowIngressDeliveryID(out)
	return nil
}

func validateKnownMappingFields(node *yaml.Node, owner string, fields map[string]struct{}) error {
	if node == nil || node.Kind == 0 {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("%s must be a mapping", owner)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := fields[key]; !ok {
			return NewUndefinedFieldDiagnostic(owner, key, fields)
		}
	}
	return nil
}

func yamlMappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == key {
			return node.Content[i+1]
		}
	}
	return nil
}

var flowPackageRequiresFieldOptions = map[string]struct{}{
	"inputs":           {},
	"outputs":          {},
	"policy":           {},
	"credentials":      {},
	"platform_version": {},
}

var flowPackageBindFieldOptions = map[string]struct{}{
	"inputs":      {},
	"outputs":     {},
	"policy":      {},
	"credentials": {},
	"observe":     {},
}

var connectorPackFieldOptions = map[string]struct{}{
	"imports": {},
}

var connectorPackImportFieldOptions = map[string]struct{}{
	"provider": {},
	"tool":     {},
}

var flowPackageRequiresPolicyFieldOptions = map[string]struct{}{
	"default":     {},
	"type":        {},
	"description": {},
	"required":    {},
}

var flowPackageConnectFieldOptions = map[string]struct{}{
	"from":     {},
	"to":       {},
	"adapter":  {},
	"using":    {},
	"map":      {},
	"delivery": {},
	"reply":    {},
}

var flowPackageConnectUsingFieldOptions = map[string]struct{}{
	"instance": {},
}

var flowPackageConnectInstanceFieldOptions = map[string]struct{}{
	"source": {},
	"target": {},
}

var flowPackageConnectMapFieldOptions = map[string]struct{}{
	"source": {},
	"target": {},
}

var typeCatalogFieldOptions = map[string]struct{}{
	"scalars": {},
	"enums":   {},
	"types":   {},
}

var typeMetadataFieldOptions = map[string]struct{}{
	"_description": {},
}

var entityMetadataFieldOptions = map[string]struct{}{
	"_description": {},
	"_owner":       {},
}

var schemaLengthRefinementFieldOptions = map[string]struct{}{
	"min": {},
	"max": {},
}

var schemaRangeRefinementFieldOptions = map[string]struct{}{
	"min": {},
	"max": {},
}

func (r *FlowPackageRequires) UnmarshalYAML(node *yaml.Node) error {
	if r == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*r = FlowPackageRequires{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("requires must be a mapping")
	}
	var out FlowPackageRequires
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "inputs":
			if err := value.Decode(&out.Inputs); err != nil {
				return fmt.Errorf("requires.inputs: %w", err)
			}
		case "outputs":
			if err := value.Decode(&out.Outputs); err != nil {
				return fmt.Errorf("requires.outputs: %w", err)
			}
		case "policy":
			policy, defaults, err := decodeFlowPackagePolicyRequires(value)
			if err != nil {
				return fmt.Errorf("requires.policy: %w", err)
			}
			out.Policy = policy
			out.PolicyDefaults = defaults
		case "credentials":
			if err := value.Decode(&out.Credentials); err != nil {
				return fmt.Errorf("requires.credentials: %w", err)
			}
		case "platform_version":
			if err := value.Decode(&out.PlatformVersion); err != nil {
				return fmt.Errorf("requires.platform_version: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("requires", key, flowPackageRequiresFieldOptions)
		}
	}
	*r = out.normalized()
	return nil
}

func (b *FlowPackageBind) UnmarshalYAML(node *yaml.Node) error {
	if b == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*b = FlowPackageBind{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("bind must be a mapping")
	}
	var out FlowPackageBind
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "inputs":
			if err := value.Decode(&out.Inputs); err != nil {
				return fmt.Errorf("bind.inputs: %w", err)
			}
		case "outputs":
			if err := value.Decode(&out.Outputs); err != nil {
				return fmt.Errorf("bind.outputs: %w", err)
			}
		case "policy":
			if err := value.Decode(&out.Policy); err != nil {
				return fmt.Errorf("bind.policy: %w", err)
			}
		case "credentials":
			if err := value.Decode(&out.Credentials); err != nil {
				return fmt.Errorf("bind.credentials: %w", err)
			}
		case "observe":
			if err := value.Decode(&out.Observe); err != nil {
				return fmt.Errorf("bind.observe: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("bind", key, flowPackageBindFieldOptions)
		}
	}
	*b = out.normalized()
	return nil
}

func (c *ConnectorPackImports) UnmarshalYAML(node *yaml.Node) error {
	if c == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*c = ConnectorPackImports{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("connector_packs must be a mapping")
	}
	var out ConnectorPackImports
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "imports":
			if err := value.Decode(&out.Imports); err != nil {
				return fmt.Errorf("connector_packs.imports: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("connector_packs", key, connectorPackFieldOptions)
		}
	}
	*c = out.normalized()
	return nil
}

func (i *ConnectorPackImport) UnmarshalYAML(node *yaml.Node) error {
	if i == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*i = ConnectorPackImport{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("connector_packs.imports entries must be mappings")
	}
	var out ConnectorPackImport
	for j := 0; j+1 < len(node.Content); j += 2 {
		key := strings.TrimSpace(node.Content[j].Value)
		value := node.Content[j+1]
		switch key {
		case "":
			continue
		case "provider":
			if err := value.Decode(&out.Provider); err != nil {
				return fmt.Errorf("provider: %w", err)
			}
		case "tool":
			if err := value.Decode(&out.Tool); err != nil {
				return fmt.Errorf("tool: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("connector_packs.imports", key, connectorPackImportFieldOptions)
		}
	}
	*i = out.normalized()
	return nil
}

func (c ConnectorPackImports) normalized() ConnectorPackImports {
	out := ConnectorPackImports{Imports: make([]ConnectorPackImport, 0, len(c.Imports))}
	for _, item := range c.Imports {
		item = item.normalized()
		if item.Provider == "" && item.Tool == "" {
			continue
		}
		out.Imports = append(out.Imports, item)
	}
	return out
}

func (i ConnectorPackImport) normalized() ConnectorPackImport {
	return ConnectorPackImport{
		Provider: normalizeConnectorPackToken(i.Provider),
		Tool:     strings.TrimSpace(i.Tool),
	}
}

func normalizeConnectorPackToken(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.ReplaceAll(raw, "-", "_")
	raw = strings.ReplaceAll(raw, " ", "_")
	return strings.Trim(raw, "_")
}

func (r FlowPackageRequires) normalized() FlowPackageRequires {
	return FlowPackageRequires{
		Inputs:          normalizeStrings(r.Inputs),
		Outputs:         normalizeStrings(r.Outputs),
		Policy:          normalizeStrings(r.Policy),
		PolicyDefaults:  normalizePolicyDefaults(r.PolicyDefaults),
		Credentials:     normalizeStrings(r.Credentials),
		PlatformVersion: strings.TrimSpace(r.PlatformVersion),
	}
}

func decodeFlowPackagePolicyRequires(node *yaml.Node) ([]string, map[string]PolicyValue, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		var values []string
		if err := node.Decode(&values); err != nil {
			return nil, nil, err
		}
		return values, nil, nil
	case yaml.MappingNode:
		var policy []string
		defaults := map[string]PolicyValue{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			if key == "" {
				continue
			}
			policy = append(policy, key)
			defaultValue, ok, err := decodeFlowPackagePolicyDefault(node.Content[i+1])
			if err != nil {
				return nil, nil, fmt.Errorf("%s: %w", key, err)
			}
			if ok {
				defaults[key] = PolicyValue{Value: defaultValue}
			}
		}
		if len(defaults) == 0 {
			defaults = nil
		}
		return policy, defaults, nil
	default:
		return nil, nil, fmt.Errorf("must be a list of policy keys or a mapping of policy keys to requirement objects")
	}
}

func decodeFlowPackagePolicyDefault(node *yaml.Node) (any, bool, error) {
	if node == nil || node.Kind == 0 {
		return nil, false, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("policy requirement must be a mapping with optional default")
	}
	var out any
	hasDefault := false
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "default":
			if err := value.Decode(&out); err != nil {
				return nil, false, fmt.Errorf("default: %w", err)
			}
			hasDefault = true
		case "type", "description", "required":
			continue
		default:
			return nil, false, NewUndefinedFieldDiagnostic("requires.policy", key, flowPackageRequiresPolicyFieldOptions)
		}
	}
	return out, hasDefault, nil
}

func normalizePolicyDefaults(in map[string]PolicyValue) map[string]PolicyValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]PolicyValue, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = PolicyValue{
			Value:       value.Value,
			Description: strings.TrimSpace(value.Description),
			Override:    value.Override,
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (b FlowPackageBind) normalized() FlowPackageBind {
	return FlowPackageBind{
		Inputs:      normalizeStringMap(b.Inputs),
		Outputs:     normalizeStringMap(b.Outputs),
		Policy:      normalizeStringMap(b.Policy),
		Credentials: normalizeStringMap(b.Credentials),
		Observe:     normalizeFlowPackageObserveGrants(b.Observe),
	}
}

func normalizeFlowPackageObserveGrants(in []FlowPackageObserveGrant) []FlowPackageObserveGrant {
	if len(in) == 0 {
		return nil
	}
	out := make([]FlowPackageObserveGrant, 0, len(in))
	for _, grant := range in {
		source := strings.TrimSpace(grant.Source)
		events := normalizeStrings(grant.Events)
		if source == "" && len(events) == 0 {
			continue
		}
		out = append(out, FlowPackageObserveGrant{
			Source: source,
			Events: events,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (c *FlowPackageConnect) UnmarshalYAML(node *yaml.Node) error {
	if c == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*c = FlowPackageConnect{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("connect entry must be a mapping")
	}
	out := FlowPackageConnect{SourceLine: node.Line}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "from":
			if err := value.Decode(&out.From); err != nil {
				return fmt.Errorf("connect.from: %w", err)
			}
		case "to":
			if err := value.Decode(&out.To); err != nil {
				return fmt.Errorf("connect.to: %w", err)
			}
		case "adapter":
			if err := value.Decode(&out.Adapter); err != nil {
				return fmt.Errorf("connect.adapter: %w", err)
			}
		case "using":
			if err := value.Decode(&out.Using); err != nil {
				return fmt.Errorf("connect.using: %w", err)
			}
		case "map":
			if err := value.Decode(&out.Map); err != nil {
				return fmt.Errorf("connect.map: %w", err)
			}
		case "delivery":
			if err := value.Decode(&out.Delivery); err != nil {
				return fmt.Errorf("connect.delivery: %w", err)
			}
		case "reply":
			if err := value.Decode(&out.Reply); err != nil {
				return fmt.Errorf("connect.reply: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("connect", key, flowPackageConnectFieldOptions)
		}
	}
	*c = out.normalized()
	return nil
}

func (u *FlowPackageConnectUsing) UnmarshalYAML(node *yaml.Node) error {
	if u == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*u = FlowPackageConnectUsing{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("connect using must be a mapping")
	}
	var out FlowPackageConnectUsing
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "instance":
			if err := value.Decode(&out.Instance); err != nil {
				return fmt.Errorf("connect.using.instance: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("connect using", key, flowPackageConnectUsingFieldOptions)
		}
	}
	*u = out.normalized()
	return nil
}

func (a *FlowPackageConnectInstanceAdapter) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*a = FlowPackageConnectInstanceAdapter{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("connect.using.instance must be a mapping")
	}
	out := FlowPackageConnectInstanceAdapter{Declared: true}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "source":
			source, err := decodeStringListNodePreserveOrder(value)
			if err != nil {
				return fmt.Errorf("connect.using.instance.source: %w", err)
			}
			out.Source = source
		case "target":
			target, err := decodeStringListNodePreserveOrder(value)
			if err != nil {
				return fmt.Errorf("connect.using.instance.target: %w", err)
			}
			out.Target = target
		default:
			return NewUndefinedFieldDiagnostic("connect using instance", key, flowPackageConnectInstanceFieldOptions)
		}
	}
	*a = out.normalized()
	return nil
}

func decodeStringListNodePreserveOrder(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return nil, nil
		}
		return []string{strings.TrimSpace(node.Value)}, nil
	case yaml.SequenceNode:
		var values []string
		if err := node.Decode(&values); err != nil {
			return nil, err
		}
		out := make([]string, 0, len(values))
		for _, value := range values {
			if value = strings.TrimSpace(value); value != "" {
				out = append(out, value)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported string list yaml node kind %d", node.Kind)
	}
}

func (m *FlowPackageConnectMap) UnmarshalYAML(node *yaml.Node) error {
	if m == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*m = FlowPackageConnectMap{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("connect map entry must be a mapping")
	}
	var out FlowPackageConnectMap
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "source":
			if err := value.Decode(&out.Source); err != nil {
				return fmt.Errorf("connect map source: %w", err)
			}
		case "target":
			if err := value.Decode(&out.Target); err != nil {
				return fmt.Errorf("connect map target: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("connect map", key, flowPackageConnectMapFieldOptions)
		}
	}
	*m = FlowPackageConnectMap{
		Source: strings.TrimSpace(out.Source),
		Target: strings.TrimSpace(out.Target),
	}
	return nil
}

func normalizeStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (d *TypeCatalogDocument) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*d = TypeCatalogDocument{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("type catalog must be a mapping")
	}
	doc := TypeCatalogDocument{
		Scalars: map[string]ScalarTypeDecl{},
		Enums:   map[string]EnumTypeDecl{},
		Types:   map[string]NamedTypeDecl{},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "scalars":
			if err := value.Decode(&doc.Scalars); err != nil {
				return err
			}
		case "enums":
			if err := value.Decode(&doc.Enums); err != nil {
				return err
			}
		case "types":
			if err := value.Decode(&doc.Types); err != nil {
				return err
			}
		default:
			return NewUndefinedFieldDiagnostic("type catalog", key, typeCatalogFieldOptions)
		}
	}
	*d = doc
	return nil
}

func (s *ScalarTypeDecl) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	base, err := decodeScalarStringNode(node)
	if err != nil {
		return err
	}
	if err := validateWave1TypeRef(base, "scalar alias"); err != nil {
		return err
	}
	if !isBuiltinWave1Scalar(base) {
		return fmt.Errorf("RETIRED: scalar alias %q must resolve to a supported built-in scalar", strings.TrimSpace(base))
	}
	s.Base = base
	return nil
}

func (e *EnumTypeDecl) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	values, err := decodeStringListNode(node)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return fmt.Errorf("enum declaration requires at least one value")
	}
	e.Values = values
	return nil
}

func (n *NamedTypeDecl) UnmarshalYAML(node *yaml.Node) error {
	if n == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*n = NamedTypeDecl{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("named type declaration must be a mapping")
	}
	decl := NamedTypeDecl{Fields: map[string]TypeFieldSpec{}}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if key == "" {
			continue
		}
		if strings.HasPrefix(key, "_") {
			switch key {
			case "_description":
				text, err := decodeScalarStringNode(value)
				if err != nil {
					return err
				}
				decl.Description = text
			default:
				return NewUndefinedFieldDiagnostic("type metadata", key, typeMetadataFieldOptions)
			}
			continue
		}
		var field TypeFieldSpec
		if err := value.Decode(&field); err != nil {
			return err
		}
		decl.Fields[key] = field
	}
	*n = decl
	return nil
}

func (f *TypeFieldSpec) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	parsed, err := decodeWave1FieldNode(node, wave1FieldNodeOptions{
		Context:           "type field",
		AllowInitial:      false,
		AllowImmutable:    false,
		AllowUnusedReason: false,
	})
	if err != nil {
		return err
	}
	f.Type = parsed.Type
	f.Description = parsed.Description
	f.Refinements = parsed.Refinements
	return nil
}

func (d *EntityContractsDocument) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*d = EntityContractsDocument{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("entity contracts document must be a mapping")
	}
	out := make(EntityContractsDocument, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		var entity EntityContract
		if err := node.Content[i+1].Decode(&entity); err != nil {
			return err
		}
		out[key] = entity
	}
	*d = out
	return nil
}

func (e *EntityContract) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*e = EntityContract{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("entity contract must be a mapping")
	}
	decl := EntityContract{Fields: map[string]EntityFieldDecl{}}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if key == "" {
			continue
		}
		if strings.HasPrefix(key, "_") {
			switch key {
			case "_description":
				text, err := decodeScalarStringNode(value)
				if err != nil {
					return err
				}
				decl.Description = text
			case "_owner":
				text, err := decodeScalarStringNode(value)
				if err != nil {
					return err
				}
				decl.Owner = text
			case "_state_model":
				return fmt.Errorf("RETIRED: entity field %q is retired; state authority is implicit from schema.yaml", key)
			default:
				return NewUndefinedFieldDiagnostic("entity metadata", key, entityMetadataFieldOptions)
			}
			continue
		}
		if key == "state_field" {
			return fmt.Errorf("RETIRED: entity field %q is retired; state authority is implicit from schema.yaml", key)
		}
		var field EntityFieldDecl
		if err := value.Decode(&field); err != nil {
			return err
		}
		decl.Fields[key] = field
	}
	*e = decl
	return nil
}

func (f *EntityFieldDecl) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	parsed, err := decodeWave1FieldNode(node, wave1FieldNodeOptions{
		Context:                 "entity field",
		AllowInitial:            true,
		AllowImmutable:          true,
		AllowIndexed:            true,
		AllowUnusedReason:       true,
		AllowUnusedReaderReason: true,
		AllowMaterializeFrom:    true,
		AllowProject:            true,
	})
	if err != nil {
		return err
	}
	f.Type = parsed.Type
	f.Initial = parsed.Initial
	f.Indexed = parsed.Indexed
	f.Immutable = parsed.Immutable
	f.Description = parsed.Description
	f.Refinements = parsed.Refinements
	f.MaterializeFrom = parsed.MaterializeFrom
	f.Project = parsed.Project
	f.UnusedReason = parsed.UnusedReason
	f.UnusedReaderReason = parsed.UnusedReaderReason
	return nil
}

func decodeWave1FieldNode(node *yaml.Node, opts wave1FieldNodeOptions) (wave1ParsedFieldNode, error) {
	if node == nil || node.Kind == 0 {
		return wave1ParsedFieldNode{}, fmt.Errorf("%s type is required", opts.Context)
	}
	switch node.Kind {
	case yaml.ScalarNode:
		typ, err := decodeScalarStringNode(node)
		if err != nil {
			return wave1ParsedFieldNode{}, err
		}
		if err := validateWave1TypeRef(typ, opts.Context); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		return wave1ParsedFieldNode{Type: typ}, nil
	case yaml.SequenceNode:
		values, err := decodeStringListNode(node)
		if err != nil {
			return wave1ParsedFieldNode{}, err
		}
		if len(values) != 1 {
			return wave1ParsedFieldNode{}, fmt.Errorf("%s list shorthand requires exactly one element type", opts.Context)
		}
		typ := "[" + strings.TrimSpace(values[0]) + "]"
		if err := validateWave1TypeRef(typ, opts.Context); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		return wave1ParsedFieldNode{Type: typ}, nil
	case yaml.MappingNode:
	default:
		return wave1ParsedFieldNode{}, fmt.Errorf("unsupported %s yaml node kind %d", opts.Context, node.Kind)
	}

	allowed := wave1FieldNodeAllowedKeys(opts)

	var field wave1ParsedFieldNode
	var listOf string
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			switch key {
			case "properties", "fields", "shape":
				return wave1ParsedFieldNode{}, fmt.Errorf("RETIRED: %s inline object declarations are retired; declare a named type in types.yaml", opts.Context)
			case "of":
				listValue, err := decodeScalarStringNode(value)
				if err != nil {
					return wave1ParsedFieldNode{}, err
				}
				listOf = listValue
				continue
			case "initial", "immutable", "indexed", "_unused_reason", "_unused_reader_reason", "materialize_from", "project":
				return wave1ParsedFieldNode{}, NewUndefinedFieldDiagnostic(opts.Context, key, allowed)
			default:
				return wave1ParsedFieldNode{}, NewUndefinedFieldDiagnostic(opts.Context, key, allowed)
			}
		}
		switch key {
		case "type":
			typ, err := decodeScalarStringNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.Type = typ
		case "description":
			text, err := decodeScalarStringNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.Description = text
		case "pattern":
			pattern, err := decodeSchemaRefinementPattern(value)
			if err != nil {
				return wave1ParsedFieldNode{}, fmt.Errorf("%s pattern: %w", opts.Context, err)
			}
			field.Refinements.Pattern = pattern
		case "length":
			length, err := decodeSchemaLengthRefinement(value)
			if err != nil {
				return wave1ParsedFieldNode{}, fmt.Errorf("%s length: %w", opts.Context, err)
			}
			field.Refinements.Length = length
		case "range":
			bounds, err := decodeSchemaRangeRefinement(value)
			if err != nil {
				return wave1ParsedFieldNode{}, fmt.Errorf("%s range: %w", opts.Context, err)
			}
			field.Refinements.Range = bounds
		case "equal_to":
			target, err := decodeScalarStringNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			target = strings.TrimSpace(target)
			if target == "" {
				return wave1ParsedFieldNode{}, fmt.Errorf("%s equal_to field is required", opts.Context)
			}
			field.Refinements.EqualTo = target
		case "citation":
			if !opts.AllowCitation {
				return wave1ParsedFieldNode{}, NewUndefinedFieldDiagnostic(opts.Context, key, allowed)
			}
			var citation CriteriaCitation
			if err := value.Decode(&citation); err != nil {
				return wave1ParsedFieldNode{}, err
			}
			citation.Criteria = strings.TrimSpace(citation.Criteria)
			citation.AllowedClasses = normalizeStrings(citation.AllowedClasses)
			field.Citation = citation
		case "initial":
			var initial any
			if err := value.Decode(&initial); err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.Initial = initial
		case "immutable":
			immutable, err := decodeBoolNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.Immutable = immutable
		case "indexed":
			indexed, err := decodeBoolNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.Indexed = indexed
		case "_unused_reason":
			text, err := decodeScalarStringNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.UnusedReason = text
		case "_unused_reader_reason":
			text, err := decodeScalarStringNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.UnusedReaderReason = text
		case "materialize_from":
			text, err := decodeScalarStringNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.MaterializeFrom = strings.TrimSpace(text)
		case "project":
			project, err := decodeProjectionMapNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.Project = project
		}
	}
	if strings.EqualFold(strings.TrimSpace(field.Type), "list") {
		if strings.TrimSpace(listOf) == "" {
			return wave1ParsedFieldNode{}, fmt.Errorf("RETIRED: %s list declarations require an of: element type", opts.Context)
		}
		if err := validateWave1TypeRef(listOf, opts.Context); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		field.Type = fmt.Sprintf("[%s]", strings.TrimSpace(listOf))
	}
	if err := validateWave1TypeRef(field.Type, opts.Context); err != nil {
		return wave1ParsedFieldNode{}, err
	}
	if opts.AllowUnusedReason && strings.TrimSpace(field.UnusedReason) != "" && len(strings.TrimSpace(field.UnusedReason)) < 10 {
		return wave1ParsedFieldNode{}, fmt.Errorf("%s _unused_reason must be at least 10 characters", opts.Context)
	}
	if opts.AllowUnusedReaderReason && strings.TrimSpace(field.UnusedReaderReason) != "" && len(strings.TrimSpace(field.UnusedReaderReason)) < 10 {
		return wave1ParsedFieldNode{}, fmt.Errorf("%s _unused_reader_reason must be at least 10 characters", opts.Context)
	}
	return field, nil
}

func wave1FieldNodeAllowedKeys(opts wave1FieldNodeOptions) map[string]struct{} {
	allowed := map[string]struct{}{
		"type":        {},
		"description": {},
		"pattern":     {},
		"length":      {},
		"range":       {},
		"equal_to":    {},
	}
	if opts.AllowInitial {
		allowed["initial"] = struct{}{}
	}
	if opts.AllowImmutable {
		allowed["immutable"] = struct{}{}
	}
	if opts.AllowIndexed {
		allowed["indexed"] = struct{}{}
	}
	if opts.AllowUnusedReason {
		allowed["_unused_reason"] = struct{}{}
	}
	if opts.AllowUnusedReaderReason {
		allowed["_unused_reader_reason"] = struct{}{}
	}
	if opts.AllowMaterializeFrom {
		allowed["materialize_from"] = struct{}{}
	}
	if opts.AllowProject {
		allowed["project"] = struct{}{}
	}
	if opts.AllowCitation {
		allowed["citation"] = struct{}{}
	}
	return allowed
}

func wave1FieldNodeSupportsKey(opts wave1FieldNodeOptions, key string) bool {
	if strings.TrimSpace(key) == "of" {
		return true
	}
	_, ok := wave1FieldNodeAllowedKeys(opts)[strings.TrimSpace(key)]
	return ok
}

func eventPayloadNodeIsRetiredNestedBlock(node *yaml.Node) bool {
	if node == nil || node.Kind != yaml.MappingNode {
		return false
	}
	if hasAnyYAMLMappingKey(node, "properties", "fields", "shape", "required") {
		return true
	}
	hasType := hasYAMLMappingKey(node, "type")
	opts := wave1FieldNodeOptions{Context: "event payload field", AllowCitation: true}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" || wave1FieldNodeSupportsKey(opts, key) {
			continue
		}
		return !hasType
	}
	return false
}

func decodeSchemaRefinementPattern(node *yaml.Node) (string, error) {
	pattern, err := decodeScalarStringNode(node)
	if err != nil {
		return "", err
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return "", fmt.Errorf("must not be empty")
	}
	if _, err := regexp.Compile(pattern); err != nil {
		return "", fmt.Errorf("must compile as a regular expression: %w", err)
	}
	return pattern, nil
}

func decodeSchemaLengthRefinement(node *yaml.Node) (SchemaLengthRefinement, error) {
	if node == nil || node.Kind != yaml.MappingNode {
		return SchemaLengthRefinement{}, fmt.Errorf("must be a mapping with min and/or max")
	}
	var out SchemaLengthRefinement
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "min":
			min, err := decodeIntNode(value)
			if err != nil {
				return SchemaLengthRefinement{}, fmt.Errorf("min: %w", err)
			}
			out.Min = &min
		case "max":
			max, err := decodeIntNode(value)
			if err != nil {
				return SchemaLengthRefinement{}, fmt.Errorf("max: %w", err)
			}
			out.Max = &max
		default:
			return SchemaLengthRefinement{}, NewUndefinedFieldDiagnostic("length", key, schemaLengthRefinementFieldOptions)
		}
	}
	if out.Min == nil && out.Max == nil {
		return SchemaLengthRefinement{}, fmt.Errorf("must declare min and/or max")
	}
	if out.Min != nil && *out.Min < 0 {
		return SchemaLengthRefinement{}, fmt.Errorf("min must be >= 0")
	}
	if out.Max != nil && *out.Max < 0 {
		return SchemaLengthRefinement{}, fmt.Errorf("max must be >= 0")
	}
	if out.Min != nil && out.Max != nil && *out.Min > *out.Max {
		return SchemaLengthRefinement{}, fmt.Errorf("min must be <= max")
	}
	return out, nil
}

func decodeSchemaRangeRefinement(node *yaml.Node) (SchemaRangeRefinement, error) {
	if node == nil || node.Kind != yaml.MappingNode {
		return SchemaRangeRefinement{}, fmt.Errorf("must be a mapping with min and/or max")
	}
	var out SchemaRangeRefinement
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "min":
			min, err := decodeFloatNode(value)
			if err != nil {
				return SchemaRangeRefinement{}, fmt.Errorf("min: %w", err)
			}
			out.Min = &min
		case "max":
			max, err := decodeFloatNode(value)
			if err != nil {
				return SchemaRangeRefinement{}, fmt.Errorf("max: %w", err)
			}
			out.Max = &max
		default:
			return SchemaRangeRefinement{}, NewUndefinedFieldDiagnostic("range", key, schemaRangeRefinementFieldOptions)
		}
	}
	if out.Min == nil && out.Max == nil {
		return SchemaRangeRefinement{}, fmt.Errorf("must declare min and/or max")
	}
	if out.Min != nil && out.Max != nil && *out.Min > *out.Max {
		return SchemaRangeRefinement{}, fmt.Errorf("min must be <= max")
	}
	return out, nil
}

func decodeIntNode(node *yaml.Node) (int, error) {
	var value int
	if err := node.Decode(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func decodeFloatNode(node *yaml.Node) (float64, error) {
	var value float64
	if err := node.Decode(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func decodeProjectionMapNode(node *yaml.Node) (map[string]any, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("entity field project must be a mapping")
	}
	out := make(map[string]any, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		var value any
		switch node.Content[i+1].Kind {
		case yaml.ScalarNode:
			switch strings.TrimSpace(node.Content[i+1].Tag) {
			case "!!int", "!!float", "!!bool":
				if err := node.Content[i+1].Decode(&value); err != nil {
					return nil, err
				}
			default:
				text, err := decodeScalarStringNode(node.Content[i+1])
				if err != nil {
					return nil, err
				}
				value = text
			}
		default:
			if err := node.Content[i+1].Decode(&value); err != nil {
				return nil, err
			}
		}
		out[key] = value
	}
	return out, nil
}

func validateWave1TypeRef(raw, context string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fmt.Errorf("%s type is required", context)
	}
	switch strings.ToLower(raw) {
	case "jsonb":
		return fmt.Errorf("RETIRED: %s type %q is retired; declare a named type in types.yaml", context, raw)
	case "object":
		return fmt.Errorf("RETIRED: %s type %q is retired; declare a named type in types.yaml", context, raw)
	}
	if strings.HasPrefix(raw, "Optional<") {
		return fmt.Errorf("RETIRED: %s type %q is not supported by the current type system", context, raw)
	}
	if keyType, valueType, ok := parseWave1MapTypeRef(raw); ok {
		if keyType == "" || valueType == "" {
			return fmt.Errorf("%s map type requires key and value types", context)
		}
		if strings.HasPrefix(keyType, "[") || strings.HasPrefix(strings.ToLower(keyType), "map[") {
			return fmt.Errorf("%s map key type %q must be scalar or enum", context, keyType)
		}
		if strings.EqualFold(keyType, "object") || strings.EqualFold(keyType, "jsonb") {
			return fmt.Errorf("RETIRED: %s map key type %q is retired", context, keyType)
		}
		if strings.EqualFold(valueType, "object") || strings.EqualFold(valueType, "jsonb") {
			return fmt.Errorf("RETIRED: %s map value type %q is retired; declare a named type in types.yaml", context, valueType)
		}
		return nil
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		inner := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]"))
		if inner == "" {
			return fmt.Errorf("%s list type requires an element type", context)
		}
		if strings.EqualFold(inner, "object") || strings.EqualFold(inner, "jsonb") {
			return fmt.Errorf("RETIRED: %s type %q is retired; declare a named type in types.yaml", context, raw)
		}
		return nil
	}
	return nil
}

func parseWave1MapTypeRef(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(strings.ToLower(raw), "map[") {
		return "", "", false
	}
	closeIdx := strings.Index(raw, "]")
	if closeIdx < len("map[]")-1 {
		return "", "", true
	}
	keyType := strings.TrimSpace(raw[len("map["):closeIdx])
	valueType := strings.TrimSpace(raw[closeIdx+1:])
	return keyType, valueType, true
}

func isBuiltinWave1Scalar(raw string) bool {
	_, ok := builtinWave1ScalarTypes[strings.TrimSpace(raw)]
	return ok
}

func buildFlatEventPayloadSpec(node *yaml.Node) (EventPayloadSpec, error) {
	spec := EventPayloadSpec{Properties: map[string]EventFieldSpec{}}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if key == "" || strings.HasPrefix(key, "_") {
			continue
		}
		switch key {
		case "description", "swarm", "emitter", "emitter_type", "producer", "_producer", "alternate_emitters", "consumer", "_consumer", "consumer_type", "_consumer_type", "_source", "_status", "_note", "intercepted", "passthrough", "runtime_handling", "owning_node", "delivery_channel", "required":
			continue
		}
		var field EventFieldSpec
		if err := value.Decode(&field); err != nil {
			return EventPayloadSpec{}, err
		}
		spec.Properties[key] = field
	}
	return spec, nil
}

type wave1FieldNodeOptions struct {
	Context                 string
	AllowInitial            bool
	AllowImmutable          bool
	AllowIndexed            bool
	AllowUnusedReason       bool
	AllowUnusedReaderReason bool
	AllowMaterializeFrom    bool
	AllowProject            bool
	AllowCitation           bool
}

type wave1ParsedFieldNode struct {
	Type               string
	Initial            any
	Indexed            bool
	Immutable          bool
	Description        string
	Refinements        SchemaRefinements
	Citation           CriteriaCitation
	MaterializeFrom    string
	Project            map[string]any
	UnusedReason       string
	UnusedReaderReason string
}
