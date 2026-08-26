package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"gopkg.in/yaml.v3"
)

// DurableDataAccessRef names one immutable resource declaration in the
// declaring agent's package. Package qualification is supplied by the loader.
type DurableDataAccessRef struct {
	Package string `json:"package,omitempty"`
	Data    string `json:"data"`
}

func (r *DurableDataAccessRef) UnmarshalYAML(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode || node.Tag == "!!null" {
		return fmt.Errorf("data_access entries must be non-null mappings")
	}
	if err := validateClosedMapping("data_access entry", node, map[string]struct{}{"package": {}, "data": {}}); err != nil {
		return err
	}
	data := yamlMappingValueNode(node, "data")
	if data == nil || data.Kind != yaml.ScalarNode || data.Tag == "!!null" || data.Value == "" || strings.TrimSpace(data.Value) != data.Value {
		return fmt.Errorf("data_access data must be a non-empty scalar without surrounding whitespace")
	}
	r.Data = data.Value
	if pkg := yamlMappingValueNode(node, "package"); pkg != nil {
		if pkg.Kind != yaml.ScalarNode || pkg.Tag == "!!null" || pkg.Value == "" || strings.TrimSpace(pkg.Value) != pkg.Value {
			return fmt.Errorf("data_access package must be a non-empty scalar without surrounding whitespace")
		}
		r.Package = pkg.Value
	}
	return nil
}

// DurableDataDeclaration is the loader-owned semantic record. Authored map
// labels remain display names; Ref is the stable package-qualified identity.
type DurableDataDeclaration struct {
	Name            string
	Ref             durabledata.DeclarationRef
	OwnerFlowID     string
	BusinessKey     string
	Schema          map[string]any
	CanonicalSchema []byte
	SchemaDigest    durabledata.SchemaDigest
	SourceFile      string
}

func loadDurableDataDeclarations(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	bundle.dataDeclarations = make(map[string]DurableDataDeclaration)
	for _, pkg := range bundle.PackageTree {
		if strings.TrimSpace(pkg.Paths.DataFile) != "" {
			return fmt.Errorf("%s: data.yaml is retired; immutable datasets are declared by authored events and imported with --data event=file", pkg.Paths.DataFile)
		}
	}
	compiled, err := bundle.CompiledEventSchemas()
	if err != nil {
		return fmt.Errorf("compile durable data declarations: %w", err)
	}
	for _, event := range compiled {
		if err := appendDurableDataEvent(bundle, event); err != nil {
			return err
		}
	}
	return validateAgentDurableDataAccess(bundle)
}

func appendDurableDataEvent(bundle *WorkflowContractBundle, event CompiledEventSchema) error {
	if !event.Importable() {
		return nil
	}
	ref, err := durabledata.ParseDeclarationRef(event.PackageKey(), event.EventName())
	if err != nil {
		return fmt.Errorf("compiled authored event %s:%s cannot own a dataset: %w", event.PackageKey(), event.EventName(), err)
	}
	businessKey := ""
	if key, ok := event.BusinessKey(); ok {
		businessKey = key.Field
	}
	schema := event.AcceptanceSchema()
	canonicalSchema := event.CanonicalAcceptanceSchema()
	if businessKey != "" {
		canonicalOwner := event.AcceptanceSchema()
		canonicalOwner["x-swarm-dataset-key"] = businessKey
		canonicalSchema, err = canonicaljson.Bytes(canonicalOwner)
		if err != nil {
			return fmt.Errorf("compiled authored event %s canonical dataset schema: %w", event.EventName(), err)
		}
	}
	declaration := DurableDataDeclaration{
		Name: event.EventName(), Ref: ref, OwnerFlowID: strings.TrimSpace(event.Source().FlowID), BusinessKey: businessKey,
		Schema: schema, CanonicalSchema: canonicalSchema, SchemaDigest: durabledata.SchemaDigestFor(canonicalSchema),
		SourceFile: event.Source().File,
	}
	if existing, duplicate := bundle.dataDeclarations[ref.Key()]; duplicate {
		return fmt.Errorf("compiled authored event dataset identity %s has multiple owners in %s and %s", ref.Key(), existing.SourceFile, declaration.SourceFile)
	}
	bundle.dataDeclarations[ref.Key()] = declaration
	return nil
}

func validateAgentDurableDataAccess(bundle *WorkflowContractBundle) error {
	bundle.resourceDataAccess = make(map[string][]durabledata.DeclarationRef)
	for _, record := range bundle.AgentDeclarationRecords() {
		seen := map[string]struct{}{}
		for _, access := range record.Entry.DataAccess {
			packageKey := strings.TrimSpace(access.Package)
			if packageKey == "" {
				packageKey = strings.TrimSpace(record.Source.PackageKey)
			}
			declaration, ok := bundle.DurableDataDeclarationByName(packageKey, access.Data)
			if !ok {
				return fmt.Errorf("agent %s data_access resource %q is not declared in package %s", record.LogicalID, access.Data, packageKey)
			}
			if _, duplicate := seen[declaration.Ref.Key()]; duplicate {
				return fmt.Errorf("agent %s repeats data_access declaration %s", record.LogicalID, declaration.Ref.Key())
			}
			seen[declaration.Ref.Key()] = struct{}{}
			key := staticAgentDeclarationKey(record.Source.PackageKey, record.OwnerFlowID, record.LogicalID)
			bundle.resourceDataAccess[key] = append(bundle.resourceDataAccess[key], declaration.Ref)
		}
		key := staticAgentDeclarationKey(record.Source.PackageKey, record.OwnerFlowID, record.LogicalID)
		sort.Slice(bundle.resourceDataAccess[key], func(i, j int) bool {
			return durabledata.CompareDeclarationRef(bundle.resourceDataAccess[key][i], bundle.resourceDataAccess[key][j]) < 0
		})
	}
	return nil
}

func (b *WorkflowContractBundle) DurableDataForAgent(packageKey, flowID, logicalID string) []durabledata.DeclarationRef {
	if b == nil {
		return nil
	}
	return append([]durabledata.DeclarationRef(nil), b.resourceDataAccess[staticAgentDeclarationKey(packageKey, flowID, logicalID)]...)
}

func (b *WorkflowContractBundle) DataProjectionRequired() bool {
	if b == nil {
		return false
	}
	for _, values := range b.staticDataAccess {
		if len(values) > 0 {
			return true
		}
	}
	for _, values := range b.resourceDataAccess {
		if len(values) > 0 {
			return true
		}
	}
	return false
}

func (b *WorkflowContractBundle) DurableDataDeclarations() []DurableDataDeclaration {
	if b == nil {
		return nil
	}
	out := make([]DurableDataDeclaration, 0, len(b.dataDeclarations))
	for _, declaration := range b.dataDeclarations {
		declaration.Schema = cloneEventSchemaMap(declaration.Schema)
		declaration.CanonicalSchema = append([]byte(nil), declaration.CanonicalSchema...)
		out = append(out, declaration)
	}
	sort.Slice(out, func(i, j int) bool { return durabledata.CompareDeclarationRef(out[i].Ref, out[j].Ref) < 0 })
	return out
}

func (b *WorkflowContractBundle) DurableDataDeclarationByName(packageKey, name string) (DurableDataDeclaration, bool) {
	if b == nil {
		return DurableDataDeclaration{}, false
	}
	for _, declaration := range b.dataDeclarations {
		if declaration.Ref.PackageKey == strings.TrimSpace(packageKey) && declaration.Name == strings.TrimSpace(name) {
			declaration.Schema = cloneEventSchemaMap(declaration.Schema)
			declaration.CanonicalSchema = append([]byte(nil), declaration.CanonicalSchema...)
			return declaration, true
		}
	}
	return DurableDataDeclaration{}, false
}

func (b *WorkflowContractBundle) DurableDataDeclarationByRef(ref durabledata.DeclarationRef) (DurableDataDeclaration, bool) {
	if b == nil {
		return DurableDataDeclaration{}, false
	}
	declaration, ok := b.dataDeclarations[ref.Key()]
	if !ok {
		return DurableDataDeclaration{}, false
	}
	declaration.Schema = cloneEventSchemaMap(declaration.Schema)
	declaration.CanonicalSchema = append([]byte(nil), declaration.CanonicalSchema...)
	return declaration, true
}
