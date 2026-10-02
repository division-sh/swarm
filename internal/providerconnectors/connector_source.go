package providerconnectors

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func parseConnectorManifestAt(body []byte, file string) (ConnectorManifest, error) {
	root, err := connectorSource(body, file)
	if err != nil {
		return ConnectorManifest{}, err
	}
	fields, err := connectorFields(root, "provider", "tools", "generation")
	if err != nil {
		return ConnectorManifest{}, err
	}
	out := ConnectorManifest{source: root}
	if err := connectorTextFields(root, fields, map[string]*string{"provider": &out.Provider}); err != nil {
		return out, err
	}
	tools, err := connectorRequired(root, fields, "tools")
	if err != nil {
		return out, err
	}
	if out.Tools, err = runtimecontracts.AdmitToolDeclarationsValue(tools); err != nil {
		return out, err
	}
	if len(out.Tools) == 0 {
		return out, connectorError(tools, "tools are required")
	}
	if value, present := fields["generation"]; present {
		generation, err := admitGenerationEvidence(value)
		if err != nil {
			return out, err
		}
		out.Generation = &generation
	}
	return out, nil
}

func admitGenerationEvidence(value yamlsource.Value) (GenerationEvidence, error) {
	fields, err := connectorFields(value, "schema_version", "generator_version", "source", "profile", "operations")
	if err != nil {
		return GenerationEvidence{}, err
	}
	var out GenerationEvidence
	if err := connectorTextFields(value, fields, map[string]*string{"schema_version": &out.SchemaVersion, "generator_version": &out.GeneratorVersion}); err != nil {
		return out, err
	}
	source, err := connectorRequired(value, fields, "source")
	if err != nil {
		return out, err
	}
	members, err := connectorFields(source, "path", "openapi_version", "sha256")
	if err != nil {
		return out, err
	}
	if err := connectorTextFields(source, members, map[string]*string{"path": &out.Source.Path, "openapi_version": &out.Source.OpenAPIVersion, "sha256": &out.Source.SHA256}); err != nil {
		return out, err
	}
	profile, err := connectorRequired(value, fields, "profile")
	if err != nil {
		return out, err
	}
	members, err = connectorFields(profile, "path", "schema_version", "sha256")
	if err != nil {
		return out, err
	}
	if err := connectorTextFields(profile, members, map[string]*string{"path": &out.Profile.Path, "schema_version": &out.Profile.SchemaVersion, "sha256": &out.Profile.SHA256}); err != nil {
		return out, err
	}
	operations, err := connectorRows(value, fields, "operations")
	if err != nil {
		return out, err
	}
	out.Operations = make([]GenerationOperationEvidence, len(operations))
	for i, row := range operations {
		members, err := connectorFields(row, "operation_id", "tool_id", "permissions", "response_success", "fixture_id", "fixture_status", "review_status")
		if err != nil {
			return out, err
		}
		op := &out.Operations[i]
		if err := connectorTextFields(row, members, map[string]*string{"operation_id": &op.OperationID, "tool_id": &op.ToolID, "fixture_id": &op.FixtureID, "fixture_status": &op.FixtureStatus, "review_status": &op.ReviewStatus}); err != nil {
			return out, err
		}
		if op.Permissions, err = connectorPermissions(row, members); err != nil {
			return out, err
		}
		success, err := connectorRequired(row, members, "response_success")
		if err != nil {
			return out, err
		}
		if op.ResponseSuccess, err = runtimecontracts.ProjectToolResponseSuccessValue(success); err != nil {
			return out, err
		}
	}
	return out, nil
}

func admitGeneratedPackIndex(body []byte, file string) (GeneratedPackIndex, error) {
	root, err := connectorSource(body, file)
	if err != nil {
		return GeneratedPackIndex{}, err
	}
	fields, err := connectorFields(root, "schema_version", "packs")
	if err != nil {
		return GeneratedPackIndex{}, err
	}
	out := GeneratedPackIndex{source: root}
	if err := connectorTextFields(root, fields, map[string]*string{"schema_version": &out.SchemaVersion}); err != nil {
		return out, err
	}
	rows, err := connectorRows(root, fields, "packs")
	if err != nil {
		return out, err
	}
	out.Packs = make([]GeneratedPackIndexEntry, len(rows))
	for i, row := range rows {
		members, err := connectorFields(row, "id", "provider", "profile", "kind", "output")
		if err != nil {
			return out, err
		}
		entry := &out.Packs[i]
		if err := connectorTextFields(row, members, map[string]*string{"id": &entry.ID, "provider": &entry.Provider, "profile": &entry.Profile, "kind": &entry.Kind, "output": &entry.Output}); err != nil {
			return out, err
		}
	}
	return out, nil
}

func connectorSource(body []byte, file string) (yamlsource.Value, error) {
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return yamlsource.Value{}, err
	}
	root := snapshot.Document(file).Root()
	return root, root.ValidateExpansion()
}

func connectorFields(value yamlsource.Value, allowed ...string) (map[string]yamlsource.Value, error) {
	entries, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]yamlsource.Value, len(entries))
	for _, entry := range entries {
		if entry.KeyTag != "!!str" || strings.TrimSpace(entry.Name) == "" {
			return nil, connectorError(entry.Value, "field name must be nonempty text")
		}
		if _, duplicate := out[entry.Name]; duplicate {
			return nil, connectorError(entry.Value, "duplicate field "+entry.Name)
		}
		known := false
		for _, name := range allowed {
			known = known || entry.Name == name
		}
		if !known {
			return nil, connectorError(entry.Value, "field "+entry.Name+" not found in connector declaration")
		}
		out[entry.Name] = entry.Value
	}
	return out, nil
}

func connectorRequired(parent yamlsource.Value, fields map[string]yamlsource.Value, name string) (yamlsource.Value, error) {
	if value, present := fields[name]; present {
		return value, nil
	}
	missing, err := parent.Lookup(name)
	if err != nil {
		return yamlsource.Value{}, err
	}
	return missing.Value, fmt.Errorf("%s required field is missing from mapping at %s", missing.Value.SemanticPath(), parent.Location())
}

func connectorTextFields(parent yamlsource.Value, fields map[string]yamlsource.Value, targets map[string]*string) error {
	names := make([]string, 0, len(targets))
	for name := range targets {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value, err := connectorRequired(parent, fields, name)
		if err != nil {
			return err
		}
		if *targets[name], err = connectorText(value); err != nil {
			return err
		}
	}
	return nil
}

func connectorText(value yamlsource.Value) (string, error) {
	scalar, err := value.Scalar()
	if err != nil || scalar.Tag != "!!str" || strings.TrimSpace(scalar.Value) == "" {
		return "", connectorError(value, "want nonempty text")
	}
	return scalar.Value, nil
}

func connectorRows(parent yamlsource.Value, fields map[string]yamlsource.Value, name string) ([]yamlsource.Value, error) {
	value, err := connectorRequired(parent, fields, name)
	if err != nil {
		return nil, err
	}
	return value.Sequence()
}

func connectorTextList(value yamlsource.Value) ([]string, error) {
	rows, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rows))
	for i, row := range rows {
		if out[i], err = connectorText(row); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func connectorPermissions(parent yamlsource.Value, fields map[string]yamlsource.Value) ([]GenerationPermission, error) {
	rows, err := connectorRows(parent, fields, "permissions")
	if err != nil {
		return nil, err
	}
	out := make([]GenerationPermission, len(rows))
	for i, row := range rows {
		members, err := connectorFields(row, "id", "note")
		if err != nil {
			return nil, err
		}
		if err := connectorTextFields(row, members, map[string]*string{"id": &out[i].ID, "note": &out[i].Note}); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func connectorBool(value yamlsource.Value) (bool, error) {
	scalar, err := value.Scalar()
	if err != nil || scalar.Tag != "!!bool" {
		return false, connectorError(value, "want boolean")
	}
	var out bool
	if err := value.Project(&out); err != nil {
		return false, err
	}
	return out, nil
}

func connectorError(value yamlsource.Value, message string) error {
	return fmt.Errorf("%s at %s: %s", value.SemanticPath(), value.Location(), message)
}
