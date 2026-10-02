package packartifact

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func admitInventoryManifest(body []byte, file string) (InventoryManifest, error) {
	root, fields, err := membershipRoot(body, file, "packs")
	if err != nil {
		return InventoryManifest{}, err
	}
	rows, err := fields["packs"].Sequence()
	if err != nil {
		return InventoryManifest{}, err
	}
	manifest := InventoryManifest{source: root, Version: 1, Packs: make([]InventoryManifestPack, len(rows))}
	for i, row := range rows {
		fields, err := membershipFields(row, "id", "type", "path")
		if err != nil {
			return InventoryManifest{}, err
		}
		if err := membershipRequired(row, fields, "id", "type", "path"); err != nil {
			return InventoryManifest{}, err
		}
		for _, field := range []struct {
			name   string
			target *string
		}{{"id", &manifest.Packs[i].ID}, {"type", &manifest.Packs[i].Type}, {"path", &manifest.Packs[i].Path}} {
			if *field.target, err = membershipText(fields[field.name]); err != nil {
				return InventoryManifest{}, err
			}
		}
	}
	return manifest, nil
}

func admitProjectManifest(body []byte, file string) (ProjectPackManifest, error) {
	root, fields, err := membershipRoot(body, file, "imports")
	if err != nil {
		return ProjectPackManifest{}, err
	}
	rows, err := fields["imports"].Sequence()
	if err != nil {
		return ProjectPackManifest{}, err
	}
	manifest := ProjectPackManifest{source: root, Version: 1, Imports: make([]ProjectPackManifestImport, len(rows))}
	for i, row := range rows {
		fields, err := membershipFields(row, "id", "type", "path", "origin")
		if err != nil {
			return ProjectPackManifest{}, err
		}
		if err := membershipRequired(row, fields, "id", "type", "path", "origin"); err != nil {
			return ProjectPackManifest{}, err
		}
		origin, err := membershipFields(fields["origin"], "source", "id", "version", "manifest_hash", "envelope_hash")
		if err != nil {
			return ProjectPackManifest{}, err
		}
		if err := membershipRequired(fields["origin"], origin, "source", "id", "version", "manifest_hash", "envelope_hash"); err != nil {
			return ProjectPackManifest{}, err
		}
		entry := &manifest.Imports[i]
		for _, field := range []struct {
			value  yamlsource.Value
			target *string
		}{
			{fields["id"], &entry.ID}, {fields["type"], &entry.Type}, {fields["path"], &entry.Path},
			{origin["source"], &entry.Origin.Source}, {origin["id"], &entry.Origin.ID}, {origin["version"], &entry.Origin.Version},
			{origin["manifest_hash"], &entry.Origin.ManifestHash}, {origin["envelope_hash"], &entry.Origin.EnvelopeHash},
		} {
			if *field.target, err = membershipText(field.value); err != nil {
				return ProjectPackManifest{}, err
			}
		}
	}
	return manifest, nil
}

func membershipRoot(body []byte, file, collection string) (yamlsource.Value, map[string]yamlsource.Value, error) {
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return yamlsource.Value{}, nil, err
	}
	root := snapshot.Document(file).Root()
	if err := root.ValidateExpansion(); err != nil {
		return root, nil, err
	}
	fields, err := membershipFields(root, "version", collection)
	if err != nil {
		return root, nil, err
	}
	if err := membershipRequired(root, fields, "version", collection); err != nil {
		return root, nil, err
	}
	version := fields["version"]
	scalar, err := version.Scalar()
	if err != nil || scalar.Tag != "!!int" {
		return root, nil, membershipError(version, "version must be integer 1")
	}
	var number int
	if err := version.Project(&number); err != nil {
		return root, nil, err
	}
	if number != 1 {
		return root, nil, membershipError(version, "unsupported version; want 1")
	}
	return root, fields, nil
}

func membershipFields(value yamlsource.Value, allowed ...string) (map[string]yamlsource.Value, error) {
	entries, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]yamlsource.Value, len(entries))
	for _, entry := range entries {
		if entry.KeyTag != "!!str" {
			return nil, membershipError(entry.Value, "field name must be text")
		}
		if _, duplicate := out[entry.Name]; duplicate {
			return nil, membershipError(entry.Value, "duplicate field "+entry.Name)
		}
		known := false
		for _, name := range allowed {
			known = known || name == entry.Name
		}
		if !known {
			return nil, membershipError(entry.Value, "unsupported field "+entry.Name)
		}
		out[entry.Name] = entry.Value
	}
	return out, nil
}

func membershipRequired(parent yamlsource.Value, fields map[string]yamlsource.Value, names ...string) error {
	for _, name := range names {
		if _, present := fields[name]; !present {
			missing, err := parent.Lookup(name)
			if err != nil {
				return err
			}
			return fmt.Errorf("%s required field is missing from mapping at %s", missing.Value.SemanticPath(), parent.Location())
		}
	}
	return nil
}

func membershipText(value yamlsource.Value) (string, error) {
	scalar, err := value.Scalar()
	if err != nil || scalar.Tag != "!!str" || strings.TrimSpace(scalar.Value) == "" {
		return "", membershipError(value, "want nonempty text")
	}
	return scalar.Value, nil
}

func membershipError(value yamlsource.Value, message string) error {
	return fmt.Errorf("%s at %s: %s", value.SemanticPath(), value.Location(), message)
}
