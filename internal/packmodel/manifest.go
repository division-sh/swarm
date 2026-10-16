package packmodel

import (
	"fmt"
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/division-sh/swarm/internal/platform"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/yamlsource"
)

const (
	ManifestFileName      = "manifest.yaml"
	MaxManifestFieldBytes = 256
)

type DirectoryKind uint8

const (
	DirectoryFlow DirectoryKind = iota + 1
	DirectoryProvider
)

func (k DirectoryKind) String() string {
	switch k {
	case DirectoryFlow:
		return "flow"
	case DirectoryProvider:
		return "provider"
	default:
		return "invalid"
	}
}

// Manifest is shared metadata admitted against a file-derived directory kind.
type Manifest struct {
	Name            string `json:"name"`
	Version         string `json:"version"`
	PlatformVersion string `json:"platform_version"`

	source       yamlsource.Value
	kind         DirectoryKind
	capabilities Capabilities
}

func (m Manifest) SourceValue() yamlsource.Value { return m.source }
func (m Manifest) Kind() DirectoryKind           { return m.kind }

func (m Manifest) Capabilities() (Capabilities, bool) {
	if m.kind != DirectoryProvider {
		return Capabilities{}, false
	}
	out := m.capabilities
	out.Can.EmitEvents = append([]string(nil), out.Can.EmitEvents...)
	out.Can.CallProviderActions = append([]string(nil), out.Can.CallProviderActions...)
	out.Cannot = append([]string(nil), out.Cannot...)
	return out, true
}

func (m Manifest) ValidatePlatformVersion(version string) error {
	if err := platform.ValidateProductPlatformVersion(m.PlatformVersion, version); err != nil {
		field, lookupErr := m.source.Lookup("platform_version")
		if lookupErr != nil {
			return lookupErr
		}
		return manifestError(field.Value, err.Error())
	}
	return nil
}

// ValidateBodyHeader checks shared envelope ownership, not the role vocabulary.
func (m Manifest) ValidateBodyHeader(root yamlsource.Value) error {
	if m.kind != DirectoryProvider {
		return manifestError(root, "provider body requires its admitted provider manifest")
	}
	if err := root.ValidateExpansion(); err != nil {
		return err
	}
	entries, err := root.Mapping()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name == "provider" {
			return manifestError(entry.Value, "provider is not a body key; the provider is name in manifest.yaml")
		}
	}
	return nil
}

func ParseManifestAt(body []byte, file string, kind DirectoryKind) (Manifest, error) {
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return Manifest{}, err
	}
	return ProjectManifest(snapshot.Document(file).Root(), kind)
}

func ProjectManifest(root yamlsource.Value, kind DirectoryKind) (Manifest, error) {
	if kind != DirectoryFlow && kind != DirectoryProvider {
		return Manifest{}, manifestError(root, "manifest requires an admitted directory kind")
	}
	if err := root.ValidateExpansion(); err != nil {
		return Manifest{}, err
	}
	fields, err := manifestFields(root, kind)
	if err != nil {
		return Manifest{}, err
	}
	out := Manifest{source: root, kind: kind}
	if err := admitManifestHeader(root, fields, &out); err != nil {
		return Manifest{}, err
	}
	if kind == DirectoryProvider {
		if err := runtimepaths.ValidateFlowSegment(out.Name); err != nil {
			return Manifest{}, manifestError(fields["name"], fmt.Sprintf("name %q is not an identifier: %s", out.Name, err))
		}
		value, present := fields["capabilities"]
		if !present {
			missing, err := root.Lookup("capabilities")
			if err != nil {
				return Manifest{}, err
			}
			return Manifest{}, manifestError(missing.Value, "required manifest field is missing")
		}
		if out.capabilities, err = admitManifestCapabilities(value); err != nil {
			return Manifest{}, err
		}
	}
	return out, nil
}

func manifestFields(root yamlsource.Value, kind DirectoryKind) (map[string]yamlsource.Value, error) {
	entries, err := root.Mapping()
	if err != nil {
		return nil, err
	}
	fields := make(map[string]yamlsource.Value, len(entries))
	for _, entry := range entries {
		if entry.KeyTag != "!!str" {
			return nil, manifestError(entry.Value, "manifest field name must be text")
		}
		if _, duplicate := fields[entry.Name]; duplicate {
			return nil, manifestError(entry.Value, "duplicate manifest field "+entry.Name)
		}
		switch entry.Name {
		case "name", "version", "platform_version":
		case "capabilities":
			if kind != DirectoryProvider {
				return nil, manifestError(entry.Value, "capabilities is only a provider manifest key")
			}
		default:
			return nil, retiredManifestField(entry.Name, entry.Value)
		}
		fields[entry.Name] = entry.Value
	}
	return fields, nil
}

func retiredManifestField(name string, value yamlsource.Value) error {
	switch name {
	case "type":
		return manifestError(value, "type is not a manifest key; a directory is a provider or a flow by the files it holds")
	case "requires":
		entries, err := value.Mapping()
		if err == nil {
			for _, entry := range entries {
				if entry.Name == "secrets" || entry.Name == "managed_credentials" {
					return retiredManifestField("requires."+entry.Name, entry.Value)
				}
			}
		}
		return manifestError(value, "requires is not a manifest key; credentials are declared on the action, trigger or channel that uses them")
	case "requires.secrets", "requires.managed_credentials":
		return manifestError(value, name+" is not a manifest key; credentials are declared on the action, trigger or channel that uses them")
	case "id", "implements", "manifest_hash", "provenance", "tests":
		return manifestError(value, name+" is not a manifest key; use name, version, platform_version and provider capabilities")
	default:
		return manifestError(value, "unknown manifest field "+name+"; allowed: name, version, platform_version and provider capabilities")
	}
}

func admitManifestHeader(root yamlsource.Value, fields map[string]yamlsource.Value, out *Manifest) error {
	for _, field := range []struct {
		name   string
		target *string
	}{{"name", &out.Name}, {"version", &out.Version}, {"platform_version", &out.PlatformVersion}} {
		value, present := fields[field.name]
		if !present {
			missing, err := root.Lookup(field.name)
			if err != nil {
				return err
			}
			return manifestError(missing.Value, "required manifest field is missing")
		}
		scalar, err := value.Scalar()
		if err != nil || scalar.Tag != "!!str" || strings.TrimSpace(scalar.Value) == "" {
			return manifestError(value, "manifest field requires nonempty text")
		}
		if len(scalar.Value) > MaxManifestFieldBytes {
			return manifestError(value, fmt.Sprintf("manifest field exceeds %d UTF-8 bytes", MaxManifestFieldBytes))
		}
		*field.target = scalar.Value
	}
	if _, err := semver.StrictNewVersion(out.Version); err != nil {
		return manifestError(fields["version"], "manifest version requires strict SemVer: "+err.Error())
	}
	if err := platform.ValidateProductPlatformRange(out.PlatformVersion); err != nil {
		return manifestError(fields["platform_version"], err.Error())
	}
	return nil
}

func admitManifestCapabilities(value yamlsource.Value) (Capabilities, error) {
	fields, err := envelopeFields(value, "can", "cannot")
	if err != nil {
		return Capabilities{}, err
	}
	if err := envelopeRequired(value, fields, "can", "cannot"); err != nil {
		return Capabilities{}, err
	}
	can, err := envelopeFields(fields["can"], "receive_https_route", "verify_secret", "emit_events", "persist_dedupe_markers", "call_provider_actions", "lower_through_activity", "journal_activity_attempts")
	if err != nil {
		return Capabilities{}, err
	}
	var out Capabilities
	if err := admitCanValues(can, &out); err != nil {
		return Capabilities{}, err
	}
	if out.Cannot, err = envelopeTexts(fields["cannot"]); err != nil {
		return Capabilities{}, err
	}
	if len(out.Cannot) == 0 {
		return Capabilities{}, manifestError(fields["cannot"], "capabilities.cannot is required and must be a nonempty text list")
	}
	for _, name := range []string{"emit_events", "call_provider_actions"} {
		if field, present := can[name]; present {
			values, err := field.Sequence()
			if err != nil || len(values) == 0 {
				return Capabilities{}, manifestError(field, "capability requires nonempty text list")
			}
		}
	}
	return out, nil
}

func manifestError(value yamlsource.Value, message string) error {
	location := value.IntroductionLocation().String()
	if resolved := value.ResolvedLocation(); resolved != value.IntroductionLocation() {
		location += " (resolved at " + resolved.String() + ")"
	}
	return fmt.Errorf("%s at %s: %s", value.SemanticPath(), location, message)
}
