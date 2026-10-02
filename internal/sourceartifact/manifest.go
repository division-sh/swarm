package sourceartifact

import (
	"fmt"
	"strings"
	"unicode"

	semver "github.com/Masterminds/semver/v3"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/yamlsource"
)

const maxManifestFieldBytes = 256

// Manifest is source metadata, never flow, execution or distribution authority.
type Manifest struct {
	Name            string `json:"name"`
	Version         string `json:"version"`
	PlatformVersion string `json:"platform_version"`
	source          yamlsource.Value
}

func (a *AdmittedSourceArtifact) Manifest(label string) (Manifest, bool) {
	if a == nil {
		return Manifest{}, false
	}
	manifest, ok := a.manifests[label]
	return manifest, ok
}

func (a *AdmittedSourceArtifact) RootManifest() (Manifest, bool) {
	return a.Manifest("manifest.yaml")
}

func (a *AdmittedSourceArtifact) ValidatePlatformVersion(version string) error {
	manifest, present := a.RootManifest()
	if !present {
		return nil
	}
	if err := platform.ValidateProductPlatformVersion(manifest.PlatformVersion, version); err != nil {
		field, lookupErr := manifest.source.Lookup("platform_version")
		if lookupErr != nil {
			return lookupErr
		}
		return manifestError(field.Value, err.Error())
	}
	return nil
}

func projectManifest(root yamlsource.Value) (Manifest, error) {
	if err := root.ValidateExpansion(); err != nil {
		return Manifest{}, err
	}
	entries, err := root.Mapping()
	if err != nil {
		return Manifest{}, err
	}
	fields := make(map[string]yamlsource.Value, len(entries))
	for _, entry := range entries {
		if entry.KeyTag != "!!str" {
			return Manifest{}, manifestError(entry.Value, "manifest field name must be text")
		}
		switch entry.Name {
		case "name", "version", "platform_version":
		default:
			return Manifest{}, manifestError(entry.Value, "unknown manifest field "+entry.Name+"; allowed: name, version, platform_version")
		}
		if _, duplicate := fields[entry.Name]; duplicate {
			return Manifest{}, manifestError(entry.Value, "duplicate manifest field "+entry.Name)
		}
		fields[entry.Name] = entry.Value
	}
	manifest := Manifest{source: root}
	for _, field := range []struct {
		name   string
		target *string
	}{{"name", &manifest.Name}, {"version", &manifest.Version}, {"platform_version", &manifest.PlatformVersion}} {
		value, present := fields[field.name]
		if !present {
			missing, lookupErr := root.Lookup(field.name)
			if lookupErr != nil {
				return Manifest{}, lookupErr
			}
			return Manifest{}, manifestError(missing.Value, "required manifest field is missing")
		}
		scalar, scalarErr := value.Scalar()
		if scalarErr != nil || scalar.Tag != "!!str" || strings.TrimSpace(scalar.Value) == "" {
			return Manifest{}, manifestError(value, "manifest field requires nonempty text")
		}
		if len(scalar.Value) > maxManifestFieldBytes {
			return Manifest{}, manifestError(value, fmt.Sprintf("manifest field exceeds %d UTF-8 bytes", maxManifestFieldBytes))
		}
		*field.target = scalar.Value
	}
	if _, err := semver.StrictNewVersion(manifest.Version); err != nil {
		return Manifest{}, manifestError(fields["version"], "manifest version requires strict SemVer: "+err.Error())
	}
	if err := platform.ValidateProductPlatformRange(manifest.PlatformVersion); err != nil {
		return Manifest{}, manifestError(fields["platform_version"], err.Error())
	}
	return manifest, nil
}

func manifestError(value yamlsource.Value, message string) error {
	location := value.IntroductionLocation().String()
	if resolved := value.ResolvedLocation(); resolved != value.IntroductionLocation() {
		location += " (resolved at " + resolved.String() + ")"
	}
	return fmt.Errorf("%s at %s: %s", value.SemanticPath(), location, message)
}

// HumanLabel is presentation only. Decode never invents a filesystem basename.
func (a *AdmittedSourceArtifact) HumanLabel() string {
	if a == nil {
		return ""
	}
	if manifest, present := a.RootManifest(); present {
		return humanSourceLabel(manifest.Name + "@" + manifest.Version)
	}
	short := ShortHashLabel(a.BundleHash())
	if a.directoryLabel != "" {
		return humanSourceLabel(a.directoryLabel + "@" + short)
	}
	return short
}

func humanSourceLabel(label string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '\u2028' || r == '\u2029' {
			return ' '
		}
		return r
	}, strings.TrimSpace(label))
}

// ShortHashLabel is never a selector or machine identity.
func ShortHashLabel(hash string) string {
	if ValidateHash(hash) != nil {
		return "unavailable"
	}
	return strings.TrimPrefix(hash, HashPrefix)[:7]
}
