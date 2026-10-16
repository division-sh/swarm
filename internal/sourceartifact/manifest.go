package sourceartifact

import (
	"strings"
	"unicode"

	"github.com/division-sh/swarm/internal/packmodel"
)

type Manifest = packmodel.Manifest

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
	return manifest.ValidatePlatformVersion(version)
}

// HumanLabel is presentation only. Decode never invents a filesystem basename.
func (a *AdmittedSourceArtifact) HumanLabel() string {
	if a == nil {
		return ""
	}
	if admission, present := a.RootAdmission(); present {
		return a.humanLabelForDirectory(admission.RootDirectory())
	}
	return ShortHashLabel(a.BundleHash())
}

func (s RootedSource) HumanLabel() string {
	if s.Validate() != nil {
		return ""
	}
	return s.artifact.humanLabelForDirectory(s.admission.RootDirectory())
}

func (a *AdmittedSourceArtifact) humanLabelForDirectory(directory string) string {
	version := ShortHashLabel(a.BundleHash())
	if manifest, present := a.RootManifest(); present {
		version = manifest.Version
	}
	return humanSourceLabel(directory + "@" + version)
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
