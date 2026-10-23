package sourceartifact

import (
	"fmt"
	"path"
)

// ProviderKey resolves only captured membership. A selected provider root also
// needs frozen root evidence, which logical content alone does not contain.
func (a *AdmittedSourceArtifact) ProviderKey(directory string) (string, error) {
	admission, _ := a.RootAdmission()
	return a.providerKey(directory, admission)
}

func (s RootedSource) ProviderKey(directory string) (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	return s.artifact.providerKey(directory, s.admission)
}

func (a *AdmittedSourceArtifact) providerKey(directory string, admission RootAdmission) (string, error) {
	provider, present := a.ProviderDirectory(directory)
	if !present {
		return "", fmt.Errorf("source has no admitted provider directory %q", directory)
	}
	if provider.Path() != "." {
		return path.Base(provider.Path()), nil
	}
	if err := admission.Validate(); err != nil {
		return "", fmt.Errorf("provider root requires its captured directory identity: %w", err)
	}
	if admission.BundleHash() != a.BundleHash() {
		return "", fmt.Errorf("provider root admission does not match its source artifact")
	}
	return admission.RootDirectory(), nil
}
