package sourceartifact

import (
	"errors"
	"fmt"

	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
)

var ErrRootAdmissionMissing = errors.New("source root admission is unavailable")
var ErrRootAdmissionAmbiguous = errors.New("source root admission is ambiguous")

// RootAdmission is frozen root evidence, separate from byte-content identity.
type RootAdmission struct {
	bundleHash    string
	rootDirectory string
}

func (a RootAdmission) BundleHash() string    { return a.bundleHash }
func (a RootAdmission) RootDirectory() string { return a.rootDirectory }
func (a RootAdmission) Equal(other RootAdmission) bool {
	return a == other
}

// RootAdmissionReference is a key, not evidence that a root was admitted.
type RootAdmissionReference struct {
	BundleHash    string `json:"bundle_hash"`
	RootDirectory string `json:"root_directory"`
}

func (a RootAdmission) Reference() RootAdmissionReference {
	return RootAdmissionReference{BundleHash: a.bundleHash, RootDirectory: a.rootDirectory}
}

func (r RootAdmissionReference) Validate() error {
	if err := ValidateHash(r.BundleHash); err != nil {
		return fmt.Errorf("root admission reference bundle_hash: %w", err)
	}
	return validateRootDirectory(r.RootDirectory)
}

func (a RootAdmission) Validate() error {
	if a == (RootAdmission{}) {
		return ErrRootAdmissionMissing
	}
	if err := ValidateHash(a.bundleHash); err != nil {
		return fmt.Errorf("root admission bundle_hash: %w", err)
	}
	return validateRootDirectory(a.rootDirectory)
}

func validateRootDirectory(directory string) error {
	if err := runtimepaths.ValidateFlowSegment(directory); err != nil {
		return fmt.Errorf("folder %q is not an identifier; rename it: %w", directory, err)
	}
	return nil
}

// RootedSource pairs owned member bytes with one exact admitted root.
type RootedSource struct {
	artifact  *AdmittedSourceArtifact
	admission RootAdmission
}

func (s RootedSource) Artifact() *AdmittedSourceArtifact { return s.artifact }
func (s RootedSource) Admission() RootAdmission          { return s.admission }

func (s RootedSource) Validate() error {
	if err := s.admission.Validate(); err != nil {
		return err
	}
	if s.artifact == nil || s.artifact.BundleHash() != s.admission.BundleHash() {
		return fmt.Errorf("root admission does not match its exact source artifact")
	}
	if s.artifact.rootAdmission != (RootAdmission{}) && !s.artifact.rootAdmission.Equal(s.admission) {
		return fmt.Errorf("root admission contradicts the captured source root")
	}
	return nil
}

func (a *AdmittedSourceArtifact) RootAdmission() (RootAdmission, bool) {
	if a == nil || a.rootAdmission.Validate() != nil || a.BundleHash() != a.rootAdmission.BundleHash() {
		return RootAdmission{}, false
	}
	return a.rootAdmission, true
}

func (a *AdmittedSourceArtifact) RootedSource() (RootedSource, error) {
	admission, present := a.RootAdmission()
	if !present {
		return RootedSource{}, ErrRootAdmissionMissing
	}
	rooted := RootedSource{artifact: a, admission: admission}
	if err := rooted.Validate(); err != nil {
		return RootedSource{}, err
	}
	return rooted, nil
}
