package sourceartifact

import "fmt"

type RootedEnsureResult struct {
	Source   RootedSource
	Artifact EnsureResult
	Created  bool
}

// RestoreRootedSource is the selected-store root-row decoder. Its production
// callers are restricted to the source-artifact persistence owner.
func RestoreRootedSource(row Persisted, reference RootAdmissionReference) (RootedSource, error) {
	if err := reference.Validate(); err != nil {
		return RootedSource{}, err
	}
	if row.BundleHash != reference.BundleHash {
		return RootedSource{}, fmt.Errorf("retained root admission differs from its source artifact")
	}
	artifact, err := row.Decode()
	if err != nil {
		return RootedSource{}, err
	}
	admission := RootAdmission{bundleHash: reference.BundleHash, rootDirectory: reference.RootDirectory}
	return RootedSource{artifact: artifact, admission: admission}, nil
}
