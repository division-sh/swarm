package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

// BundleHash returns the exact identity of the immutable admitted source.
// Platform inputs and compiled projections are intentionally not source
// members and therefore cannot affect this value.
func BundleHash(bundle *WorkflowContractBundle) (string, error) {
	if bundle == nil || bundle.SourceArtifact == nil {
		return "", fmt.Errorf("workflow contract bundle has no admitted source artifact")
	}
	hash := bundle.SourceArtifact.BundleHash()
	if err := sourceartifact.ValidateHash(hash); err != nil {
		return "", err
	}
	return hash, nil
}

func IsBundleHash(value string) bool {
	return sourceartifact.ValidateHash(value) == nil
}

func ValidateBundleHash(value string) error {
	return sourceartifact.ValidateHash(value)
}
