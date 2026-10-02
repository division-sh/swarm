package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

type BundleIdentity struct {
	SourceLabel     string `json:"source_label"`
	WorkflowName    string `json:"workflow_name"`
	WorkflowVersion string `json:"workflow_version"`
	BundleHash      string `json:"bundle_hash"`
}

// SourceExecutionIdentity is the machine identity of an exact source artifact.
// Distribution metadata is deliberately not an executable workflow version.
func SourceExecutionIdentity(bundleHash string) (BundleIdentity, error) {
	if err := sourceartifact.ValidateHash(bundleHash); err != nil {
		return BundleIdentity{}, err
	}
	return BundleIdentity{WorkflowName: ".", WorkflowVersion: bundleHash, BundleHash: bundleHash}, nil
}

func BootBundleIdentity(bundle *WorkflowContractBundle) (BundleIdentity, error) {
	if bundle == nil {
		return BundleIdentity{}, fmt.Errorf("workflow contract bundle is required")
	}
	bundleHash, err := BundleHash(bundle)
	if err != nil {
		return BundleIdentity{}, err
	}
	return BundleIdentity{
		SourceLabel:     bundle.SourceArtifact.HumanLabel(),
		WorkflowName:    strings.TrimSpace(bundle.WorkflowName()),
		WorkflowVersion: strings.TrimSpace(bundle.WorkflowVersion()),
		BundleHash:      bundleHash,
	}, nil
}
