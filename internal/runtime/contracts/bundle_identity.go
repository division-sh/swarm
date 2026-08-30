package contracts

import (
	"fmt"
	"path/filepath"
	"strings"
)

type BundleIdentity struct {
	WorkflowName    string `json:"workflow_name"`
	WorkflowVersion string `json:"workflow_version"`
	BundleHash      string `json:"bundle_hash"`
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
		WorkflowName:    strings.TrimSpace(bundle.WorkflowName()),
		WorkflowVersion: strings.TrimSpace(bundle.WorkflowVersion()),
		BundleHash:      bundleHash,
	}, nil
}

func sameFilePath(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr == nil && rightErr == nil {
		return filepath.Clean(leftAbs) == filepath.Clean(rightAbs)
	}
	return filepath.Clean(left) == filepath.Clean(right)
}
