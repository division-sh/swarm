package contracts

import (
	"fmt"
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
