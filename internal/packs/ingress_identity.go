package packs

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
)

// An endpoint alias is presentation, never the identity of its declaration.
func IngressSubjectID(bundleHash, flowPath, provider string) (string, error) {
	if bundleHash == "" || bundleHash != strings.TrimSpace(bundleHash) {
		return "", fmt.Errorf("ingress subject requires exact source and provider identity")
	}
	if err := runtimepaths.ValidateFlowSegment(provider); err != nil {
		return "", fmt.Errorf("ingress subject requires an exact portable provider directory key: %w", err)
	}
	flow, err := identity.AdmitFlowIdentity(flowPath)
	if err != nil {
		return "", fmt.Errorf("ingress subject declaration: %w", err)
	}
	return "ingress:" + bundleHash + ":" + flow.String() + ":" + provider, nil
}
