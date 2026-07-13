package sealedpackage

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// Options toggles package-boundary variants on top of the canonical parent-connect route.
type Options struct {
	OmitConsumerInputBind      bool
	OmitConsumerPolicyBind     bool
	OmitConsumerCredentialBind bool
	ForbiddenSiblingWildcard   bool
	InvalidConnectReceiver     bool
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	root := Write(t, opts)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t),
		root,
		runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func Write(t testing.TB, opts Options) string {
	// routing-example-census: different-concept issue=none owner=sealed_flow_package_bindings proof=internal/runtime/conformance/sealed_flow_package_conformance_test.go:TestSealedFlowPackageConformance_CoversBoundaryOwners
	t.Helper()
	return canonicalrouting.CopySealedParentConnect(t, canonicalrouting.SealedParentConnectOptions{
		OmitConsumerInputBind:      opts.OmitConsumerInputBind,
		OmitConsumerPolicyBind:     opts.OmitConsumerPolicyBind,
		OmitConsumerCredentialBind: opts.OmitConsumerCredentialBind,
		ForbiddenSiblingWildcard:   opts.ForbiddenSiblingWildcard,
		InvalidConnectReceiver:     opts.InvalidConnectReceiver,
	})
}
