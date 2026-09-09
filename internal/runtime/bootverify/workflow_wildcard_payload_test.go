package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestLocalWildcardPayloadReaderConsumesExactProducerSchema(t *testing.T) {
	source := wildcardPayloadProofSource(t, canonicalrouting.LocalWildcardPayloadValid)
	report := Run(context.Background(), source, Options{})
	if errors := report.Errors(); len(errors) != 0 {
		t.Fatalf("wildcard payload verification: %#v", errors)
	}
	resolved, err := executablePayloadStructuralType(source, identitytest.FlowNode(t, "worker", "observer"), "task.*")
	if err != nil {
		t.Fatal(err)
	}
	field, ok := resolved.Field("work_id")
	if !ok || field.IsOptional || field.TypeRef != "text" {
		t.Fatalf("wildcard work_id = %#v, exists=%t", field, ok)
	}
}

func TestLocalWildcardPayloadReaderRejectsMissingAndIncompatibleProducerSchemas(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		variant    canonicalrouting.LocalWildcardPayloadVariant
	}{
		{"missing finite expansion", "no finite producer schema expansion", canonicalrouting.LocalWildcardPayloadMissing},
		{"incompatible type", "incompatible producer schemas", canonicalrouting.LocalWildcardPayloadIncompatibleType},
		{"incompatible presence", "incompatible producer schemas", canonicalrouting.LocalWildcardPayloadIncompatiblePresence},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := Run(context.Background(), wildcardPayloadProofSource(t, tc.variant), Options{})
			if !reportContains(report.Errors(), "condition_expression_validation", tc.want) {
				t.Fatalf("verification errors = %#v, want %q", report.Errors(), tc.want)
			}
		})
	}
}

func wildcardPayloadProofSource(t *testing.T, variant canonicalrouting.LocalWildcardPayloadVariant) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyLocalWildcardPayload(t, variant)
	repo := repoRootForBootverifyTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load wildcard proof: %v", err)
	}
	return semanticview.Wrap(bundle)
}
