package correlation_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1RootContextCarriesOnlyMatchingExistingEvidence(t *testing.T) {
	rooted, _ := sourceartifactfixture.Rooted(t, "shop-a", sourceartifactfixture.Artifact())
	fact := sourceartifactfixture.FactFor(rooted.Artifact())
	ctx := correlation.WithSourceArtifactFact(context.Background(), fact)
	if root, ok, err := correlation.RootAdmissionFromContext(ctx); ok || err != nil || root != (sourceartifact.RootAdmission{}) {
		t.Fatal("content context invented a root")
	}
	ctx = correlation.WithRootAdmission(ctx, rooted.Admission())
	got, ok, err := correlation.RootAdmissionFromContext(ctx)
	if err != nil || !ok || !got.Equal(rooted.Admission()) {
		t.Fatal("exact root context lost its existing admission")
	}
	for name, invalid := range map[string]context.Context{
		"nil":             nil,
		"no_content":      correlation.WithRootAdmission(nil, rooted.Admission()),
		"cleared_root":    correlation.WithRootAdmission(ctx, sourceartifact.RootAdmission{}),
		"changed_content": correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.FactFor(sourceartifactfixture.New("nodes.yaml", []byte("other: {type: procedure}\n")))),
	} {
		t.Run(name, func(t *testing.T) {
			root, ok, err := correlation.RootAdmissionFromContext(invalid)
			if ok || root != (sourceartifact.RootAdmission{}) || (err == nil) != (invalid == nil) {
				t.Fatal("invalid context reused stale root evidence")
			}
		})
	}
	second, _ := sourceartifactfixture.Rooted(t, "shop-b", rooted.Artifact())
	replaced := correlation.WithRootAdmission(ctx, second.Admission())
	if got, ok, err := correlation.RootAdmissionFromContext(replaced); err != nil || !ok || !got.Equal(second.Admission()) {
		t.Fatal("explicit same-content root replacement lost its exact pair")
	}
	if got, ok, err := correlation.RootAdmissionFromContext(ctx); err != nil || !ok || !got.Equal(rooted.Admission()) {
		t.Fatal("derived context changed the frozen parent evidence")
	}
}
