package correlation

import (
	"context"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

type rootAdmissionContextKey struct{}

// WithRootAdmission carries existing opaque admission alongside the independent
// content fact. Even a zero value replaces inherited evidence, failing closed.
func WithRootAdmission(ctx context.Context, admission sourceartifact.RootAdmission) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, rootAdmissionContextKey{}, admission)
}

func RootAdmissionFromContext(ctx context.Context) (sourceartifact.RootAdmission, bool) {
	if ctx == nil {
		return sourceartifact.RootAdmission{}, false
	}
	admission, ok := ctx.Value(rootAdmissionContextKey{}).(sourceartifact.RootAdmission)
	fact, hasFact := SourceArtifactFactFromContext(ctx)
	if !ok || admission.Validate() != nil || !hasFact || admission.BundleHash() != fact.BundleHash() {
		return sourceartifact.RootAdmission{}, false
	}
	return admission, true
}
