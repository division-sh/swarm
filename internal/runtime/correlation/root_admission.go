package correlation

import (
	"context"
	"fmt"

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

// CheckRootAdmissionContext never supplies missing admission. Root-independent
// callers may have no root context; a supplied root must agree exactly.
func CheckRootAdmissionContext(ctx context.Context, reference sourceartifact.RootAdmissionReference) error {
	if err := reference.Validate(); err != nil {
		return err
	}
	if ctx == nil {
		return nil
	}
	admission, supplied := ctx.Value(rootAdmissionContextKey{}).(sourceartifact.RootAdmission)
	if !supplied {
		return nil
	}
	if err := admission.Validate(); err != nil {
		return err
	}
	if _, valid := RootAdmissionFromContext(ctx); !valid || admission.Reference() != reference {
		return fmt.Errorf("rooted execution context does not match the exact run source root")
	}
	return nil
}
