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

func RootAdmissionFromContext(ctx context.Context) (sourceartifact.RootAdmission, bool, error) {
	if ctx == nil {
		return sourceartifact.RootAdmission{}, false, nil
	}
	admission, ok := ctx.Value(rootAdmissionContextKey{}).(sourceartifact.RootAdmission)
	if !ok {
		return sourceartifact.RootAdmission{}, false, nil
	}
	if err := admission.Validate(); err != nil {
		return sourceartifact.RootAdmission{}, false, err
	}
	fact, hasFact := SourceArtifactFactFromContext(ctx)
	if !hasFact || admission.BundleHash() != fact.BundleHash() {
		return sourceartifact.RootAdmission{}, false, fmt.Errorf("rooted context does not match its content fact")
	}
	return admission, true, nil
}

// CheckRootAdmissionContext never supplies missing admission. Root-independent
// callers may have no root context; a supplied root must agree exactly.
func CheckRootAdmissionContext(ctx context.Context, reference sourceartifact.RootAdmissionReference) error {
	if err := reference.Validate(); err != nil {
		return err
	}
	admission, supplied, err := RootAdmissionFromContext(ctx)
	if err != nil {
		return err
	}
	if !supplied {
		return nil
	}
	if admission.Reference() != reference {
		return fmt.Errorf("rooted execution context does not match the exact run source root")
	}
	return nil
}
