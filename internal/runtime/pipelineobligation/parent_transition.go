package pipelineobligation

import (
	"context"
	"strings"
)

type publicationRunKey struct{}

// WithPublicationRun carries the admitted event's run to its exact claim.
// An empty run is intentional for run-less platform publications.
func WithPublicationRun(ctx context.Context, runID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, publicationRunKey{}, strings.TrimSpace(runID))
}

func PublicationRunFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	runID, ok := ctx.Value(publicationRunKey{}).(string)
	return runID, ok
}

// ParentTransition excludes new claims and joins admitted claims for one run
// until the owning transition has settled.
type ParentTransition interface{ Done() }

// ParentTransitionOwner retains stop's recovery-only drain and fork's
// recovery-plus-publication drain on the same selected-store claim owner.
type ParentTransitionOwner interface {
	BeginParentTransition(context.Context, string) (ParentTransition, error)
	BeginSourceForkTransition(context.Context, string) (ParentTransition, error)
}
