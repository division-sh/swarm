package authoractivity

import (
	"context"
	"strings"
)

type InboundProjection struct {
	SubjectType string
	SubjectID   string
	Summary     string
}

type inboundProjectionContextKey struct{}

func WithInboundProjection(ctx context.Context, projection InboundProjection) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	projection.SubjectType = strings.TrimSpace(projection.SubjectType)
	projection.SubjectID = strings.TrimSpace(projection.SubjectID)
	projection.Summary = strings.TrimSpace(projection.Summary)
	return context.WithValue(ctx, inboundProjectionContextKey{}, projection)
}

func InboundProjectionFromContext(ctx context.Context) (InboundProjection, bool) {
	if ctx == nil {
		return InboundProjection{}, false
	}
	projection, ok := ctx.Value(inboundProjectionContextKey{}).(InboundProjection)
	return projection, ok
}
