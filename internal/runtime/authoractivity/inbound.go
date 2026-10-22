package authoractivity

import (
	"context"
)

type InboundProjection struct {
	SubjectType string
	SubjectID   string
}

type inboundProjectionContextKey struct{}

func InboundProjectionFromContext(ctx context.Context) (InboundProjection, bool) {
	if ctx == nil {
		return InboundProjection{}, false
	}
	projection, ok := ctx.Value(inboundProjectionContextKey{}).(InboundProjection)
	return projection, ok
}
