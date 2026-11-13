package channeldelivery

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

type linkNoticeBindingKey struct{}
type linkNoticeRequest struct {
	binding operatorchannel.Binding
	create  bool
}

var ErrLinkNoticeAbsent = errors.New("link notice has not been created")

// The expected binding is a comparison operand, never send authority.
func WithLinkNoticeBinding(ctx context.Context, binding operatorchannel.Binding, create bool) context.Context {
	return context.WithValue(ctx, linkNoticeBindingKey{}, linkNoticeRequest{binding, create})
}

func LinkNoticeBinding(ctx context.Context) (operatorchannel.Binding, bool, bool) {
	request, ok := ctx.Value(linkNoticeBindingKey{}).(linkNoticeRequest)
	return request.binding, ok, request.create
}
