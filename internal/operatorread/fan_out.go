package operatorread

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

type FanOutReader interface {
	ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
}
