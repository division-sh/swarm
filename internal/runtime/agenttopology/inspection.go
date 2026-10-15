package agenttopology

import "context"

// SourceSetHeadReader observes the persisted head without acquiring possession.
type SourceSetHeadReader interface {
	InspectSourceSetHead(context.Context) (SourceSetPlan, bool, error)
}
