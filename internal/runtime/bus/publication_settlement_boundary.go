package bus

import "context"

// This stack-owned boundary can flush completed predecessors, but cannot add
// members, acquire a claim, or authorize the nested publication itself.
type publicationSettlementBoundary interface {
	flushBeforeNestedPublication() error
}

func flushEnclosingPublicationSettlement(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	scope, _ := ctx.Value(deliveryDispatchScopeKey{}).(*deliveryDispatchScope)
	if scope == nil || scope.publicationSettlement == nil {
		return nil
	}
	return scope.publicationSettlement.flushBeforeNestedPublication()
}
