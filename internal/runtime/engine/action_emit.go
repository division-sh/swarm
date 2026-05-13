package engine

import "context"

type actionEmitIntentCollectorKey struct{}

// WithActionEmitIntentCollector lets built-in action runners contribute
// runtime-owned result events to the executor's transactional outbox.
func WithActionEmitIntentCollector(ctx context.Context, collector *[]EmitIntent) context.Context {
	if collector == nil {
		return ctx
	}
	return context.WithValue(ctx, actionEmitIntentCollectorKey{}, collector)
}

func QueueActionEmitIntent(ctx context.Context, intent EmitIntent) bool {
	if ctx == nil {
		return false
	}
	collector, ok := ctx.Value(actionEmitIntentCollectorKey{}).(*[]EmitIntent)
	if !ok || collector == nil {
		return false
	}
	*collector = append(*collector, intent)
	return true
}
