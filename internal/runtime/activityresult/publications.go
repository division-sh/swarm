package activityresult

import (
	"context"
	"fmt"
)

type publishedInventoryKey struct{}

type publishedInventoryState struct {
	runID     string
	byRequest map[string]PublishedContinuation
}

// PublishedInventory is immutable accounting evidence. Execution authority is
// independently supplied and checked by the selected receiver/activity owners.
type PublishedInventory struct {
	state *publishedInventoryState
}

func NewPublishedInventory(runID string, publications []PublishedContinuation) (PublishedInventory, error) {
	if runID == "" {
		return PublishedInventory{}, fmt.Errorf("activity publication inventory requires an exact child run")
	}
	byRequest := make(map[string]PublishedContinuation, len(publications))
	for _, publication := range publications {
		if !publication.Present() || publication.RunID() != runID {
			return PublishedInventory{}, fmt.Errorf("selected activity accounting has foreign child evidence")
		}
		if _, duplicate := byRequest[publication.RequestID()]; duplicate {
			return PublishedInventory{}, fmt.Errorf("selected activity accounting repeats a request")
		}
		byRequest[publication.RequestID()] = publication
	}
	return PublishedInventory{state: &publishedInventoryState{runID: runID, byRequest: byRequest}}, nil
}

func (p PublishedInventory) Present() bool { return p.state != nil }

func (p PublishedInventory) RequireRun(runID string) error {
	if p.state == nil || p.state.runID != runID {
		return fmt.Errorf("activity publication inventory has foreign selected run")
	}
	return nil
}

func (p PublishedInventory) Bind(ctx context.Context) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.state == nil {
		return nil, fmt.Errorf("activity publication inventory is not admitted")
	}
	return context.WithValue(ctx, publishedInventoryKey{}, p.state), nil
}

func (p PublishedInventory) RequireBound(ctx context.Context) error {
	if state, _ := ctx.Value(publishedInventoryKey{}).(*publishedInventoryState); p.state == nil || state != p.state {
		return fmt.Errorf("activity publication inventory differs from its receiver owner")
	}
	return nil
}

func PublishedContinuationFromContext(ctx context.Context, requestID string) (PublishedContinuation, bool) {
	state, _ := ctx.Value(publishedInventoryKey{}).(*publishedInventoryState)
	if state == nil {
		return PublishedContinuation{}, false
	}
	publication, found := state.byRequest[requestID]
	return publication, found
}
