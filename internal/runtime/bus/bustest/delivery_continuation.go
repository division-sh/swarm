package bustest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// DeliveryContinuationOwner models only the process-local handoff boundary for
// EventBus behavior tests. Selected-store tests remain strict: only committed
// handoff proofs admit acquisition. Non-durable fake stores may opt into
// uncommitted acquisition because they cannot produce a durable store proof.
type DeliveryContinuationOwner struct {
	mu               sync.Mutex
	allowUncommitted bool
	held             map[string]bool
}

func NewDeliveryContinuationOwner(allowUncommitted bool) *DeliveryContinuationOwner {
	return &DeliveryContinuationOwner{
		allowUncommitted: allowUncommitted,
		held:             make(map[string]bool),
	}
}

func (o *DeliveryContinuationOwner) AcceptCommitted(proofs []runtimedelivery.DurableHandoffProof) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, proof := range proofs {
		if err := proof.Validate(); err != nil {
			return err
		}
		if _, exists := o.held[proof.DeliveryID()]; exists {
			return fmt.Errorf("test delivery %s already has a local owner", proof.DeliveryID())
		}
		o.held[proof.DeliveryID()] = true
	}
	return nil
}

func (o *DeliveryContinuationOwner) Acquire(deliveryID string) (worklifetime.DeliveryContinuation, error) {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return nil, errors.New("test delivery id is required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	held, exists := o.held[deliveryID]
	if !exists {
		if !o.allowUncommitted {
			return nil, fmt.Errorf("test delivery %s has no committed handoff", deliveryID)
		}
		o.held[deliveryID] = false
	} else if !held {
		return nil, fmt.Errorf("test delivery %s is already carrier-owned", deliveryID)
	} else {
		o.held[deliveryID] = false
	}
	return &deliveryContinuation{owner: o, deliveryID: deliveryID}, nil
}

func (o *DeliveryContinuationOwner) Retain(snapshot runtimedelivery.Snapshot) error {
	deliveryID := strings.TrimSpace(snapshot.DeliveryID)
	if deliveryID == "" {
		return errors.New("test retained delivery id is required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.held[deliveryID] = true
	return nil
}

func (o *DeliveryContinuationOwner) Release(deliveryID string) error {
	o.mu.Lock()
	delete(o.held, strings.TrimSpace(deliveryID))
	o.mu.Unlock()
	return nil
}

func (*DeliveryContinuationOwner) OwnsPersistedRecovery() bool { return false }

func (*DeliveryContinuationOwner) Signal() {}

type deliveryContinuation struct {
	mu         sync.Mutex
	owner      *DeliveryContinuationOwner
	deliveryID string
	settled    bool
}

func (c *deliveryContinuation) DeliveryID() string {
	if c == nil {
		return ""
	}
	return c.deliveryID
}

func (c *deliveryContinuation) Resolve(_ context.Context, intent worklifetime.DeliveryContinuationIntent) (worklifetime.DeliveryContinuationResolution, error) {
	if c == nil || c.owner == nil {
		return 0, errors.New("test delivery continuation is required")
	}
	if intent != worklifetime.DeliveryContinuationReturn && intent != worklifetime.DeliveryContinuationConsume {
		return 0, errors.New("test delivery continuation resolution intent is invalid")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settled {
		return 0, errors.New("test delivery continuation is already settled")
	}
	c.owner.mu.Lock()
	if intent == worklifetime.DeliveryContinuationReturn {
		c.owner.held[c.deliveryID] = true
	} else {
		delete(c.owner.held, c.deliveryID)
	}
	c.owner.mu.Unlock()
	c.settled = true
	if intent == worklifetime.DeliveryContinuationReturn {
		return worklifetime.DeliveryContinuationReturned, nil
	}
	return worklifetime.DeliveryContinuationConsumed, nil
}
