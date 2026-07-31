package bus

import (
	"context"
	"errors"
	"strings"
	"sync"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// permissiveTestDeliveryOwner isolates EventBus routing tests from selected-store
// recovery. Store/conformance tests use the real committed-handoff owner.
type permissiveTestDeliveryOwner struct{}

func (permissiveTestDeliveryOwner) AcceptCommitted(proofs []runtimedelivery.DurableHandoffProof) error {
	for _, proof := range proofs {
		if err := proof.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (permissiveTestDeliveryOwner) Acquire(deliveryID string) (worklifetime.DeliveryContinuation, error) {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return nil, errors.New("test delivery id is required")
	}
	return &permissiveTestDeliveryContinuation{deliveryID: deliveryID}, nil
}

func (permissiveTestDeliveryOwner) Retain(snapshot runtimedelivery.Snapshot) error {
	if strings.TrimSpace(snapshot.DeliveryID) == "" {
		return errors.New("test retained delivery id is required")
	}
	return nil
}

func (permissiveTestDeliveryOwner) Release(string) error { return nil }

func (permissiveTestDeliveryOwner) OwnsPersistedRecovery() bool { return false }

func (permissiveTestDeliveryOwner) Signal() {}

type permissiveTestDeliveryContinuation struct {
	mu         sync.Mutex
	deliveryID string
	settled    bool
}

type controlledTestDeliveryOwner struct {
	mu             sync.Mutex
	failAcquire    bool
	returnFailures int
	returnAttempts int
	returnedIDs    []string
	signals        int
}

func (o *controlledTestDeliveryOwner) AcceptCommitted(proofs []runtimedelivery.DurableHandoffProof) error {
	for _, proof := range proofs {
		if err := proof.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (o *controlledTestDeliveryOwner) Acquire(deliveryID string) (worklifetime.DeliveryContinuation, error) {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return nil, errors.New("test delivery id is required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failAcquire {
		return nil, errors.New("injected delivery continuation admission failure")
	}
	return &controlledTestDeliveryContinuation{owner: o, deliveryID: deliveryID}, nil
}

func (*controlledTestDeliveryOwner) Retain(snapshot runtimedelivery.Snapshot) error {
	if strings.TrimSpace(snapshot.DeliveryID) == "" {
		return errors.New("test retained delivery id is required")
	}
	return nil
}

func (*controlledTestDeliveryOwner) Release(string) error { return nil }

func (*controlledTestDeliveryOwner) OwnsPersistedRecovery() bool { return false }

func (o *controlledTestDeliveryOwner) Signal() {
	o.mu.Lock()
	o.signals++
	o.mu.Unlock()
}

type controlledTestDeliveryContinuation struct {
	mu         sync.Mutex
	owner      *controlledTestDeliveryOwner
	deliveryID string
	settled    bool
}

func (c *controlledTestDeliveryContinuation) DeliveryID() string {
	if c == nil {
		return ""
	}
	return c.deliveryID
}

func (c *controlledTestDeliveryContinuation) Resolve(_ context.Context, intent worklifetime.DeliveryContinuationIntent) (worklifetime.DeliveryContinuationResolution, error) {
	if c == nil || c.owner == nil {
		return 0, errors.New("test delivery continuation is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settled {
		return 0, errors.New("test delivery continuation is already settled")
	}
	if intent == worklifetime.DeliveryContinuationReturn {
		c.owner.mu.Lock()
		defer c.owner.mu.Unlock()
		c.owner.returnAttempts++
		if c.owner.returnAttempts <= c.owner.returnFailures {
			return 0, errors.New("injected delivery continuation return failure")
		}
		c.owner.returnedIDs = append(c.owner.returnedIDs, c.deliveryID)
		c.settled = true
		return worklifetime.DeliveryContinuationReturned, nil
	}
	if intent != worklifetime.DeliveryContinuationConsume {
		return 0, errors.New("test delivery continuation resolution intent is invalid")
	}
	c.settled = true
	return worklifetime.DeliveryContinuationConsumed, nil
}

func (c *permissiveTestDeliveryContinuation) DeliveryID() string {
	if c == nil {
		return ""
	}
	return c.deliveryID
}

func (c *permissiveTestDeliveryContinuation) Resolve(_ context.Context, intent worklifetime.DeliveryContinuationIntent) (worklifetime.DeliveryContinuationResolution, error) {
	if c == nil {
		return 0, errors.New("test delivery continuation is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settled {
		return 0, errors.New("test delivery continuation is already settled")
	}
	c.settled = true
	if intent == worklifetime.DeliveryContinuationReturn {
		return worklifetime.DeliveryContinuationReturned, nil
	}
	if intent == worklifetime.DeliveryContinuationConsume {
		return worklifetime.DeliveryContinuationConsumed, nil
	}
	return 0, errors.New("test delivery continuation resolution intent is invalid")
}
