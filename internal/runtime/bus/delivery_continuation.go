package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// selectedDeliveryTransfers owns only the bounded commit-to-carrier handoff of
// one selected-contract execution. It does not retain, schedule, or recover
// work; selected retryable failures terminalize before runtime retirement.
type selectedDeliveryTransfers struct {
	mu        sync.Mutex
	authority runtimedelivery.ExecutionAuthority
	held      map[string]selectedDeliveryOwnership
}

type selectedDeliveryOwnership uint8

const (
	selectedDeliveryPublication selectedDeliveryOwnership = iota + 1
	selectedDeliveryCarrier
	selectedDeliveryAttempt
	selectedDeliveryTerminalCarrier
)

func newSelectedDeliveryTransfers(authority runtimedelivery.ExecutionAuthority) (*selectedDeliveryTransfers, error) {
	if authority.Kind() != runtimedelivery.ExecutionAuthoritySelectedContractFork {
		return nil, errors.New("selected delivery transfer authority is required")
	}
	if err := authority.Validate(); err != nil {
		return nil, err
	}
	return &selectedDeliveryTransfers{authority: authority, held: make(map[string]selectedDeliveryOwnership)}, nil
}

func (o *selectedDeliveryTransfers) AcceptCommitted(proofs []runtimedelivery.DurableHandoffProof) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	seen := make(map[string]struct{}, len(proofs))
	for _, proof := range proofs {
		if err := proof.Validate(); err != nil {
			return err
		}
		if !proof.Authority().Equal(o.authority) {
			return fmt.Errorf("selected delivery %s crossed execution authority", proof.DeliveryID())
		}
		if _, duplicate := seen[proof.DeliveryID()]; duplicate {
			return fmt.Errorf("selected delivery %s is duplicated in committed handoff batch", proof.DeliveryID())
		}
		seen[proof.DeliveryID()] = struct{}{}
		if _, exists := o.held[proof.DeliveryID()]; exists {
			return fmt.Errorf("selected delivery %s already has a local owner", proof.DeliveryID())
		}
	}
	for _, proof := range proofs {
		o.held[proof.DeliveryID()] = selectedDeliveryPublication
	}
	return nil
}

func (o *selectedDeliveryTransfers) Acquire(deliveryID string) (worklifetime.DeliveryContinuation, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	held, exists := o.held[deliveryID]
	if !exists || held != selectedDeliveryPublication {
		return nil, fmt.Errorf("selected delivery %s is not publication-owned", deliveryID)
	}
	o.held[deliveryID] = selectedDeliveryCarrier
	return &selectedDeliveryCapability{owner: o, deliveryID: deliveryID}, nil
}

func (*selectedDeliveryTransfers) Retain(runtimedelivery.Snapshot) error {
	return errors.New("selected-contract delivery cannot retain retry continuation")
}

func (*selectedDeliveryTransfers) OwnsPersistedRecovery() bool { return false }

func (*selectedDeliveryTransfers) Signal() {}

func (o *selectedDeliveryTransfers) Release(deliveryID string) error {
	if o == nil || deliveryID == "" {
		return errors.New("exact selected delivery identity is required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, exists := o.held[deliveryID]; !exists {
		return nil
	}
	if o.held[deliveryID] == selectedDeliveryCarrier {
		o.held[deliveryID] = selectedDeliveryTerminalCarrier
	} else {
		delete(o.held, deliveryID)
	}
	return nil
}

type selectedDeliveryCapability struct {
	mu         sync.Mutex
	owner      *selectedDeliveryTransfers
	deliveryID string
	settled    bool
}

func (c *selectedDeliveryCapability) DeliveryID() string {
	if c == nil {
		return ""
	}
	return c.deliveryID
}

func (c *selectedDeliveryCapability) Resolve(_ context.Context, intent worklifetime.DeliveryContinuationIntent) (worklifetime.DeliveryContinuationResolution, error) {
	if c == nil || c.owner == nil {
		return 0, errors.New("selected delivery capability is required")
	}
	if intent != worklifetime.DeliveryContinuationReturn && intent != worklifetime.DeliveryContinuationConsume {
		return 0, errors.New("selected delivery continuation resolution intent is invalid")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settled {
		return 0, errors.New("selected delivery capability is already settled")
	}
	c.owner.mu.Lock()
	current, exists := c.owner.held[c.deliveryID]
	if !exists || (current != selectedDeliveryCarrier && current != selectedDeliveryTerminalCarrier) {
		c.owner.mu.Unlock()
		return 0, fmt.Errorf("selected delivery %s is not carrier-owned", c.deliveryID)
	}
	if current == selectedDeliveryTerminalCarrier {
		delete(c.owner.held, c.deliveryID)
		c.owner.mu.Unlock()
		c.settled = true
		return worklifetime.DeliveryContinuationTerminal, nil
	}
	if intent == worklifetime.DeliveryContinuationReturn {
		c.owner.held[c.deliveryID] = selectedDeliveryPublication
	} else {
		c.owner.held[c.deliveryID] = selectedDeliveryAttempt
	}
	c.owner.mu.Unlock()
	c.settled = true
	if intent == worklifetime.DeliveryContinuationReturn {
		return worklifetime.DeliveryContinuationReturned, nil
	}
	return worklifetime.DeliveryContinuationConsumed, nil
}
