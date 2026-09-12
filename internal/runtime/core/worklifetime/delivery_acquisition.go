package worklifetime

import "errors"

// DeliveryAcquisition is the result of one exact owner's atomic election.
// Another owner is evidence of contention, never a synthetic carrier or an
// assertion that this caller executed the delivery.
type DeliveryAcquisition struct {
	deliveryID   string
	disposition  DeliveryAcquisitionDisposition
	continuation DeliveryContinuation
}

type DeliveryAcquisitionDisposition uint8

const (
	DeliveryAcquired DeliveryAcquisitionDisposition = iota + 1
	DeliveryAlreadyOwned
	DeliveryTerminallyFenced
)

func AcquiredDelivery(continuation DeliveryContinuation) DeliveryAcquisition {
	if continuation == nil {
		return DeliveryAcquisition{}
	}
	return DeliveryAcquisition{deliveryID: continuation.DeliveryID(), disposition: DeliveryAcquired, continuation: continuation}
}

func AlreadyOwnedDelivery(deliveryID string) DeliveryAcquisition {
	return DeliveryAcquisition{deliveryID: deliveryID, disposition: DeliveryAlreadyOwned}
}

func TerminallyFencedDelivery(deliveryID string) DeliveryAcquisition {
	return DeliveryAcquisition{deliveryID: deliveryID, disposition: DeliveryTerminallyFenced}
}

func (a DeliveryAcquisition) Disposition() DeliveryAcquisitionDisposition { return a.disposition }

func (a DeliveryAcquisition) Acquired() (DeliveryContinuation, bool) {
	return a.continuation, a.disposition == DeliveryAcquired && a.continuation != nil
}

func (a DeliveryAcquisition) Validate(deliveryID string) error {
	if deliveryID == "" || a.deliveryID != deliveryID {
		return errors.New("delivery acquisition requires its exact delivery identity")
	}
	switch a.disposition {
	case DeliveryAcquired:
		if a.continuation == nil || a.continuation.DeliveryID() != deliveryID {
			return errors.New("acquired delivery requires its exact continuation")
		}
	case DeliveryAlreadyOwned, DeliveryTerminallyFenced:
		if a.continuation != nil {
			return errors.New("unacquired delivery cannot carry execution authority")
		}
	default:
		return errors.New("unknown delivery acquisition disposition")
	}
	return nil
}
