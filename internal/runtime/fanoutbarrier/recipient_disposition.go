package fanoutbarrier

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

// RecipientDisposition reduces one publication's admitted delivery partition.
// An unfinished recipient prevents terminal settlement, including when another
// recipient has already failed or been canceled.
func RecipientDisposition(deliveries []deliverylifecycle.Snapshot) (fanoutobligation.InheritedTerminalDisposition, bool, error) {
	return recipientDisposition(deliveries, "")
}

// RecipientDispositionWithRetained combines an exact terminal source partition
// with its continuing child recipients. It never settles an unfinished child.
// Admission of the partition is separate from reducing its terminal result.
func RecipientDispositionWithRetained(deliveries []deliverylifecycle.Snapshot, retained fanoutobligation.InheritedTerminalDisposition) (fanoutobligation.InheritedTerminalDisposition, bool, error) {
	if !retained.Valid() || retained == fanoutobligation.InheritedNoRoute {
		return "", false, fmt.Errorf("continuing fan-out recipients require a terminal delivery contribution")
	}
	return recipientDisposition(deliveries, retained)
}

func recipientDisposition(deliveries []deliverylifecycle.Snapshot, retained fanoutobligation.InheritedTerminalDisposition) (fanoutobligation.InheritedTerminalDisposition, bool, error) {
	if len(deliveries) == 0 {
		return "", false, fmt.Errorf("fan-out recipient disposition requires delivery evidence")
	}
	terminal, deadLettered, canceled := true, retained == fanoutobligation.InheritedDeadLettered, retained == fanoutobligation.InheritedCanceled
	for _, delivery := range deliveries {
		status, err := deliverylifecycle.ParseStatus(string(delivery.Status))
		if err != nil {
			return "", false, err
		}
		if status != delivery.Status {
			return "", false, fmt.Errorf("fan-out recipient requires canonical delivery status")
		}
		if err := deliverylifecycle.ValidateCanceledSnapshot(delivery); err != nil {
			return "", false, err
		}
		terminal = terminal && delivery.Terminal()
		deadLettered = deadLettered || delivery.Status == deliverylifecycle.StatusDeadLetter
		canceled = canceled || delivery.Status == deliverylifecycle.StatusCanceled
	}
	if !terminal {
		return "", false, nil
	}
	if deadLettered {
		return fanoutobligation.InheritedDeadLettered, true, nil
	}
	if canceled {
		return fanoutobligation.InheritedCanceled, true, nil
	}
	return fanoutobligation.InheritedSucceeded, true, nil
}
