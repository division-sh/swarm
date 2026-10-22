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
	return reduceRecipientDisposition(len(deliveries), retained, func(i int) (deliverylifecycle.Status, error) {
		delivery := deliveries[i]
		return delivery.Status, deliverylifecycle.ValidateCanceledSnapshot(delivery)
	})
}

// Fixed-cut evidence is reduced without manufacturing a live delivery snapshot.
func HistoricalRecipientDisposition(deliveries []deliverylifecycle.HistoricalFact, retained fanoutobligation.InheritedTerminalDisposition) (fanoutobligation.InheritedTerminalDisposition, bool, error) {
	if retained != "" && (!retained.Valid() || retained == fanoutobligation.InheritedNoRoute) {
		return "", false, fmt.Errorf("continuing fan-out recipients require a terminal delivery contribution")
	}
	return reduceRecipientDisposition(len(deliveries), retained, func(i int) (deliverylifecycle.Status, error) {
		delivery := deliveries[i]
		return delivery.Status, delivery.Validate()
	})
}

func reduceRecipientDisposition(count int, retained fanoutobligation.InheritedTerminalDisposition, at func(int) (deliverylifecycle.Status, error)) (fanoutobligation.InheritedTerminalDisposition, bool, error) {
	if count == 0 {
		return "", false, fmt.Errorf("fan-out recipient disposition requires delivery evidence")
	}
	terminal, deadLettered, canceled := true, retained == fanoutobligation.InheritedDeadLettered, retained == fanoutobligation.InheritedCanceled
	for i := 0; i < count; i++ {
		captured, err := at(i)
		if err != nil {
			return "", false, err
		}
		status, err := deliverylifecycle.ParseStatus(string(captured))
		if err != nil {
			return "", false, err
		}
		if status != captured {
			return "", false, fmt.Errorf("fan-out recipient requires canonical delivery status")
		}
		terminal = terminal && status.Terminal()
		deadLettered = deadLettered || status == deliverylifecycle.StatusDeadLetter
		canceled = canceled || status == deliverylifecycle.StatusCanceled
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
