package runfork

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// PendingJoinOccurrences enumerates retained obligations absent from this cut's
// event membership. Coordinates and cause evidence do not grant execution.
func (p RunForkPlan) PendingJoinOccurrences() ([]genericschedule.TransferredJoinOccurrence, error) {
	sources := append([]genericschedule.TransferredJoinOccurrence(nil), p.TransferredJoins...)
	for _, obligation := range p.FanOutObligations {
		barrier := obligation.Barrier
		if barrier == nil || barrier.Status != fanoutbarrier.StatusClosedPending || barrier.TransferredOccurrence == nil {
			continue
		}
		source, err := genericschedule.NewTransferredFanOutBarrierOccurrence(*barrier)
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	if len(sources) == 0 {
		return nil, nil
	}
	ids, known := p.HistoricalEventIDs(p.ForkPoint.Revision)
	if !known {
		return nil, fmt.Errorf("pending join requires exact fixed-cut event membership")
	}
	members := make(map[string]bool, len(ids))
	for _, id := range ids {
		members[id] = true
	}
	seen := make(map[string]bool, len(sources))
	pending := make([]genericschedule.TransferredJoinOccurrence, 0, len(sources))
	for _, source := range sources {
		if err := source.Validate(); err != nil {
			return nil, err
		}
		id := source.Publication.EventID
		if source.Command.RunID != p.SourceRunID || seen[id] {
			return nil, fmt.Errorf("pending join repeats or borrows a source obligation")
		}
		seen[id] = true
		if _, published := p.HistoricalJoinPublication(id); published {
			if !members[id] {
				return nil, fmt.Errorf("pending join publication contradicts fixed-cut membership")
			}
			continue
		}
		if members[id] {
			return nil, fmt.Errorf("pending join omits its claimed committed publication")
		}
		pending = append(pending, source)
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].Publication.EventID < pending[j].Publication.EventID })
	return pending, nil
}

func (p RunForkPlan) PendingJoinOccurrence(eventID string) (genericschedule.TransferredJoinOccurrence, bool, error) {
	sources, err := p.PendingJoinOccurrences()
	if err != nil {
		return genericschedule.TransferredJoinOccurrence{}, false, err
	}
	for _, source := range sources {
		if source.Publication.EventID == eventID {
			return source, true, nil
		}
	}
	return genericschedule.TransferredJoinOccurrence{}, false, nil
}
