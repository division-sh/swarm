package runfork

import (
	"fmt"
	"slices"

	"github.com/google/uuid"
)

// The native cut supplies this derived partition from existing reply contexts
// and delivery history. It narrows planning; it is not execution authority.
func (p RunForkPlan) WithHistoricalRequestPublications(revision int64, partitions map[string][]string) (RunForkPlan, error) {
	ids, ok := p.HistoricalEventIDs(revision)
	if !ok {
		return RunForkPlan{}, fmt.Errorf("request publications require fixed historical event membership")
	}
	members := make(map[string]bool, len(ids))
	for _, id := range ids {
		members[id] = true
	}
	pending := make(map[[2]string]bool, len(p.PendingWork))
	for _, work := range p.PendingWork {
		pending[[2]string{work.EventID, work.DeliveryID}] = true
	}
	requests := make(map[string][]string, len(partitions))
	for eventID, deliveries := range partitions {
		if !members[eventID] || len(deliveries) == 0 {
			return RunForkPlan{}, fmt.Errorf("request publication lacks its historical unfinished partition")
		}
		seen := make(map[string]bool, len(deliveries))
		for _, id := range deliveries {
			if id == "" || seen[id] || !pending[[2]string{eventID, id}] {
				return RunForkPlan{}, fmt.Errorf("request publication has a duplicate or foreign delivery")
			}
			seen[id] = true
		}
		requests[eventID] = slices.Clone(deliveries)
	}
	p.historicalRequests = requests
	return p, nil
}

func (p RunForkPlan) RequestPendingRecipientPartition(eventID string) ([]string, bool, error) {
	id, err := uuid.Parse(eventID)
	if err != nil || id == uuid.Nil || id.String() != eventID {
		return nil, false, fmt.Errorf("request partition requires an exact canonical source event UUID")
	}
	deliveries, present := p.historicalRequests[eventID]
	if present && (p.historicalRevision <= 0 || p.ForkPoint.Revision != p.historicalRevision) {
		return nil, false, fmt.Errorf("request partition does not belong to the fixed cut")
	}
	return slices.Clone(deliveries), present, nil
}
