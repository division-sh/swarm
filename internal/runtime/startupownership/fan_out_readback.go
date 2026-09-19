package startupownership

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

type FanOutExecutionObservation struct {
	Key      fanoutobligation.IntentKey
	GrantID  string
	Eligible bool
	Reason   string
}

type FanOutExecutionSnapshot struct {
	ObservedAt time.Time
	Rows       []FanOutExecutionObservation
}

type fanOutExecutionObserver interface {
	ObserveFanOutExecutions(context.Context, []FanOutRegistrationObservation, []fanoutobligation.IntentKey) (FanOutExecutionSnapshot, error)
}

type fanOutCommitTiming struct {
	key      fanoutobligation.IntentKey
	duration time.Duration
}

// ObserveFanOutRuntimePage augments a bounded durable page with separately
// observed execution evidence. Nothing in this projection authorizes execution.
// Timing storage is bounded to the last completed commit per registration.
func ObserveFanOutRuntimePage(ctx context.Context, capability ProcessCapability, page fanoutobligation.ListPage) (fanoutobligation.ListPage, error) {
	p, ok := capability.(*processCapability)
	if !ok || p == nil {
		return fanoutobligation.ListPage{}, errors.New("fan-out runtime observation requires retained process capability")
	}
	if len(page.Intents) > fanoutobligation.MaxListLimit {
		return fanoutobligation.ListPage{}, errors.New("fan-out runtime observation exceeds page bound")
	}
	page.Intents = append([]fanoutobligation.IntentReadback{}, page.Intents...)
	if len(page.Intents) == 0 {
		return page, nil
	}
	p.mu.Lock()
	grants, registrations := p.fanOutRegistrationsLocked()
	var service *fanOutServingService
	if p.fanOutCapacity != nil {
		service = p.fanOutCapacity.service
	}
	retired := p.requireLive() != nil || p.fanOutClosing
	p.mu.Unlock()
	if len(grants) == 0 || service == nil {
		for i := range page.Intents {
			page.Intents[i].Runtime = fanoutobligation.UnavailableRuntimeReadback()
			if retired {
				page.Intents[i].Runtime.Availability = "retired"
				page.Intents[i].Runtime.Reason = "process_retired"
			}
		}
		return page, nil
	}
	observer, ok := service.store.(fanOutExecutionObserver)
	if !ok {
		return fanoutobligation.ListPage{}, errors.New("selected fan-out owner lacks execution observation")
	}
	keys := make([]fanoutobligation.IntentKey, len(page.Intents))
	seen := make(map[fanoutobligation.IntentKey]bool, len(keys))
	for i := range page.Intents {
		keys[i] = page.Intents[i].Key
		if keys[i].RunID != page.RunID || seen[keys[i]] {
			return fanoutobligation.ListPage{}, errors.New("fan-out runtime page has duplicate or foreign-run keys")
		}
		seen[keys[i]] = true
		if err := keys[i].Validate(); err != nil {
			return fanoutobligation.ListPage{}, err
		}
	}
	observation, err := observer.ObserveFanOutExecutions(ctx, grants, keys)
	if err != nil {
		return fanoutobligation.ListPage{}, err
	}
	if observation.ObservedAt.IsZero() || len(observation.Rows) != len(keys) {
		return fanoutobligation.ListPage{}, errors.New("fan-out execution observation is incomplete")
	}
	byKey := make(map[fanoutobligation.IntentKey]FanOutExecutionObservation, len(keys))
	for _, row := range observation.Rows {
		if _, duplicate := byKey[row.Key]; duplicate || row.Reason == "" || (row.Eligible && row.GrantID == "") {
			return fanoutobligation.ListPage{}, errors.New("fan-out execution observation is contradictory")
		}
		byKey[row.Key] = row
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range page.Intents {
		row := &page.Intents[i]
		execution, found := byKey[row.Key]
		if !found {
			return fanoutobligation.ListPage{}, fmt.Errorf("fan-out execution observation omits %s", row.Key.String())
		}
		row.Runtime = fanoutobligation.UnavailableRuntimeReadback()
		r := registrations[execution.GrantID]
		if execution.GrantID == "" {
			row.Runtime.Reason = execution.Reason
			continue
		}
		if r == nil {
			return fanoutobligation.ListPage{}, errors.New("fan-out execution observation returned an unregistered grant")
		}
		for _, grant := range grants {
			if grant.Grant.GrantID == execution.GrantID && row.BundleHash != grant.Grant.BundleHash {
				return fanoutobligation.ListPage{}, errors.New("fan-out page bundle differs from the observed execution grant")
			}
		}
		generationRetired := false
		select {
		case <-r.grant.Done():
			generationRetired = true
		default:
		}
		if p.requireLive() != nil || p.fanOutClosing || generationRetired {
			row.Runtime.Availability = "retired"
			row.Runtime.Reason = "generation_retired"
			continue
		}
		workers, active := p.fanOutCapacity.limit, p.fanOutCapacity.used
		eligible, at := execution.Eligible, observation.ObservedAt
		if r.turnsClosing || r.ctx.Err() != nil {
			eligible, execution.Reason = false, "registration_closing"
		}
		row.Runtime = fanoutobligation.RuntimeReadback{Availability: "available", Reason: execution.Reason, Eligible: &eligible, Workers: &workers, ActiveWorkers: &active, ObservedAt: &at}
		if timing := r.lastCommit; timing != nil && timing.key == row.Key {
			ms := float64(timing.duration) / float64(time.Millisecond)
			row.Runtime.LastCommitMS = &ms
		}
	}
	return page, nil
}
