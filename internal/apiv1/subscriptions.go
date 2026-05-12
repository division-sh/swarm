package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/store"
)

const (
	defaultSubscriptionQueueSize     = 64
	defaultSubscriptionPollInterval  = time.Second
	defaultSubscriptionHealthCadence = 5 * time.Second
	subscriptionBatchLimit           = 1000
	subscriptionMaxPages             = 10
)

type SubscriptionRuntimeOptions struct {
	PollInterval   time.Duration
	HealthInterval time.Duration
	QueueSize      int
}

type SubscriptionRuntime struct {
	now            func() time.Time
	ready          func() bool
	database       Pinger
	observability  ObservabilityReadStore
	bundle         runtimecontracts.BundleIdentity
	pollInterval   time.Duration
	healthInterval time.Duration
	queueSize      int
}

type subscriptionIDResult struct {
	SubscriptionID string `json:"subscription_id"`
}

type rpcSubscriptionNotification struct {
	JSONRPC string                `json:"jsonrpc"`
	Method  string                `json:"method"`
	Params  rpcSubscriptionParams `json:"params"`
}

type rpcSubscriptionParams struct {
	Subscription string `json:"subscription"`
	Result       any    `json:"result"`
}

type subscriptionPlan struct {
	Result subscriptionIDResult
	Start  func()
	Cancel context.CancelFunc
}

func OperatorSubscriptions(opts OperatorReadOptions, overrides ...SubscriptionRuntimeOptions) *SubscriptionRuntime {
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	ready := opts.Ready
	if ready == nil {
		ready = func() bool { return false }
	}
	out := &SubscriptionRuntime{
		now:           now,
		ready:         ready,
		database:      opts.Database,
		observability: opts.Observability,
		bundle:        opts.Bundle,
	}
	if len(overrides) > 0 {
		out.pollInterval = overrides[0].PollInterval
		out.healthInterval = overrides[0].HealthInterval
		out.queueSize = overrides[0].QueueSize
	}
	return out.withDefaults()
}

func (r *SubscriptionRuntime) withDefaults() *SubscriptionRuntime {
	if r == nil {
		return nil
	}
	if r.now == nil {
		r.now = func() time.Time { return time.Now().UTC() }
	}
	if r.ready == nil {
		r.ready = func() bool { return false }
	}
	if r.pollInterval <= 0 {
		r.pollInterval = defaultSubscriptionPollInterval
	}
	if r.healthInterval <= 0 {
		r.healthInterval = defaultSubscriptionHealthCadence
	}
	if r.queueSize <= 0 {
		r.queueSize = defaultSubscriptionQueueSize
	}
	return r
}

func (r *SubscriptionRuntime) prepare(session *webSocketSession, req Request) (subscriptionPlan, error) {
	if r == nil {
		return subscriptionPlan{}, fmt.Errorf("subscription runtime is required")
	}
	switch req.Method {
	case "health.subscribe":
		return r.prepareHealthSubscription(session), nil
	case "event.subscribe":
		return r.prepareEventSubscription(session, req)
	case "run.subscribe_trace":
		return r.prepareRunTraceSubscription(session, req)
	default:
		return subscriptionPlan{}, NewApplicationError(MethodUnavailableCode, false, map[string]any{"method": req.Method})
	}
}

func (r *SubscriptionRuntime) prepareHealthSubscription(session *webSocketSession) subscriptionPlan {
	id, ctx, cancel := session.newSubscriptionContext("health")
	return subscriptionPlan{
		Result: subscriptionIDResult{SubscriptionID: id},
		Start: func() {
			go r.runHealthSubscription(ctx, session, id)
		},
		Cancel: cancel,
	}
}

func (r *SubscriptionRuntime) prepareEventSubscription(session *webSocketSession, req Request) (subscriptionPlan, error) {
	reads, err := requireObservabilityReadStore(r.observability)
	if err != nil {
		return subscriptionPlan{}, err
	}
	filter, err := eventListFilterParam(req.Params)
	if err != nil {
		return subscriptionPlan{}, err
	}
	replaySince, err := timestampParam(req.Params, "replay_since")
	if err != nil {
		return subscriptionPlan{}, err
	}
	id, ctx, cancel := session.newSubscriptionContext("event")
	return subscriptionPlan{
		Result: subscriptionIDResult{SubscriptionID: id},
		Start: func() {
			go r.runEventSubscription(ctx, session, id, reads, filter, subscriptionBaseSince(replaySince, r.now()))
		},
		Cancel: cancel,
	}, nil
}

func (r *SubscriptionRuntime) prepareRunTraceSubscription(session *webSocketSession, req Request) (subscriptionPlan, error) {
	reads, err := requireObservabilityReadStore(r.observability)
	if err != nil {
		return subscriptionPlan{}, err
	}
	runID, err := requiredStringParam(req.Params, "run_id")
	if err != nil {
		return subscriptionPlan{}, err
	}
	replaySince, err := timestampParam(req.Params, "replay_since")
	if err != nil {
		return subscriptionPlan{}, err
	}
	baseSince := subscriptionBaseSince(replaySince, r.now())
	if _, _, err := reads.LoadRunDebugTracePage(session.ctx, runID, store.RunDebugTraceQueryOptions{Limit: 1, Since: baseSince}); errors.Is(err, store.ErrRunNotFound) {
		return subscriptionPlan{}, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
	} else if errors.Is(err, store.ErrInvalidObservabilityCursor) {
		return subscriptionPlan{}, NewInvalidParamsError(map[string]any{"field": "replay_since", "reason": "invalid run trace replay watermark"})
	} else if err != nil {
		return subscriptionPlan{}, err
	}
	id, ctx, cancel := session.newSubscriptionContext("run-trace")
	return subscriptionPlan{
		Result: subscriptionIDResult{SubscriptionID: id},
		Start: func() {
			go r.runTraceSubscription(ctx, session, id, reads, runID, baseSince)
		},
		Cancel: cancel,
	}, nil
}

func (s *webSocketSession) newSubscriptionContext(prefix string) (string, context.Context, context.CancelFunc) {
	subscriptionID := strings.Trim(strings.TrimSpace(prefix)+"-"+uuid.NewString(), "-")
	ctx, cancel := context.WithCancel(s.ctx)
	s.registerSubscription(subscriptionID, cancel)
	return subscriptionID, ctx, cancel
}

func (r *SubscriptionRuntime) runHealthSubscription(ctx context.Context, session *webSocketSession, subscriptionID string) {
	if !session.notify(subscriptionID, operatorHealthSnapshot(ctx, r.ready, r.database, r.bundle)) {
		return
	}
	ticker := time.NewTicker(r.healthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !session.notify(subscriptionID, operatorHealthSnapshot(ctx, r.ready, r.database, r.bundle)) {
				return
			}
		}
	}
}

func (r *SubscriptionRuntime) runEventSubscription(ctx context.Context, session *webSocketSession, subscriptionID string, reads ObservabilityReadStore, filter store.OperatorEventListFilter, since *time.Time) {
	seen := map[string]string{}
	if !r.emitEventNotifications(ctx, session, subscriptionID, reads, filter, since, seen) {
		return
	}
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.emitEventNotifications(ctx, session, subscriptionID, reads, filter, since, seen) {
				return
			}
		}
	}
}

func (r *SubscriptionRuntime) emitEventNotifications(ctx context.Context, session *webSocketSession, subscriptionID string, reads ObservabilityReadStore, filter store.OperatorEventListFilter, since *time.Time, seen map[string]string) bool {
	cursor := ""
	for page := 0; page < subscriptionMaxPages; page++ {
		result, err := reads.ListOperatorEvents(ctx, store.OperatorEventListOptions{
			Filter: filter,
			Since:  since,
			Limit:  subscriptionBatchLimit,
			Cursor: cursor,
			Order:  "asc",
		})
		if err != nil {
			session.cancel()
			return false
		}
		for _, event := range result.Events {
			key := strings.TrimSpace(event.EventID)
			if key == "" {
				key = subscriptionPayloadFingerprint(event)
			}
			fingerprint := subscriptionPayloadFingerprint(event)
			if seen[key] == fingerprint {
				continue
			}
			seen[key] = fingerprint
			if !session.notify(subscriptionID, event) {
				return false
			}
		}
		cursor = strings.TrimSpace(result.NextCursor)
		if cursor == "" {
			return true
		}
	}
	session.cancel()
	return false
}

func (r *SubscriptionRuntime) runTraceSubscription(ctx context.Context, session *webSocketSession, subscriptionID string, reads ObservabilityReadStore, runID string, since *time.Time) {
	seen := map[string]string{}
	if !r.emitTraceNotifications(ctx, session, subscriptionID, reads, runID, since, seen) {
		return
	}
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.emitTraceNotifications(ctx, session, subscriptionID, reads, runID, since, seen) {
				return
			}
		}
	}
}

func (r *SubscriptionRuntime) emitTraceNotifications(ctx context.Context, session *webSocketSession, subscriptionID string, reads ObservabilityReadStore, runID string, since *time.Time, seen map[string]string) bool {
	cursor := ""
	for page := 0; page < subscriptionMaxPages; page++ {
		rows, nextCursor, err := reads.LoadRunDebugTracePage(ctx, runID, store.RunDebugTraceQueryOptions{
			Limit:  subscriptionBatchLimit,
			Cursor: cursor,
			Since:  since,
		})
		if err != nil {
			session.cancel()
			return false
		}
		for _, row := range rows {
			key := runTraceSubscriptionKey(row)
			fingerprint := subscriptionPayloadFingerprint(row)
			if seen[key] == fingerprint {
				continue
			}
			seen[key] = fingerprint
			if !session.notify(subscriptionID, row) {
				return false
			}
		}
		cursor = strings.TrimSpace(nextCursor)
		if cursor == "" {
			return true
		}
	}
	session.cancel()
	return false
}

func operatorHealthSnapshot(ctx context.Context, ready func() bool, database Pinger, bundle runtimecontracts.BundleIdentity) healthCheckResult {
	runtimeOK := false
	if ready != nil {
		runtimeOK = ready()
	}
	dbOK := false
	if database != nil {
		dbOK = database.Ping(ctx) == nil
	}
	return healthCheckResult{
		Alive:     true,
		Ready:     runtimeOK && dbOK,
		DBOK:      dbOK,
		RuntimeOK: runtimeOK,
		Bundle:    bundle,
	}
}

func subscriptionBaseSince(replaySince *time.Time, now time.Time) *time.Time {
	if replaySince != nil {
		value := replaySince.UTC()
		return &value
	}
	value := now.UTC()
	return &value
}

func runTraceSubscriptionKey(row store.RunDebugTraceRow) string {
	parts := []string{
		strings.TrimSpace(row.EventID),
		strings.TrimSpace(row.DeliveryID),
		strings.TrimSpace(row.TurnID),
	}
	key := strings.Join(parts, "\x00")
	if strings.Trim(key, "\x00") != "" {
		return key
	}
	return subscriptionPayloadFingerprint(row)
}

func subscriptionPayloadFingerprint(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%#v", value)
	}
	return string(raw)
}
