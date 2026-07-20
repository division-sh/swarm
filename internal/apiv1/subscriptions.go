package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
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
	decisionCards  decisioncard.Store
	bundle         runtimecontracts.BundleIdentity
	pollInterval   time.Duration
	healthInterval time.Duration
	queueSize      int
	workOwner      *worklifetime.Process
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

type ownedSubscriptionState uint8

const (
	ownedSubscriptionPrepared ownedSubscriptionState = iota
	ownedSubscriptionStarted
	ownedSubscriptionCancelled
)

type ownedSubscriptionWork struct {
	mu     sync.Mutex
	state  ownedSubscriptionState
	ctx    context.Context
	cancel context.CancelFunc
	lease  *worklifetime.Lease
	settle sync.Once
}

func (w *ownedSubscriptionWork) Start(run func(context.Context)) {
	if w == nil || run == nil {
		return
	}
	w.mu.Lock()
	if w.state != ownedSubscriptionPrepared {
		w.mu.Unlock()
		return
	}
	w.state = ownedSubscriptionStarted
	w.mu.Unlock()
	go func() {
		defer w.settleLease()
		run(w.ctx)
	}()
}

func (w *ownedSubscriptionWork) Cancel() {
	if w == nil {
		return
	}
	w.mu.Lock()
	settlePrepared := w.state == ownedSubscriptionPrepared
	if settlePrepared {
		w.state = ownedSubscriptionCancelled
	}
	w.mu.Unlock()
	w.cancel()
	if settlePrepared {
		w.settleLease()
	}
}

func (w *ownedSubscriptionWork) settleLease() {
	if w == nil {
		return
	}
	w.settle.Do(func() { _ = w.lease.Done() })
}

type runtimeLogSubscriptionState struct {
	since *time.Time
	seen  map[string]string
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
		decisionCards: opts.DecisionCards,
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
		return r.prepareHealthSubscription(session)
	case "event.subscribe":
		return r.prepareEventSubscription(session, req)
	case "run.subscribe_trace":
		return r.prepareRunTraceSubscription(session, req)
	case "runtime.subscribe_logs":
		return r.prepareRuntimeLogSubscription(session, req)
	case "mailbox.subscribe":
		return r.prepareDecisionCardSubscription(session, req)
	default:
		return subscriptionPlan{}, NewApplicationError(MethodUnavailableCode, false, map[string]any{"method": req.Method})
	}
}

func (r *SubscriptionRuntime) prepareDecisionCardSubscription(session *webSocketSession, req Request) (subscriptionPlan, error) {
	if r.decisionCards == nil {
		return subscriptionPlan{}, fmt.Errorf("decision card subscription store is required")
	}
	cursor := int64(0)
	if raw, ok := req.Params["cursor"]; ok && !isEmptyParam(raw) {
		value, valid := integerParam(raw)
		if !valid || value < 0 {
			return subscriptionPlan{}, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "must be a non-negative change sequence"})
		}
		cursor = int64(value)
	}
	id, work, err := r.newOwnedSubscriptionWork(session, "mailbox")
	if err != nil {
		return subscriptionPlan{}, err
	}
	return subscriptionPlan{
		Result: subscriptionIDResult{SubscriptionID: id},
		Start: func() {
			work.Start(func(ctx context.Context) { r.runDecisionCardSubscription(ctx, session, id, cursor) })
		},
		Cancel: work.Cancel,
	}, nil
}

func (r *SubscriptionRuntime) prepareHealthSubscription(session *webSocketSession) (subscriptionPlan, error) {
	id, work, err := r.newOwnedSubscriptionWork(session, "health")
	if err != nil {
		return subscriptionPlan{}, err
	}
	return subscriptionPlan{
		Result: subscriptionIDResult{SubscriptionID: id},
		Start:  func() { work.Start(func(ctx context.Context) { r.runHealthSubscription(ctx, session, id) }) },
		Cancel: work.Cancel,
	}, nil
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
	if err := requireEventListRunScope(filter); err != nil {
		return subscriptionPlan{}, err
	}
	replaySince, err := timestampParam(req.Params, "replay_since")
	if err != nil {
		return subscriptionPlan{}, err
	}
	id, work, err := r.newOwnedSubscriptionWork(session, "event")
	if err != nil {
		return subscriptionPlan{}, err
	}
	return subscriptionPlan{
		Result: subscriptionIDResult{SubscriptionID: id},
		Start: func() {
			work.Start(func(ctx context.Context) {
				r.runEventSubscription(ctx, session, id, reads, filter, subscriptionBaseSince(replaySince, r.now()))
			})
		},
		Cancel: work.Cancel,
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
	filter, err := runTraceFilterParam(req.Params)
	if err != nil {
		return subscriptionPlan{}, err
	}
	includeInternal, err := optionalBoolParam(req.Params, "include_internal", false)
	if err != nil {
		return subscriptionPlan{}, err
	}
	baseSince := subscriptionBaseSince(replaySince, r.now())
	if _, _, err := reads.LoadRunDebugTracePage(session.ctx, runID, store.RunDebugTraceQueryOptions{Limit: 1, Since: baseSince, Filter: filter, ExcludeRuntimeLogs: !includeInternal}); errors.Is(err, store.ErrRunNotFound) {
		return subscriptionPlan{}, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
	} else if errors.Is(err, store.ErrInvalidObservabilityCursor) {
		return subscriptionPlan{}, NewInvalidParamsError(map[string]any{"field": "replay_since", "reason": "invalid run trace replay watermark"})
	} else if err != nil {
		return subscriptionPlan{}, err
	}
	id, work, err := r.newOwnedSubscriptionWork(session, "run-trace")
	if err != nil {
		return subscriptionPlan{}, err
	}
	return subscriptionPlan{
		Result: subscriptionIDResult{SubscriptionID: id},
		Start: func() {
			work.Start(func(ctx context.Context) {
				r.runTraceSubscription(ctx, session, id, reads, runID, baseSince, filter, includeInternal)
			})
		},
		Cancel: work.Cancel,
	}, nil
}

func (r *SubscriptionRuntime) prepareRuntimeLogSubscription(session *webSocketSession, req Request) (subscriptionPlan, error) {
	reads, err := requireObservabilityReadStore(r.observability)
	if err != nil {
		return subscriptionPlan{}, err
	}
	opts, replaySince, err := runtimeLogSubscriptionOptionsFromParams(req.Params)
	if err != nil {
		return subscriptionPlan{}, err
	}
	opts.Since = subscriptionBaseSince(replaySince, r.now())
	opts.Limit = subscriptionBatchLimit
	opts.Order = "asc"
	id, work, err := r.newOwnedSubscriptionWork(session, "runtime-logs")
	if err != nil {
		return subscriptionPlan{}, err
	}
	return subscriptionPlan{
		Result: subscriptionIDResult{SubscriptionID: id},
		Start: func() {
			work.Start(func(ctx context.Context) { r.runRuntimeLogSubscription(ctx, session, id, reads, opts) })
		},
		Cancel: work.Cancel,
	}, nil
}

func runtimeLogSubscriptionOptionsFromParams(params map[string]any) (store.OperatorRuntimeLogListOptions, *time.Time, error) {
	allowed := map[string]struct{}{
		"run_id":       {},
		"bundle_hash":  {},
		"entity_id":    {},
		"session_id":   {},
		"component":    {},
		"level":        {},
		"error_code":   {},
		"source":       {},
		"replay_since": {},
	}
	for name := range params {
		if _, ok := allowed[name]; !ok {
			return store.OperatorRuntimeLogListOptions{}, nil, NewInvalidParamsError(map[string]any{"field": name, "reason": "unknown parameter"})
		}
	}
	out := store.OperatorRuntimeLogListOptions{}
	var err error
	if out.RunID, _, err = optionalStringParam(params, "run_id"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, nil, err
	}
	if out.BundleHash, err = optionalBundleHashParam(params, "bundle_hash"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, nil, err
	}
	if out.EntityID, _, err = optionalStringParam(params, "entity_id"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, nil, err
	}
	if out.SessionID, _, err = optionalStringParam(params, "session_id"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, nil, err
	}
	if out.Component, _, err = optionalStringParam(params, "component"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, nil, err
	}
	if out.Level, _, err = optionalStringParam(params, "level"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, nil, err
	}
	if out.ErrorCode, _, err = optionalStringParam(params, "error_code"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, nil, err
	}
	if out.Source, _, err = optionalStringParam(params, "source"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, nil, err
	}
	replaySince, err := timestampParam(params, "replay_since")
	if err != nil {
		return store.OperatorRuntimeLogListOptions{}, nil, err
	}
	return out, replaySince, nil
}

func (r *SubscriptionRuntime) newOwnedSubscriptionWork(session *webSocketSession, prefix string) (string, *ownedSubscriptionWork, error) {
	if r == nil || r.workOwner == nil {
		return "", nil, errors.New("process work owner is required for API subscription")
	}
	id := strings.Trim(strings.TrimSpace(prefix)+"-"+uuid.NewString(), "-")
	ctx, cancel := context.WithCancel(session.ctx)
	lease, err := r.workOwner.Begin(ctx)
	if err != nil {
		cancel()
		return "", nil, fmt.Errorf("admit API subscription: %w", err)
	}
	work := &ownedSubscriptionWork{ctx: lease.Context(), cancel: cancel, lease: lease}
	session.registerSubscription(id, work.Cancel)
	return id, work, nil
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

func (r *SubscriptionRuntime) runDecisionCardSubscription(ctx context.Context, session *webSocketSession, subscriptionID string, cursor int64) {
	emit := func() bool {
		changes, err := r.decisionCards.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{After: cursor, Limit: 200})
		if err != nil {
			session.close()
			return false
		}
		for _, change := range changes {
			notification, err := r.decisionCardChangeProjection(ctx, change)
			if err != nil {
				session.close()
				return false
			}
			if !session.notify(subscriptionID, notification) {
				return false
			}
			cursor = change.Sequence
		}
		return true
	}
	if !emit() {
		return
	}
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !emit() {
				return
			}
		}
	}
}

func (r *SubscriptionRuntime) decisionCardChangeProjection(ctx context.Context, change decisioncard.Change) (any, error) {
	card, err := r.decisionCards.GetDecisionCard(ctx, change.CardID)
	if err != nil {
		return nil, err
	}
	if card.Anchor.Kind() != decisioncard.AnchorKindProposedEffect {
		return change, nil
	}
	store, ok := r.decisionCards.(decisioncard.ProposedEffectStore)
	if !ok || store == nil {
		return nil, fmt.Errorf("proposed-effect subscription readback store is not configured")
	}
	effect, err := store.ProposedEffectReadback(ctx, change.CardID)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"sequence": change.Sequence, "card_id": change.CardID, "run_id": change.RunID,
		"change_type": change.ChangeType, "payload": change.Payload.Interface(), "created_at": change.CreatedAt,
		"effect": effect,
	}, nil
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
			session.close()
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
	session.close()
	return false
}

func (r *SubscriptionRuntime) runTraceSubscription(ctx context.Context, session *webSocketSession, subscriptionID string, reads ObservabilityReadStore, runID string, since *time.Time, filter store.RunDebugTraceFilter, includeInternal bool) {
	seen := map[string]string{}
	if !r.emitTraceNotifications(ctx, session, subscriptionID, reads, runID, since, filter, includeInternal, seen) {
		return
	}
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.emitTraceNotifications(ctx, session, subscriptionID, reads, runID, since, filter, includeInternal, seen) {
				return
			}
		}
	}
}

func (r *SubscriptionRuntime) emitTraceNotifications(ctx context.Context, session *webSocketSession, subscriptionID string, reads ObservabilityReadStore, runID string, since *time.Time, filter store.RunDebugTraceFilter, includeInternal bool, seen map[string]string) bool {
	cursor := ""
	for page := 0; page < subscriptionMaxPages; page++ {
		rows, nextCursor, err := reads.LoadRunDebugTracePage(ctx, runID, store.RunDebugTraceQueryOptions{
			Limit:              subscriptionBatchLimit,
			Cursor:             cursor,
			Since:              since,
			Filter:             filter,
			ExcludeRuntimeLogs: !includeInternal,
		})
		if err != nil {
			session.close()
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
	session.close()
	return false
}

func (r *SubscriptionRuntime) runRuntimeLogSubscription(ctx context.Context, session *webSocketSession, subscriptionID string, reads ObservabilityReadStore, opts store.OperatorRuntimeLogListOptions) {
	state := &runtimeLogSubscriptionState{
		since: opts.Since,
		seen:  map[string]string{},
	}
	if !r.emitRuntimeLogNotifications(ctx, session, subscriptionID, reads, opts, state) {
		return
	}
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.emitRuntimeLogNotifications(ctx, session, subscriptionID, reads, opts, state) {
				return
			}
		}
	}
}

func (r *SubscriptionRuntime) emitRuntimeLogNotifications(ctx context.Context, session *webSocketSession, subscriptionID string, reads ObservabilityReadStore, opts store.OperatorRuntimeLogListOptions, state *runtimeLogSubscriptionState) bool {
	if state == nil {
		state = &runtimeLogSubscriptionState{}
	}
	if state.seen == nil {
		state.seen = map[string]string{}
	}
	cursor := ""
	var latestDelivered time.Time
	for page := 0; page < subscriptionMaxPages; page++ {
		query := opts
		query.Since = state.since
		query.Cursor = cursor
		result, err := reads.ListOperatorRuntimeLogs(ctx, query)
		if err != nil {
			session.close()
			return false
		}
		for _, log := range result.Logs {
			key := strings.TrimSpace(log.LogID)
			if key == "" {
				key = subscriptionPayloadFingerprint(log)
			}
			fingerprint := subscriptionPayloadFingerprint(log)
			if state.seen[key] == fingerprint {
				latestDelivered = laterRuntimeLogWatermark(latestDelivered, log.TS)
				continue
			}
			state.seen[key] = fingerprint
			if !session.notify(subscriptionID, log) {
				return false
			}
			latestDelivered = laterRuntimeLogWatermark(latestDelivered, log.TS)
		}
		cursor = strings.TrimSpace(result.NextCursor)
		if cursor == "" {
			advanceRuntimeLogSubscriptionSince(state, latestDelivered)
			return true
		}
	}
	session.close()
	return false
}

func laterRuntimeLogWatermark(current time.Time, candidate time.Time) time.Time {
	if candidate.IsZero() {
		return current
	}
	candidate = candidate.UTC()
	if current.IsZero() || candidate.After(current) {
		return candidate
	}
	return current
}

func advanceRuntimeLogSubscriptionSince(state *runtimeLogSubscriptionState, latest time.Time) {
	if state == nil || latest.IsZero() {
		return
	}
	next := latest.UTC().Add(-time.Nanosecond)
	if state.since != nil && !next.After(state.since.UTC()) {
		return
	}
	value := next
	state.since = &value
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
