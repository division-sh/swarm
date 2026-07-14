package llm

import (
	"context"
	"strings"
	"sync"
	"time"
)

const (
	conversationWatchdogStateHealthyLongRunning = "healthy_long_running"
	conversationWatchdogStateNoOutput           = "no_output"
	conversationWatchdogBlockingLayerSession    = "session_execution"
	conversationWatchdogActionTurnLongRunning   = "turn_long_running"
	conversationWatchdogActionSessionNoOutput   = "session_no_output"
	conversationWatchdogOutcomeObserved         = "observed"
	conversationWatchdogOutcomeWarningEmitted   = "warning_emitted"
)

var sessionWatchdogPollInterval = time.Second

type sessionWatchdogMonitorWriter struct {
	base    MonitorTurnWriter
	cancel  context.CancelFunc
	done    chan struct{}
	ctx     context.Context
	meta    MonitorTurnMeta
	store   ConversationPersistence
	events  EventPublisher
	nowFn   func() time.Time
	startAt time.Time

	mu           sync.Mutex
	lastOutputAt time.Time
	sawOutput    bool
	firedLong    bool
	firedSilent  bool
}

func conversationWatchdogThresholds(turnTimeout time.Duration) (time.Duration, time.Duration) {
	if turnTimeout <= 0 {
		turnTimeout = 120 * time.Second
	}
	longRunningAfter := turnTimeout / 4
	if longRunningAfter < 30*time.Second {
		longRunningAfter = 30 * time.Second
	}
	if longRunningAfter > 2*time.Minute {
		longRunningAfter = 2 * time.Minute
	}
	noOutputAfter := longRunningAfter * 2
	if noOutputAfter <= longRunningAfter {
		noOutputAfter = longRunningAfter + 30*time.Second
	}
	return longRunningAfter, noOutputAfter
}

func newSessionWatchdogMonitorWriter(ctx context.Context, base MonitorTurnWriter, store ConversationPersistence, events EventPublisher, meta MonitorTurnMeta) MonitorTurnWriter {
	if store == nil || strings.TrimSpace(meta.AgentID) == "" || strings.TrimSpace(meta.SessionID) == "" || !meta.Memory.Enabled {
		return base
	}
	if err := meta.MemoryIdentity.Validate(); err != nil {
		return base
	}
	longRunningAfter := meta.WatchdogLongRunningAfter
	noOutputAfter := meta.WatchdogNoOutputAfter
	if longRunningAfter <= 0 || noOutputAfter <= 0 {
		longRunningAfter, noOutputAfter = conversationWatchdogThresholds(0)
	}
	if longRunningAfter <= 0 || noOutputAfter <= 0 {
		return base
	}
	loopCtx, cancel := context.WithCancel(ctx)
	nowFn := time.Now
	w := &sessionWatchdogMonitorWriter{
		base:    base,
		cancel:  cancel,
		done:    make(chan struct{}),
		ctx:     loopCtx,
		meta:    meta,
		store:   store,
		events:  events,
		nowFn:   nowFn,
		startAt: nowFn(),
	}
	go w.loop(loopCtx, longRunningAfter, noOutputAfter)
	return w
}

func (w *sessionWatchdogMonitorWriter) WriteStdout(line []byte) {
	if w.base != nil {
		w.base.WriteStdout(line)
	}
	if len(strings.TrimSpace(string(line))) == 0 {
		return
	}
	w.mu.Lock()
	w.sawOutput = true
	w.lastOutputAt = w.nowFn()
	w.mu.Unlock()
}

func (w *sessionWatchdogMonitorWriter) WriteStderr(line []byte) {
	if w.base != nil {
		w.base.WriteStderr(line)
	}
}

func (w *sessionWatchdogMonitorWriter) WriteNotice(format string, args ...any) {
	if w.base != nil {
		w.base.WriteNotice(format, args...)
	}
}

func (w *sessionWatchdogMonitorWriter) Close() error {
	if w.cancel != nil {
		w.cancel()
	}
	if w.done != nil {
		<-w.done
	}
	if w.base != nil {
		return w.base.Close()
	}
	return nil
}

func (w *sessionWatchdogMonitorWriter) loop(ctx context.Context, longRunningAfter, noOutputAfter time.Duration) {
	defer close(w.done)
	ticker := time.NewTicker(sessionWatchdogPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.evaluate(longRunningAfter, noOutputAfter)
		}
	}
}

func (w *sessionWatchdogMonitorWriter) evaluate(longRunningAfter, noOutputAfter time.Duration) {
	now := w.nowFn()
	w.mu.Lock()
	elapsed := now.Sub(w.startAt)
	lastOutputAt := w.lastOutputAt
	sawOutput := w.sawOutput
	firedLong := w.firedLong
	firedSilent := w.firedSilent
	w.mu.Unlock()

	if !firedLong && sawOutput && elapsed >= longRunningAfter {
		w.emit(conversationWatchdogStateHealthyLongRunning, conversationWatchdogActionTurnLongRunning, conversationWatchdogOutcomeObserved, lastOutputAt, now)
		w.mu.Lock()
		w.firedLong = true
		w.mu.Unlock()
	}

	silentFor := elapsed
	if sawOutput && !lastOutputAt.IsZero() {
		silentFor = now.Sub(lastOutputAt)
	}
	if !firedSilent && elapsed >= noOutputAfter && silentFor >= noOutputAfter {
		w.emit(conversationWatchdogStateNoOutput, conversationWatchdogActionSessionNoOutput, conversationWatchdogOutcomeWarningEmitted, lastOutputAt, now)
		w.mu.Lock()
		w.firedSilent = true
		w.mu.Unlock()
	}
}

func (w *sessionWatchdogMonitorWriter) emit(state, action, outcome string, lastOutputAt, now time.Time) {
	if w.store == nil {
		return
	}
	payload := &ConversationWatchdog{
		State:         state,
		BlockingLayer: conversationWatchdogBlockingLayerSession,
		Action:        action,
		Outcome:       outcome,
		RecordedAt:    now.UTC().Format(time.RFC3339Nano),
	}
	if !lastOutputAt.IsZero() {
		payload.LastOutputAt = lastOutputAt.UTC().Format(time.RFC3339Nano)
	}
	if err := w.store.UpdateLiveSessionWatchdog(w.ctx, ConversationWatchdogUpdate{
		SessionID: strings.TrimSpace(w.meta.SessionID),
		AgentID:   strings.TrimSpace(w.meta.AgentID),
		Identity:  w.meta.MemoryIdentity.Normalize(),
		Watchdog:  payload,
	}); err != nil {
		logPublisherRuntime(w.ctx, w.events, "warn", "persist_session_watchdog_failed", "Persisting live session watchdog state failed", strings.TrimSpace(w.meta.AgentID), strings.TrimSpace(w.meta.SessionID), "", map[string]any{
			"watchdog_state":  payload.State,
			"watchdog_action": payload.Action,
		}, err)
		return
	}
	logPublisherRuntime(w.ctx, w.events, "info", action, "Live session watchdog state updated", strings.TrimSpace(w.meta.AgentID), strings.TrimSpace(w.meta.SessionID), "", map[string]any{
		"watchdog_state":   payload.State,
		"blocking_layer":   payload.BlockingLayer,
		"watchdog_action":  payload.Action,
		"watchdog_outcome": payload.Outcome,
		"last_output_at":   payload.LastOutputAt,
		"recorded_at":      payload.RecordedAt,
	}, nil)
}
