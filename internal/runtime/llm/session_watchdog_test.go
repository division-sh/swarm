package llm

import (
	"context"
	"testing"
	"time"

	"swarm/internal/runtime/sessions"
)

type noopMonitorWriter struct{}

func (noopMonitorWriter) WriteStdout([]byte)         {}
func (noopMonitorWriter) WriteStderr([]byte)         {}
func (noopMonitorWriter) WriteNotice(string, ...any) {}
func (noopMonitorWriter) Close() error               { return nil }

func waitForWatchdogUpdate(t *testing.T, store *captureConversationStore) ConversationWatchdogUpdate {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if store.watchdogUpdate.Watchdog != nil {
			return store.watchdogUpdate
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for watchdog update")
	return ConversationWatchdogUpdate{}
}

func TestSessionWatchdogMonitorWriter_PersistsHealthyLongRunningStateAfterOutput(t *testing.T) {
	prevPoll := sessionWatchdogPollInterval
	sessionWatchdogPollInterval = 5 * time.Millisecond
	defer func() { sessionWatchdogPollInterval = prevPoll }()

	store := &captureConversationStore{}
	writer := newSessionWatchdogMonitorWriter(context.Background(), noopMonitorWriter{}, store, nil, MonitorTurnMeta{
		AgentID:                  "agent-1",
		SessionID:                "sess-1",
		ScopeKey:                 "global",
		SessionScope:             "global",
		ConversationMode:         sessions.RuntimeModeSession.String(),
		WatchdogLongRunningAfter: 20 * time.Millisecond,
		WatchdogNoOutputAfter:    60 * time.Millisecond,
	})
	t.Cleanup(func() { _ = writer.Close() })

	writer.WriteStdout([]byte(`{"type":"assistant","message":"working"}`))
	update := waitForWatchdogUpdate(t, store)
	if update.Watchdog.State != conversationWatchdogStateHealthyLongRunning {
		t.Fatalf("watchdog state = %q, want %q", update.Watchdog.State, conversationWatchdogStateHealthyLongRunning)
	}
	if update.Watchdog.Action != conversationWatchdogActionTurnLongRunning {
		t.Fatalf("watchdog action = %q, want %q", update.Watchdog.Action, conversationWatchdogActionTurnLongRunning)
	}
	if update.Watchdog.Outcome != conversationWatchdogOutcomeObserved {
		t.Fatalf("watchdog outcome = %q, want %q", update.Watchdog.Outcome, conversationWatchdogOutcomeObserved)
	}
	if update.Watchdog.BlockingLayer != conversationWatchdogBlockingLayerSession {
		t.Fatalf("watchdog blocking_layer = %q, want %q", update.Watchdog.BlockingLayer, conversationWatchdogBlockingLayerSession)
	}
	if update.Watchdog.LastOutputAt == "" {
		t.Fatal("expected last_output_at to be recorded")
	}
}

func TestSessionWatchdogMonitorWriter_PersistsNoOutputStateWithoutStdout(t *testing.T) {
	prevPoll := sessionWatchdogPollInterval
	sessionWatchdogPollInterval = 5 * time.Millisecond
	defer func() { sessionWatchdogPollInterval = prevPoll }()

	store := &captureConversationStore{}
	writer := newSessionWatchdogMonitorWriter(context.Background(), noopMonitorWriter{}, store, nil, MonitorTurnMeta{
		AgentID:                  "agent-1",
		SessionID:                "sess-1",
		ScopeKey:                 "global",
		SessionScope:             "global",
		ConversationMode:         sessions.RuntimeModeSession.String(),
		WatchdogLongRunningAfter: 20 * time.Millisecond,
		WatchdogNoOutputAfter:    20 * time.Millisecond,
	})
	t.Cleanup(func() { _ = writer.Close() })

	update := waitForWatchdogUpdate(t, store)
	if update.Watchdog.State != conversationWatchdogStateNoOutput {
		t.Fatalf("watchdog state = %q, want %q", update.Watchdog.State, conversationWatchdogStateNoOutput)
	}
	if update.Watchdog.Action != conversationWatchdogActionSessionNoOutput {
		t.Fatalf("watchdog action = %q, want %q", update.Watchdog.Action, conversationWatchdogActionSessionNoOutput)
	}
	if update.Watchdog.Outcome != conversationWatchdogOutcomeWarningEmitted {
		t.Fatalf("watchdog outcome = %q, want %q", update.Watchdog.Outcome, conversationWatchdogOutcomeWarningEmitted)
	}
	if update.Watchdog.BlockingLayer != conversationWatchdogBlockingLayerSession {
		t.Fatalf("watchdog blocking_layer = %q, want %q", update.Watchdog.BlockingLayer, conversationWatchdogBlockingLayerSession)
	}
	if update.Watchdog.LastOutputAt != "" {
		t.Fatalf("last_output_at = %q, want empty", update.Watchdog.LastOutputAt)
	}
}
