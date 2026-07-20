package llm

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type noopMonitorWriter struct{}

func (noopMonitorWriter) WriteStdout([]byte)         {}
func (noopMonitorWriter) WriteStderr([]byte)         {}
func (noopMonitorWriter) WriteNotice(string, ...any) {}
func (noopMonitorWriter) Close() error               { return nil }

type noopMonitorWriterPtr struct{}

func (*noopMonitorWriterPtr) WriteStdout([]byte)         {}
func (*noopMonitorWriterPtr) WriteStderr([]byte)         {}
func (*noopMonitorWriterPtr) WriteNotice(string, ...any) {}
func (*noopMonitorWriterPtr) Close() error               { return nil }

type blockingWatchdogStore struct {
	started chan struct{}
}

func (s *blockingWatchdogStore) UpsertConversation(context.Context, ConversationRecord) error {
	return nil
}

func (s *blockingWatchdogStore) LoadActiveConversation(context.Context, string, string, string, string) (ConversationRecord, bool, error) {
	return ConversationRecord{}, false, nil
}

func (s *blockingWatchdogStore) UpdateLiveSessionWatchdog(ctx context.Context, _ ConversationWatchdogUpdate) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func waitForWatchdogUpdate(t *testing.T, store *captureConversationStore) ConversationWatchdogUpdate {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		update := store.capturedWatchdogUpdate()
		if update.Watchdog != nil {
			return update
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for watchdog update")
	return ConversationWatchdogUpdate{}
}

func TestSessionWatchdogMonitorWriter_PersistsHealthyLongRunningStateAfterOutput(t *testing.T) {
	prevPoll := sessionWatchdogPollInterval
	sessionWatchdogPollInterval = 5 * time.Millisecond

	store := &captureConversationStore{}
	writer, err := newSessionWatchdogMonitorWriter(llmTestWorkContext(t, context.Background()), noopMonitorWriter{}, store, nil, MonitorTurnMeta{
		AgentID:                  "agent-1",
		SessionID:                "sess-1",
		Memory:                   testMemory(),
		MemoryIdentity:           testMemoryIdentity("agent-1", "support/inst-1"),
		WatchdogLongRunningAfter: 20 * time.Millisecond,
		WatchdogNoOutputAfter:    time.Hour,
	})
	if err != nil {
		t.Fatalf("new session watchdog: %v", err)
	}
	defer func() {
		_ = writer.Close()
		sessionWatchdogPollInterval = prevPoll
	}()

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

func TestSessionWatchdogMonitorWriter_SkipsStatelessMemory(t *testing.T) {
	base := &noopMonitorWriterPtr{}
	writer, err := newSessionWatchdogMonitorWriter(context.Background(), base, &captureConversationStore{}, nil, MonitorTurnMeta{
		AgentID:                  "agent-1",
		SessionID:                "sess-1",
		Memory:                   agentmemory.Authored(false),
		WatchdogLongRunningAfter: 20 * time.Millisecond,
		WatchdogNoOutputAfter:    20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new stateless session watchdog: %v", err)
	}
	if writer != base {
		t.Fatalf("expected stateless memory to return base writer, got %T", writer)
	}
}

func TestSessionWatchdogMonitorWriter_PersistsNoOutputStateWithoutStdout(t *testing.T) {
	prevPoll := sessionWatchdogPollInterval
	sessionWatchdogPollInterval = 5 * time.Millisecond

	store := &captureConversationStore{}
	writer, err := newSessionWatchdogMonitorWriter(llmTestWorkContext(t, context.Background()), noopMonitorWriter{}, store, nil, MonitorTurnMeta{
		AgentID:                  "agent-1",
		SessionID:                "sess-1",
		Memory:                   testMemory(),
		MemoryIdentity:           testMemoryIdentity("agent-1", "support/inst-1"),
		WatchdogLongRunningAfter: 20 * time.Millisecond,
		WatchdogNoOutputAfter:    20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new session watchdog: %v", err)
	}
	defer func() {
		_ = writer.Close()
		sessionWatchdogPollInterval = prevPoll
	}()

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

func TestSessionWatchdogMonitorWriter_CloseCancelsBlockedWatchdogPersistence(t *testing.T) {
	prevPoll := sessionWatchdogPollInterval
	sessionWatchdogPollInterval = 5 * time.Millisecond
	defer func() { sessionWatchdogPollInterval = prevPoll }()

	store := &blockingWatchdogStore{started: make(chan struct{}, 1)}
	writer, err := newSessionWatchdogMonitorWriter(llmTestWorkContext(t, context.Background()), noopMonitorWriter{}, store, nil, MonitorTurnMeta{
		AgentID:                  "agent-1",
		SessionID:                "sess-1",
		Memory:                   testMemory(),
		MemoryIdentity:           testMemoryIdentity("agent-1", "support/inst-1"),
		WatchdogLongRunningAfter: 20 * time.Millisecond,
		WatchdogNoOutputAfter:    20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new session watchdog: %v", err)
	}

	select {
	case <-store.started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for watchdog persistence to start")
	}

	done := make(chan struct{})
	go func() {
		_ = writer.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Close did not unblock after canceling watchdog persistence")
	}
}

func TestSessionWatchdogJoinedOnGenerationRetire(t *testing.T) {
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "watchdog-runtime",
		BundleHash:        "watchdog-bundle",
	})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	ctx := worklifetime.WithOccurrence(context.Background(), owner)
	writer, err := newSessionWatchdogMonitorWriter(ctx, noopMonitorWriter{}, &captureConversationStore{}, nil, MonitorTurnMeta{
		AgentID:                  "agent-1",
		SessionID:                "sess-1",
		Memory:                   testMemory(),
		MemoryIdentity:           testMemoryIdentity("agent-1", "support/inst-1"),
		WatchdogLongRunningAfter: time.Hour,
		WatchdogNoOutputAfter:    2 * time.Hour,
	})
	if err != nil {
		t.Fatalf("new session watchdog: %v", err)
	}
	if got := owner.ActiveCount(); got != 1 {
		t.Fatalf("active watchdog work = %d, want 1", got)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := owner.RetireAndWait(waitCtx); err != nil {
		t.Fatalf("retire runtime occurrence: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close retired watchdog: %v", err)
	}
	if got := owner.ActiveCount(); got != 0 {
		t.Fatalf("active watchdog work after retirement = %d, want 0", got)
	}
	if _, err := process.Join(waitCtx); err != nil {
		t.Fatalf("join process: %v", err)
	}
}
