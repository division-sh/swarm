package testpostgres

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type runAcquireResult struct {
	name  string
	lease *RunLease
	err   error
}

func TestRunAdmissionFIFOWithoutBarging(t *testing.T) {
	root := t.TempDir()
	firstAdmission := testRunAdmission(root, nil)
	first := acquireTestRun(t, firstAdmission, context.Background(), "first", 1)

	results := make(chan runAcquireResult, 2)
	start := func(name string) {
		go func() {
			lease, err := testRunAdmission(root, nil).Acquire(context.Background(), testRunCommand(name), 1)
			results <- runAcquireResult{name: name, lease: lease, err: err}
		}()
	}
	start("second")
	waitForWaitingRuns(t, firstAdmission, 1)
	start("third")
	waitForWaitingRuns(t, firstAdmission, 2)

	if err := first.Complete(true); err != nil {
		t.Fatal(err)
	}
	second := waitForRunResult(t, results)
	if second.err != nil || second.name != "second" {
		t.Fatalf("first admitted waiter = %q err=%v, want second", second.name, second.err)
	}
	select {
	case got := <-results:
		t.Fatalf("third contender barged while second held the slot: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
	if err := second.lease.Complete(true); err != nil {
		t.Fatal(err)
	}
	third := waitForRunResult(t, results)
	if third.err != nil || third.name != "third" {
		t.Fatalf("second admitted waiter = %q err=%v, want third", third.name, third.err)
	}
	if err := third.lease.Complete(true); err != nil {
		t.Fatal(err)
	}
}

func TestRunAdmissionQueuedCancellationRemovesOnlyCaller(t *testing.T) {
	root := t.TempDir()
	admission := testRunAdmission(root, nil)
	active := acquireTestRun(t, admission, context.Background(), "active", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := testRunAdmission(root, nil).Acquire(ctx, testRunCommand("cancelled"), 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire() error = %v, want deadline", err)
	}
	doc, err := admission.loadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Waiting) != 0 || len(doc.Active) != 1 || doc.Active[0].ID != active.id {
		t.Fatalf("registry after cancellation = waiting=%d active=%+v", len(doc.Waiting), doc.Active)
	}
	entries, err := os.ReadDir(filepath.Join(root, "run-tickets"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("retired ticket authority survives: %v", entries)
	}
	if err := active.Complete(false); err != nil {
		t.Fatal(err)
	}
}

func TestRunAdmissionRejectsConflictingLiveCapacity(t *testing.T) {
	root := t.TempDir()
	admission := testRunAdmission(root, nil)
	active := acquireTestRun(t, admission, context.Background(), "active", 1)
	defer active.Complete(false)

	_, err := testRunAdmission(root, nil).Acquire(context.Background(), testRunCommand("conflict"), 2)
	if err == nil || !strings.Contains(err.Error(), "capacity is 1") {
		t.Fatalf("Acquire() error = %v, want capacity mismatch", err)
	}
}

func TestRunAdmissionPublishesOnlySuccessfulDuration(t *testing.T) {
	root := t.TempDir()
	admission := testRunAdmission(root, nil)
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	admission.now = func() time.Time { return now }

	failed := acquireTestRun(t, admission, context.Background(), "same", 1)
	now = now.Add(10 * time.Second)
	if err := failed.Complete(false); err != nil {
		t.Fatal(err)
	}
	doc, err := admission.loadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.History) != 0 {
		t.Fatalf("failed run published history: %+v", doc.History)
	}

	succeeded := acquireTestRun(t, admission, context.Background(), "same", 1)
	now = now.Add(12 * time.Second)
	if err := succeeded.Complete(true); err != nil {
		t.Fatal(err)
	}
	doc, err = admission.loadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.History) != 1 || doc.History[0].DurationSeconds != 12 {
		t.Fatalf("successful history = %+v", doc.History)
	}
}

func TestRunAdmissionReportsPositionETAAndDoesNotRewriteWhilePolling(t *testing.T) {
	root := t.TempDir()
	admission := testRunAdmission(root, nil)
	activeCommand := testRunCommand("active")
	activeCommand.FallbackDuration = 2 * time.Minute
	active, err := admission.Acquire(context.Background(), activeCommand, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Complete(false)

	var output bytes.Buffer
	waitingAdmission := testRunAdmission(root, &output)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := waitingAdmission.Acquire(ctx, RunCommand{Args: []string{"go", "test", "./..."}, FallbackDuration: 3 * time.Minute}, 1)
		result <- err
	}()
	waitForWaitingRuns(t, admission, 1)
	statePath := filepath.Join(root, "runs-v1.json")
	before, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	after, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("polling rewrote registry: before=%s after=%s", before.ModTime(), after.ModTime())
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued result = %v", err)
	}
	text := output.String()
	for _, want := range []string{"Test capacity is busy.", "Queue position: 1", "Active slots: 1/1", "Estimated start:", "Estimated completion:", "updates every"} {
		if !strings.Contains(text, want) {
			t.Fatalf("queue output missing %q:\n%s", want, text)
		}
	}
}

func TestRunAdmissionReconcilesFreeStaleActiveAndWaitingRecords(t *testing.T) {
	root := t.TempDir()
	admission := testRunAdmission(root, nil)
	if err := admission.initialize(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	staleWaitingID := "11111111-1111-4111-8111-111111111111"
	staleActiveID := "22222222-2222-4222-8222-222222222222"
	command := []string{"go", "test", "./..."}
	doc := runRegistryDocument{
		Version: runRegistryVersion, Capacity: 1, NextSequence: 2,
		Waiting: []runWaitingRecord{{ID: staleWaitingID, Sequence: 2, PID: os.Getpid(), Command: command, CommandKey: normalizedCommandKey(command), EnqueuedAtUTC: now}},
		Active:  []runActiveRecord{{ID: staleActiveID, Sequence: 1, PID: os.Getpid(), Slot: 0, Command: command, CommandKey: normalizedCommandKey(command), StartedAtUTC: now}},
	}
	if err := admission.saveRegistry(doc); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(admission.ticketPath(staleWaitingID), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	lease := acquireTestRun(t, admission, context.Background(), "new", 1)
	if err := lease.Complete(false); err != nil {
		t.Fatal(err)
	}
	doc, err := admission.loadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Waiting) != 0 || len(doc.Active) != 0 {
		t.Fatalf("stale rows survive: waiting=%+v active=%+v", doc.Waiting, doc.Active)
	}
}

func TestRunAdmissionFailsClosedOnCorruptRegistry(t *testing.T) {
	root := t.TempDir()
	admission := testRunAdmission(root, nil)
	if err := admission.initialize(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "runs-v1.json"), []byte(`{"version":1,"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := admission.Acquire(context.Background(), testRunCommand("corrupt"), 1)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("Acquire() error = %v, want fail-closed decode", err)
	}
}

func TestRunCapacityFromEnvironment(t *testing.T) {
	t.Setenv(RunCapacityEnv, "3")
	if got, err := RunCapacityFromEnvironment(); err != nil || got != 3 {
		t.Fatalf("capacity = %d err=%v", got, err)
	}
	t.Setenv(RunCapacityEnv, "0")
	if _, err := RunCapacityFromEnvironment(); err == nil {
		t.Fatal("zero capacity accepted")
	}
}

func TestRunAndServiceLeasesAttachExactAuthorityToChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows handle attachment is compile-proven separately")
	}
	root := t.TempDir()
	admission := testRunAdmission(root, nil)
	runLease := acquireTestRun(t, admission, context.Background(), "attach", 1)
	defer runLease.Complete(false)
	serviceLock, acquired, err := acquireFileLock(filepath.Join(root, "service.lock"), false)
	if err != nil || !acquired {
		t.Fatalf("acquire service lock: acquired=%v err=%v", acquired, err)
	}
	defer serviceLock.Close()
	service := &Service{lease: serviceLock}
	cmd := exec.Command("true")
	if err := runLease.InheritTo(cmd); err != nil {
		t.Fatal(err)
	}
	if err := service.InheritLeaseTo(cmd); err != nil {
		t.Fatal(err)
	}
	if len(cmd.ExtraFiles) != 2 || cmd.ExtraFiles[0] != runLease.lock.File() || cmd.ExtraFiles[1] != serviceLock.File() {
		t.Fatalf("inherited files = %v, want exact run and service authorities", cmd.ExtraFiles)
	}
}

func testRunAdmission(root string, output *bytes.Buffer) *RunAdmission {
	_ = os.Chmod(root, 0o700)
	var writer = ioDiscardBuffer(output)
	admission := NewRunAdmission(root, writer)
	admission.PollInterval = 2 * time.Millisecond
	admission.RefreshInterval = 20 * time.Millisecond
	return admission
}

func ioDiscardBuffer(output *bytes.Buffer) *bytes.Buffer {
	if output != nil {
		return output
	}
	return &bytes.Buffer{}
}

func testRunCommand(name string) RunCommand {
	return RunCommand{Args: []string{"go", "test", "./" + name}}
}

func acquireTestRun(t *testing.T, admission *RunAdmission, ctx context.Context, name string, capacity int) *RunLease {
	t.Helper()
	lease, err := admission.Acquire(ctx, testRunCommand(name), capacity)
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func waitForWaitingRuns(t *testing.T, admission *RunAdmission, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		doc, err := admission.loadRegistry()
		if err == nil && len(doc.Waiting) == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d queued runs", count)
}

func waitForRunResult(t *testing.T, results <-chan runAcquireResult) runAcquireResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for admitted run")
		return runAcquireResult{}
	}
}
