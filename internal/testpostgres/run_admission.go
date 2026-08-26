package testpostgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	runRegistryVersion = 1
	RunCapacityEnv     = "SWARM_TEST_RUN_SLOTS"
	RunWrapperEnv      = "SWARM_TEST_RUN_WRAPPER_ACTIVE"
	defaultRunCapacity = 1
	maxRunHistory      = 10
	maxRunHistoryTotal = 100
)

// RunCommand identifies one admitted go test invocation and its fallback ETA.
type RunCommand struct {
	Args             []string
	FallbackDuration time.Duration
}

// RunAdmission owns host/account-scoped ordering and capacity for test runs.
type RunAdmission struct {
	StateRoot       string
	Output          io.Writer
	PollInterval    time.Duration
	RefreshInterval time.Duration
	now             func() time.Time
	beforeSave      func(runRegistryDocument) error
}

// RunLease is the exact active slot possession retained by launched work.
type RunLease struct {
	admission *RunAdmission
	id        string
	slot      int
	command   string
	startedAt time.Time
	lock      *fileLock
	inherited bool
	closed    bool
}

type runRegistryDocument struct {
	Version      int                `json:"version"`
	Capacity     int                `json:"capacity"`
	NextSequence uint64             `json:"next_sequence"`
	Waiting      []runWaitingRecord `json:"waiting"`
	Active       []runActiveRecord  `json:"active"`
	History      []runHistoryRecord `json:"history"`
}

type runWaitingRecord struct {
	ID              string   `json:"id"`
	Sequence        uint64   `json:"sequence"`
	PID             int      `json:"pid"`
	Command         []string `json:"command"`
	CommandKey      string   `json:"command_key"`
	EnqueuedAtUTC   string   `json:"enqueued_at_utc"`
	ExpectedSeconds float64  `json:"expected_seconds,omitempty"`
}

type runActiveRecord struct {
	ID              string   `json:"id"`
	Sequence        uint64   `json:"sequence"`
	PID             int      `json:"pid"`
	Slot            int      `json:"slot"`
	Command         []string `json:"command"`
	CommandKey      string   `json:"command_key"`
	StartedAtUTC    string   `json:"started_at_utc"`
	ExpectedSeconds float64  `json:"expected_seconds,omitempty"`
}

type runHistoryRecord struct {
	CommandKey      string  `json:"command_key"`
	DurationSeconds float64 `json:"duration_seconds"`
	CompletedAtUTC  string  `json:"completed_at_utc"`
}

type runQueueSnapshot struct {
	Position            int
	Active              int
	Capacity            int
	EstimatedStart      time.Duration
	EstimatedCompletion time.Duration
	StartKnown          bool
	CompletionKnown     bool
}

func DefaultRunAdmission(output io.Writer) (*RunAdmission, error) {
	stateRoot, err := defaultServiceStateRoot()
	if err != nil {
		return nil, err
	}
	return NewRunAdmission(stateRoot, output), nil
}

func NewRunAdmission(stateRoot string, output io.Writer) *RunAdmission {
	if output == nil {
		output = io.Discard
	}
	return &RunAdmission{
		StateRoot:       stateRoot,
		Output:          output,
		PollInterval:    250 * time.Millisecond,
		RefreshInterval: 30 * time.Second,
		now:             time.Now,
	}
}

// RunCapacityFromEnvironment returns the single configured slot universe.
func RunCapacityFromEnvironment() (int, error) {
	raw, ok := os.LookupEnv(RunCapacityEnv)
	if !ok {
		return defaultRunCapacity, nil
	}
	capacity, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || capacity <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", RunCapacityEnv)
	}
	return capacity, nil
}

// Acquire registers a durable FIFO ticket and blocks until its slot is owned.
func (a *RunAdmission) Acquire(ctx context.Context, command RunCommand, capacity int) (*RunLease, error) {
	if ctx == nil {
		return nil, fmt.Errorf("run admission context is nil")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("run capacity must be positive")
	}
	if err := validateRunCommand(command.Args); err != nil {
		return nil, err
	}
	if err := a.initialize(); err != nil {
		return nil, fmt.Errorf("initialize run admission: %w", err)
	}

	id := uuid.NewString()
	ticketLock, acquired, err := acquireFileLock(a.ticketPath(id), false)
	if err != nil || !acquired {
		return nil, fmt.Errorf("acquire run ticket authority: %w", err)
	}
	registered := false
	defer func() {
		if !registered {
			_ = ticketLock.Close()
			_ = removeRunTicketAuthority(a.ticketPath(id))
		}
	}()

	now := a.nowUTC()
	commandKey := normalizedCommandKey(command.Args)
	err = a.withRegistry(func(doc *runRegistryDocument) error {
		occupied, err := a.reconcile(doc, "", capacity)
		if err != nil {
			return err
		}
		if err := agreeRunCapacity(doc, capacity, occupied); err != nil {
			return err
		}
		doc.NextSequence++
		doc.Waiting = append(doc.Waiting, runWaitingRecord{
			ID: id, Sequence: doc.NextSequence, PID: os.Getpid(),
			Command: append([]string(nil), command.Args...), CommandKey: commandKey,
			EnqueuedAtUTC:   now.Format(time.RFC3339Nano),
			ExpectedSeconds: expectedRunSeconds(*doc, commandKey, command.FallbackDuration),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("register run ticket: %w", err)
	}
	registered = true

	cleanupTicket := true
	defer func() {
		if cleanupTicket {
			_ = a.removeWaitingTicket(id)
			_ = ticketLock.Close()
			_ = removeRunTicketAuthority(a.ticketPath(id))
		}
	}()

	queuedAt := now
	nextReport := now
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("wait for test slot: %w", err)
		}

		var lease *RunLease
		var snapshot runQueueSnapshot
		err := a.withRegistry(func(doc *runRegistryDocument) error {
			occupied, err := a.reconcile(doc, id, capacity)
			if err != nil {
				return err
			}
			if err := agreeRunCapacity(doc, capacity, occupied); err != nil {
				return err
			}
			sortWaiting(doc.Waiting)
			index := waitingIndex(doc.Waiting, id)
			if index < 0 {
				return fmt.Errorf("run ticket %s disappeared while its authority is held", id)
			}
			if index == 0 {
				for slot := 0; slot < capacity; slot++ {
					if occupied[slot] {
						continue
					}
					slotLock, acquired, err := acquireFileLock(a.slotPath(slot), true)
					if err != nil {
						return err
					}
					if !acquired {
						occupied[slot] = true
						continue
					}
					record := doc.Waiting[index]
					startedAt := a.nowUTC()
					doc.Waiting = append(doc.Waiting[:index], doc.Waiting[index+1:]...)
					doc.Active = append(doc.Active, runActiveRecord{
						ID: record.ID, Sequence: record.Sequence, PID: record.PID, Slot: slot,
						Command: record.Command, CommandKey: record.CommandKey,
						StartedAtUTC: startedAt.Format(time.RFC3339Nano), ExpectedSeconds: record.ExpectedSeconds,
					})
					lease = &RunLease{admission: a, id: id, slot: slot, command: commandKey, startedAt: startedAt, lock: slotLock}
					return nil
				}
			}
			snapshot = a.snapshot(*doc, id, occupied)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("wait for test slot: %w", err)
		}
		if lease != nil {
			if err := ticketLock.Close(); err != nil {
				_ = lease.Complete(context.Background(), false)
				return nil, fmt.Errorf("release admitted run ticket authority: %w", err)
			}
			if err := removeRunTicketAuthority(a.ticketPath(id)); err != nil {
				_ = lease.Complete(context.Background(), false)
				return nil, err
			}
			cleanupTicket = false
			waited := a.nowUTC().Sub(queuedAt)
			fmt.Fprintf(a.Output, "Test slot acquired after %s. Starting %s\n", conciseDuration(waited), displayCommand(command.Args))
			return lease, nil
		}

		now = a.nowUTC()
		if !now.Before(nextReport) {
			a.printSnapshot(snapshot)
			nextReport = now.Add(a.refreshInterval())
		}
		if err := waitForContext(ctx, a.pollInterval()); err != nil {
			return nil, fmt.Errorf("wait for test slot: %w", err)
		}
	}
}

// InheritTo attaches the exact active slot lease to the child process.
func (l *RunLease) InheritTo(cmd *exec.Cmd) error {
	if l == nil || l.closed || l.lock == nil {
		return fmt.Errorf("active run lease is required")
	}
	if err := inheritFileLock(cmd, l.lock); err != nil {
		return err
	}
	l.inherited = true
	return nil
}

// Join waits until inherited test work releases the slot, then retains it for
// the supervisor so settlement cannot race a contender.
func (l *RunLease) Join(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("run join context is nil")
	}
	if l == nil || l.closed {
		return nil
	}
	if !l.inherited {
		return nil
	}
	var settlement *fileLock
	err := l.admission.withRegistry(func(doc *runRegistryDocument) error {
		index := activeIndex(doc.Active, l.id)
		if index < 0 || doc.Active[index].Slot != l.slot {
			return fmt.Errorf("active run %s is missing or changed", l.id)
		}
		var acquired bool
		var err error
		settlement, acquired, err = acquireFileLock(l.admission.settlementPath(l.slot), true)
		if err != nil {
			return err
		}
		if !acquired {
			return fmt.Errorf("run slot %d already has an active settlement handoff", l.slot)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if l.lock != nil {
		if err := l.lock.Drop(); err != nil {
			return errors.Join(err, settlement.Close())
		}
		l.lock = nil
	}
	probe, err := waitForFileLock(ctx, l.admission.slotPath(l.slot))
	if err != nil {
		return errors.Join(
			fmt.Errorf("wait for test work to release slot %d: %w; active evidence retained for reconciliation", l.slot, err),
			settlement.Close(),
		)
	}
	l.lock = probe
	l.inherited = false
	if err := settlement.Close(); err != nil {
		return fmt.Errorf("release run slot %d settlement handoff: %w", l.slot, err)
	}
	return nil
}

// Complete joins inherited work, publishes successful duration evidence, and releases the slot.
func (l *RunLease) Complete(ctx context.Context, success bool) error {
	if ctx == nil {
		return fmt.Errorf("run completion context is nil")
	}
	if l == nil || l.closed {
		return nil
	}
	if err := l.Join(ctx); err != nil {
		return err
	}
	l.closed = true
	now := l.admission.nowUTC()
	stateErr := l.admission.withRegistry(func(doc *runRegistryDocument) error {
		index := activeIndex(doc.Active, l.id)
		if index < 0 || doc.Active[index].Slot != l.slot {
			return fmt.Errorf("active run %s is missing or changed", l.id)
		}
		doc.Active = append(doc.Active[:index], doc.Active[index+1:]...)
		if success {
			duration := now.Sub(l.startedAt)
			if duration > 0 {
				doc.History = append(doc.History, runHistoryRecord{
					CommandKey: l.command, DurationSeconds: duration.Seconds(),
					CompletedAtUTC: now.Format(time.RFC3339Nano),
				})
				doc.History = boundedRunHistory(doc.History)
			}
		}
		if l.lock != nil {
			if err := l.lock.Close(); err != nil {
				return err
			}
			l.lock = nil
		}
		return nil
	})
	if l.lock != nil {
		if l.inherited {
			stateErr = errors.Join(stateErr, l.lock.Drop())
		} else {
			stateErr = errors.Join(stateErr, l.lock.Close())
		}
		l.lock = nil
	}
	return stateErr
}

func (a *RunAdmission) initialize() error {
	if strings.TrimSpace(a.StateRoot) == "" {
		return fmt.Errorf("run admission state root is empty")
	}
	if _, err := os.Lstat(a.StateRoot); err == nil {
		if err := validatePrivateStateRoot(a.StateRoot); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, dir := range []string{a.StateRoot, filepath.Join(a.StateRoot, "run-tickets"), filepath.Join(a.StateRoot, "run-slots")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	return validatePrivateStateRoot(a.StateRoot)
}

func (a *RunAdmission) withRegistry(fn func(*runRegistryDocument) error) error {
	lock, acquired, err := acquireFileLock(filepath.Join(a.StateRoot, "runs-v1.lock"), false)
	if err != nil || !acquired {
		return fmt.Errorf("acquire run registry lock: %w", err)
	}
	defer lock.Close()
	doc, err := a.loadRegistry()
	if err != nil {
		return err
	}
	original := doc
	if err := fn(&doc); err != nil {
		return err
	}
	if reflect.DeepEqual(original, doc) {
		return nil
	}
	if a.beforeSave != nil {
		if err := a.beforeSave(doc); err != nil {
			return err
		}
	}
	return a.saveRegistry(doc)
}

func (a *RunAdmission) loadRegistry() (runRegistryDocument, error) {
	path := filepath.Join(a.StateRoot, "runs-v1.json")
	if err := validateExistingAuthorityFile(path); err != nil {
		return runRegistryDocument{}, err
	}
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return runRegistryDocument{Version: runRegistryVersion}, nil
	}
	if err != nil {
		return runRegistryDocument{}, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var doc runRegistryDocument
	if err := decoder.Decode(&doc); err != nil {
		return runRegistryDocument{}, fmt.Errorf("decode run registry: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return runRegistryDocument{}, fmt.Errorf("decode run registry trailing data")
	}
	if err := validateRunRegistry(doc); err != nil {
		return runRegistryDocument{}, err
	}
	return doc, nil
}

func (a *RunAdmission) saveRegistry(doc runRegistryDocument) error {
	if err := validateRunRegistry(doc); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(a.StateRoot, "runs-v1.json")
	tmp := path + ".tmp-" + uuid.NewString()
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return validateExistingAuthorityFile(path)
}

func (a *RunAdmission) reconcile(doc *runRegistryDocument, callerID string, requestedCapacity int) ([]bool, error) {
	for index := 0; index < len(doc.Waiting); {
		record := doc.Waiting[index]
		if record.ID == callerID {
			index++
			continue
		}
		probe, acquired, err := acquireFileLock(a.ticketPath(record.ID), true)
		if err != nil {
			return nil, err
		}
		if !acquired {
			index++
			continue
		}
		if err := probe.Close(); err != nil {
			return nil, err
		}
		if err := removeRunTicketAuthority(a.ticketPath(record.ID)); err != nil {
			return nil, err
		}
		doc.Waiting = append(doc.Waiting[:index], doc.Waiting[index+1:]...)
	}

	probeCapacity := doc.Capacity
	if requestedCapacity > probeCapacity {
		probeCapacity = requestedCapacity
	}
	occupied := make([]bool, probeCapacity)
	activeBySlot := make(map[int]int, len(doc.Active))
	for index, record := range doc.Active {
		if previous, exists := activeBySlot[record.Slot]; exists {
			return nil, fmt.Errorf("run slots contain duplicate active records at slot %d (%d and %d)", record.Slot, previous, index)
		}
		activeBySlot[record.Slot] = index
	}
	for slot := 0; slot < probeCapacity; slot++ {
		settlement, acquired, err := acquireFileLock(a.settlementPath(slot), true)
		if err != nil {
			return nil, err
		}
		if !acquired {
			occupied[slot] = true
			continue
		}
		if err := settlement.Close(); err != nil {
			return nil, err
		}
		probe, acquired, err := acquireFileLock(a.slotPath(slot), true)
		if err != nil {
			return nil, err
		}
		if !acquired {
			occupied[slot] = true
			continue
		}
		if err := probe.Close(); err != nil {
			return nil, err
		}
		if index, exists := activeBySlot[slot]; exists {
			doc.Active[index].ID = ""
		}
	}
	doc.Active = compactActive(doc.Active)
	return occupied, nil
}

func agreeRunCapacity(doc *runRegistryDocument, requested int, occupied []bool) error {
	live := len(doc.Waiting) > 0 || len(doc.Active) > 0
	for _, value := range occupied {
		live = live || value
	}
	if doc.Capacity == 0 || !live {
		doc.Capacity = requested
		return nil
	}
	if doc.Capacity != requested {
		return fmt.Errorf("live test queue capacity is %d but this invocation requests %d via %s", doc.Capacity, requested, RunCapacityEnv)
	}
	return nil
}

func (a *RunAdmission) snapshot(doc runRegistryDocument, id string, occupied []bool) runQueueSnapshot {
	sortWaiting(doc.Waiting)
	index := waitingIndex(doc.Waiting, id)
	snapshot := runQueueSnapshot{Position: index + 1, Capacity: doc.Capacity}
	for slot := 0; slot < doc.Capacity && slot < len(occupied); slot++ {
		if occupied[slot] {
			snapshot.Active++
		}
	}
	if index < 0 || doc.Capacity <= 0 {
		return snapshot
	}
	availability := make([]time.Duration, doc.Capacity)
	known := make([]bool, doc.Capacity)
	for slot := range known {
		known[slot] = !occupied[slot]
	}
	activeBySlot := make(map[int]runActiveRecord, len(doc.Active))
	for _, record := range doc.Active {
		activeBySlot[record.Slot] = record
	}
	now := a.nowUTC()
	for slot := 0; slot < doc.Capacity; slot++ {
		if !occupied[slot] {
			continue
		}
		record, ok := activeBySlot[slot]
		if !ok || record.ExpectedSeconds <= 0 {
			known[slot] = false
			continue
		}
		started, err := time.Parse(time.RFC3339Nano, record.StartedAtUTC)
		if err != nil {
			known[slot] = false
			continue
		}
		remaining := time.Duration(record.ExpectedSeconds*float64(time.Second)) - now.Sub(started)
		if remaining < 0 {
			remaining = 0
		}
		availability[slot] = remaining
		known[slot] = true
	}
	for waitingIndex, record := range doc.Waiting {
		slot := earliestKnownSlot(availability, known)
		if slot < 0 {
			return snapshot
		}
		if waitingIndex == index {
			snapshot.EstimatedStart = availability[slot]
			snapshot.StartKnown = true
			if record.ExpectedSeconds > 0 {
				snapshot.EstimatedCompletion = snapshot.EstimatedStart + time.Duration(record.ExpectedSeconds*float64(time.Second))
				snapshot.CompletionKnown = true
			}
			return snapshot
		}
		if record.ExpectedSeconds <= 0 {
			known[slot] = false
			continue
		}
		availability[slot] += time.Duration(record.ExpectedSeconds * float64(time.Second))
	}
	return snapshot
}

func (a *RunAdmission) printSnapshot(snapshot runQueueSnapshot) {
	fmt.Fprintln(a.Output, "Test capacity is busy.")
	fmt.Fprintf(a.Output, "Queue position: %d\n", snapshot.Position)
	fmt.Fprintf(a.Output, "Active slots: %d/%d\n", snapshot.Active, snapshot.Capacity)
	if snapshot.StartKnown {
		fmt.Fprintf(a.Output, "Estimated start: %s\n", conciseDuration(snapshot.EstimatedStart))
	} else {
		fmt.Fprintln(a.Output, "Estimated start: unknown")
	}
	if snapshot.CompletionKnown {
		fmt.Fprintf(a.Output, "Estimated completion: %s\n", conciseDuration(snapshot.EstimatedCompletion))
	} else {
		fmt.Fprintln(a.Output, "Estimated completion: unknown")
	}
	fmt.Fprintf(a.Output, "Waiting for a test slot... updates every %s\n", conciseDuration(a.refreshInterval()))
}

func (a *RunAdmission) removeWaitingTicket(id string) error {
	return a.withRegistry(func(doc *runRegistryDocument) error {
		index := waitingIndex(doc.Waiting, id)
		if index >= 0 {
			doc.Waiting = append(doc.Waiting[:index], doc.Waiting[index+1:]...)
		}
		return nil
	})
}

func removeRunTicketAuthority(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove retired run ticket authority %q: %w", path, err)
	}
	return nil
}

func validateRunRegistry(doc runRegistryDocument) error {
	if doc.Version != runRegistryVersion {
		return fmt.Errorf("unsupported run registry version %d", doc.Version)
	}
	if doc.Capacity < 0 {
		return fmt.Errorf("run registry capacity must not be negative")
	}
	if doc.Capacity == 0 && (doc.NextSequence != 0 || len(doc.Waiting) != 0 || len(doc.Active) != 0 || len(doc.History) != 0) {
		return fmt.Errorf("zero-capacity run registry must be empty")
	}
	ids := make(map[string]bool)
	sequences := make(map[uint64]bool)
	var previousWaitingSequence uint64
	for _, record := range doc.Waiting {
		if err := validateRunRecord(record.ID, record.Sequence, record.PID, record.Command, record.CommandKey, record.EnqueuedAtUTC, record.ExpectedSeconds); err != nil {
			return err
		}
		if ids[record.ID] || sequences[record.Sequence] {
			return fmt.Errorf("duplicate run waiting identity or sequence")
		}
		if record.Sequence <= previousWaitingSequence || record.Sequence > doc.NextSequence {
			return fmt.Errorf("run waiting sequence %d is out of order or beyond next_sequence", record.Sequence)
		}
		previousWaitingSequence = record.Sequence
		ids[record.ID], sequences[record.Sequence] = true, true
	}
	slots := make(map[int]bool)
	for _, record := range doc.Active {
		if err := validateRunRecord(record.ID, record.Sequence, record.PID, record.Command, record.CommandKey, record.StartedAtUTC, record.ExpectedSeconds); err != nil {
			return err
		}
		if doc.Capacity <= 0 || record.Slot < 0 || record.Slot >= doc.Capacity {
			return fmt.Errorf("active run %s has invalid slot %d", record.ID, record.Slot)
		}
		if ids[record.ID] || sequences[record.Sequence] || slots[record.Slot] {
			return fmt.Errorf("duplicate active run identity, sequence, or slot")
		}
		if record.Sequence > doc.NextSequence {
			return fmt.Errorf("active run sequence %d is beyond next_sequence", record.Sequence)
		}
		ids[record.ID], sequences[record.Sequence], slots[record.Slot] = true, true, true
	}
	for _, record := range doc.History {
		if record.CommandKey == "" || !validSeconds(record.DurationSeconds) || record.DurationSeconds <= 0 {
			return fmt.Errorf("invalid run duration history record")
		}
		if _, err := time.Parse(time.RFC3339Nano, record.CompletedAtUTC); err != nil {
			return fmt.Errorf("invalid run duration completion time: %w", err)
		}
	}
	return nil
}

func validateRunRecord(id string, sequence uint64, pid int, command []string, key, timestamp string, expected float64) error {
	if _, err := uuid.Parse(id); err != nil || sequence == 0 || pid <= 0 {
		return fmt.Errorf("invalid run registry identity %q", id)
	}
	if err := validateRunCommand(command); err != nil {
		return err
	}
	if normalizedCommandKey(command) != key {
		return fmt.Errorf("run command key mismatch for %s", id)
	}
	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		return fmt.Errorf("invalid run timestamp for %s: %w", id, err)
	}
	if !validSeconds(expected) {
		return fmt.Errorf("invalid expected duration for %s", id)
	}
	return nil
}

func validateRunCommand(command []string) error {
	if len(command) < 2 || command[0] != "go" || command[1] != "test" {
		return fmt.Errorf("run admission accepts only go test commands")
	}
	for _, arg := range command {
		if strings.ContainsRune(arg, '\x00') {
			return fmt.Errorf("go test argument contains NUL")
		}
	}
	return nil
}

func validSeconds(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func normalizedCommandKey(command []string) string {
	digest := sha256.Sum256([]byte(strings.Join(command, "\x00")))
	return hex.EncodeToString(digest[:])
}

func expectedRunSeconds(doc runRegistryDocument, commandKey string, fallback time.Duration) float64 {
	var values []float64
	for _, record := range doc.History {
		if record.CommandKey == commandKey {
			values = append(values, record.DurationSeconds)
		}
	}
	if len(values) == 0 {
		if fallback > 0 {
			return fallback.Seconds()
		}
		return 0
	}
	sort.Float64s(values)
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
}

func boundedRunHistory(history []runHistoryRecord) []runHistoryRecord {
	counts := make(map[string]int)
	keep := make([]bool, len(history))
	for index := len(history) - 1; index >= 0; index-- {
		key := history[index].CommandKey
		if counts[key] < maxRunHistory {
			keep[index] = true
			counts[key]++
		}
	}
	result := make([]runHistoryRecord, 0, len(history))
	for index, record := range history {
		if keep[index] {
			result = append(result, record)
		}
	}
	if len(result) > maxRunHistoryTotal {
		result = result[len(result)-maxRunHistoryTotal:]
	}
	return result
}

func compactActive(records []runActiveRecord) []runActiveRecord {
	result := records[:0]
	for _, record := range records {
		if record.ID != "" {
			result = append(result, record)
		}
	}
	return result
}

func sortWaiting(records []runWaitingRecord) {
	sort.Slice(records, func(i, j int) bool { return records[i].Sequence < records[j].Sequence })
}

func waitingIndex(records []runWaitingRecord, id string) int {
	for index, record := range records {
		if record.ID == id {
			return index
		}
	}
	return -1
}

func activeIndex(records []runActiveRecord, id string) int {
	for index, record := range records {
		if record.ID == id {
			return index
		}
	}
	return -1
}

func earliestKnownSlot(availability []time.Duration, known []bool) int {
	best := -1
	for index := range availability {
		if !known[index] {
			return -1
		}
		if best < 0 || availability[index] < availability[best] {
			best = index
		}
	}
	return best
}

func (a *RunAdmission) ticketPath(id string) string {
	return filepath.Join(a.StateRoot, "run-tickets", id+".lock")
}

func (a *RunAdmission) slotPath(slot int) string {
	return filepath.Join(a.StateRoot, "run-slots", strconv.Itoa(slot)+".lock")
}

func (a *RunAdmission) settlementPath(slot int) string {
	return filepath.Join(a.StateRoot, "run-slots", strconv.Itoa(slot)+".settlement.lock")
}

func (a *RunAdmission) nowUTC() time.Time {
	if a.now == nil {
		return time.Now().UTC()
	}
	return a.now().UTC()
}

func (a *RunAdmission) pollInterval() time.Duration {
	if a.PollInterval <= 0 {
		return 250 * time.Millisecond
	}
	return a.PollInterval
}

func (a *RunAdmission) refreshInterval() time.Duration {
	if a.RefreshInterval <= 0 {
		return 30 * time.Second
	}
	return a.RefreshInterval
}

func waitForContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func displayCommand(command []string) string {
	values := make([]string, len(command))
	for index, arg := range command {
		if arg != "" && !strings.ContainsAny(arg, " \t\n\"'\\") {
			values[index] = arg
			continue
		}
		values[index] = strconv.Quote(arg)
	}
	return strings.Join(values, " ")
}

func conciseDuration(duration time.Duration) string {
	if duration < 0 {
		duration = 0
	}
	if duration < time.Second {
		return duration.Round(time.Millisecond).String()
	}
	return duration.Round(time.Second).String()
}
