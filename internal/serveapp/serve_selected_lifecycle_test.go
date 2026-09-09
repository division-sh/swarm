package serveapp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
)

type observedTerminalJoinContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *observedTerminalJoinContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

type observedTerminalStore struct {
	activatedSelectedStore
	mu       sync.Mutex
	attempts int
}

func (s *observedTerminalStore) CloseActivated(receipt *worklifetime.ProcessJoinReceipt) error {
	s.mu.Lock()
	s.attempts++
	s.mu.Unlock()
	return s.activatedSelectedStore.CloseActivated(receipt)
}

func TestTerminalStoreReleaseJoinsDelayedAccessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected *selectedStoreOwner
			if backend == "sqlite" {
				selected = openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "terminal.db"), nil)
			} else {
				dsn, db, _ := testutil.StartPostgres(t)
				selected = openSelectedPostgresOwner(t, dsn, db, nil)
			}
			db, _, _ := selectedRuntimeStoreForTest(t, projectServeRuntimePersistence(selected))
			process := worklifetime.NewProcess()
			observed := &observedTerminalStore{activatedSelectedStore: selected}
			lifecycle, err := activateServeLifecycle(observed, process)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := process.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			joinCtx := &observedTerminalJoinContext{Context: cancelled, entered: make(chan struct{})}
			completed := make(chan error, 1)
			go func() { completed <- lifecycle.Finalize(joinCtx, nil) }()
			<-joinCtx.entered
			observed.mu.Lock()
			attempts := observed.attempts
			observed.mu.Unlock()
			if attempts != 0 {
				t.Errorf("store close attempted before accepted work settled: %d", attempts)
			}
			var one int
			if err := db.QueryRowContext(context.WithoutCancel(lease.Context()), "SELECT 1").Scan(&one); err != nil || one != 1 {
				t.Errorf("delayed accepted store access lost its capability: value=%d error=%v", one, err)
			}
			if err := lease.Done(); err != nil {
				t.Error(err)
			}
			if err := <-completed; !errors.Is(err, context.Canceled) {
				t.Errorf("shutdown did not retain its expired join diagnostic: %v", err)
			}
			if err := db.PingContext(context.Background()); err == nil {
				t.Error("successfully joined selected store remained open")
			}
			observed.mu.Lock()
			defer observed.mu.Unlock()
			if observed.attempts != 1 {
				t.Errorf("store release attempts = %d, want one after join", observed.attempts)
			}
		})
	}
}

func TestServeActivatesSelectedStoreBeforeProcessOwnedConstruction(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read serve owner: %v", err)
	}
	activation := bytes.Index(source, []byte("activateServeLifecycle(storeLifetime, processWorkOwner)"))
	if activation < 0 {
		t.Fatal("serve selected-store activation owner is missing")
	}
	for _, marker := range []string{
		"loadedBundles, err = loadRuntimeCompositionBundles",
		"loadedBundles, err = loadServeRecoveredResetSources",
		"buildServeRuntimeBundleContext(request)",
		"startServeOwnershipWatch(ownershipWatchCtx",
		"installServeSourceSet(ctx, processCapability",
		"processCapability.IssueGenerationGrant(ctx",
	} {
		consumer := bytes.Index(source, []byte(marker))
		if consumer < 0 || consumer < activation {
			t.Fatalf("process-owned consumer %q is not ordered after selected-store activation", marker)
		}
	}
	acquisition := bytes.Index(source, []byte("processCapability, err = stores.StartupOwnership().AcquireProcessCapability"))
	load := bytes.Index(source, []byte("loadedBundles, err = loadRuntimeCompositionBundles"))
	if acquisition < activation || acquisition >= load {
		t.Fatal("source loading must follow retained process authority acquisition")
	}
}

type selectedLifecycleStoreProbe struct {
	mu             sync.Mutex
	process        *worklifetime.Process
	capability     *selectedLifecycleCapabilityProbe
	closeAttempts  int
	closed         bool
	failFirstClose bool
}

func (s *selectedLifecycleStoreProbe) Activate(process *worklifetime.Process) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.process != nil {
		return errors.New("already activated")
	}
	s.process = process
	return nil
}

func (s *selectedLifecycleStoreProbe) CloseActivated(receipt *worklifetime.ProcessJoinReceipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeAttempts++
	if s.process == nil {
		return errors.New("not activated")
	}
	if err := s.process.ValidateJoinReceipt(receipt); err != nil {
		return err
	}
	if s.capability != nil && s.capability.live() {
		return errors.New("selected store closed while process capability remained live")
	}
	if s.failFirstClose && s.closeAttempts == 1 {
		return errors.New("injected transient close failure")
	}
	s.closed = true
	return nil
}

func (s *selectedLifecycleStoreProbe) snapshot() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeAttempts, s.closed
}

type selectedLifecycleCapabilityProbe struct {
	mu               sync.Mutex
	releaseAttempts  int
	failFirstRelease bool
	terminal         bool
}

func (c *selectedLifecycleCapabilityProbe) Release(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseAttempts++
	if c.failFirstRelease && c.releaseAttempts == 1 {
		return errors.New("injected transient release failure")
	}
	c.terminal = true
	return nil
}

func (c *selectedLifecycleCapabilityProbe) TerminalResult() (runtimestartupownership.TerminalResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.terminal {
		return runtimestartupownership.TerminalResult{}, false
	}
	return runtimestartupownership.TerminalResult{Cause: runtimestartupownership.TerminalReleased}, true
}

func (c *selectedLifecycleCapabilityProbe) live() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.terminal
}

func (c *selectedLifecycleCapabilityProbe) attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.releaseAttempts
}

func TestActivatedServeLifecycleTimeoutReportsButWaitsForExactWork(t *testing.T) {
	process := worklifetime.NewProcess()
	store := &selectedLifecycleStoreProbe{}
	capability := &selectedLifecycleCapabilityProbe{}
	store.capability = capability
	lifecycle, err := activateServeLifecycle(store, process)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := lifecycle.SetProcessCapability(capability); err != nil {
		t.Fatalf("set capability: %v", err)
	}
	lease, err := process.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin process work: %v", err)
	}

	done := make(chan error, 1)
	joinCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	go func() {
		done <- lifecycle.Finalize(joinCtx, errors.New("prior cleanup failure"))
	}()
	time.Sleep(30 * time.Millisecond)
	if capability.attempts() != 0 {
		t.Fatalf("capability released before active work joined: attempts=%d", capability.attempts())
	}
	if attempts, closed := store.snapshot(); attempts != 0 || closed {
		t.Fatalf("store closed before active work joined: attempts=%d closed=%v", attempts, closed)
	}
	if err := lease.Done(); err != nil {
		t.Fatalf("settle process work: %v", err)
	}

	result := <-done
	for _, want := range []string{"prior cleanup failure", "process work join exceeded shutdown budget"} {
		if result == nil || !strings.Contains(result.Error(), want) {
			t.Fatalf("finalize error = %v, want %q", result, want)
		}
	}
	if capability.attempts() != 1 {
		t.Fatalf("release attempts = %d, want 1", capability.attempts())
	}
	if attempts, closed := store.snapshot(); attempts != 1 || !closed {
		t.Fatalf("store close = attempts:%d closed:%v, want exact close", attempts, closed)
	}
}

func TestActivatedServeLifecycleRetriesRetainedCapabilityAndExactClose(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			process := worklifetime.NewProcess()
			capability := &selectedLifecycleCapabilityProbe{failFirstRelease: true}
			store := &selectedLifecycleStoreProbe{capability: capability, failFirstClose: true}
			lifecycle, err := activateServeLifecycle(store, process)
			if err != nil {
				t.Fatalf("activate: %v", err)
			}
			if err := lifecycle.SetProcessCapability(capability); err != nil {
				t.Fatalf("set capability: %v", err)
			}
			result := lifecycle.Finalize(context.Background(), nil)
			for _, want := range []string{"injected transient release failure", "injected transient close failure"} {
				if result == nil || !strings.Contains(result.Error(), want) {
					t.Fatalf("finalize error = %v, want retained diagnostic %q", result, want)
				}
			}
			if capability.attempts() != 2 {
				t.Fatalf("release attempts = %d, want 2", capability.attempts())
			}
			if attempts, closed := store.snapshot(); attempts != 2 || !closed {
				t.Fatalf("store close = attempts:%d closed:%v, want retry then exact close", attempts, closed)
			}
		})
	}
}
