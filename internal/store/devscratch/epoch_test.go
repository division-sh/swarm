package devscratch_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeconstruction "github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/devscratch"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

const (
	helperModeEnv = "SWARM_DEV_SCRATCH_HELPER_MODE"
	helperRootEnv = "SWARM_DEV_SCRATCH_HELPER_ROOT"
)

func TestDevScratchEpochHelperProcess(t *testing.T) {
	mode := os.Getenv(helperModeEnv)
	if mode == "" {
		return
	}
	coordinate, err := devscratch.Resolve(os.Getenv(helperRootEnv))
	if err != nil {
		t.Fatal(err)
	}
	authority, err := devscratch.Acquire(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "after-open" {
		if err := authority.PrepareFreshStore(); err != nil {
			t.Fatal(err)
		}
		selected, _, err := storeconstruction.OpenSQLiteRuntimeWithOwnershipBinding(coordinate.DatabasePath)
		if err != nil {
			t.Fatal(err)
		}
		_ = selected
	}
	fmt.Println("READY")
	_ = os.Stdout.Sync()
	select {}
}

func TestDevScratchEpochDefaultContextOwnsCanonicalCoordinate(t *testing.T) {
	coordinate := scratchCoordinate(t)
	first := acquirePrepared(t, coordinate)
	defer first.AbortBeforeStoreOpen()
	if _, err := devscratch.Acquire(coordinate); err == nil || !strings.Contains(err.Error(), "owns this canonical project") {
		t.Fatalf("second acquire error = %v, want live canonical owner refusal", err)
	}
}

func TestDevScratchEpochRejectsSecondExplicitContextName(t *testing.T) {
	coordinate := scratchCoordinate(t)
	first, err := devscratch.Acquire(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	defer first.AbortBeforeStoreOpen()
	// Context names are intentionally absent from the owner API.
	if _, err := devscratch.Acquire(coordinate); err == nil {
		t.Fatal("second explicit context obtained a distinct scratch epoch")
	}
}

func TestDevScratchEpochRejectsDifferentSwarmDirOwner(t *testing.T) {
	coordinate := scratchCoordinate(t)
	first, err := devscratch.Acquire(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	defer first.AbortBeforeStoreOpen()
	// Swarm-directory selection is intentionally absent from the owner API.
	if _, err := devscratch.Acquire(coordinate); err == nil {
		t.Fatal("alternate Swarm directory obtained a distinct scratch epoch")
	}
}

func TestDevScratchEpochReclaimsCrashBeforeStoreOpen(t *testing.T) {
	coordinate := scratchCoordinate(t)
	helper := startEpochHelper(t, coordinate.ProjectRoot, "before-open")
	requireContended(t, coordinate)
	helper.kill(t)
	authority := acquirePrepared(t, coordinate)
	if err := authority.AbortBeforeStoreOpen(); err != nil {
		t.Fatal(err)
	}
}

func TestDevScratchEpochReclaimsCrashAfterStoreOpen(t *testing.T) {
	coordinate := scratchCoordinate(t)
	helper := startEpochHelper(t, coordinate.ProjectRoot, "after-open")
	requireContended(t, coordinate)
	helper.kill(t)
	authority := acquirePrepared(t, coordinate)
	if _, err := os.Stat(coordinate.DatabasePath); !os.IsNotExist(err) {
		t.Fatalf("predecessor database stat error = %v, want complete replacement", err)
	}
	if err := authority.AbortBeforeStoreOpen(); err != nil {
		t.Fatal(err)
	}
}

func TestDevScratchEpochDistinguishesLiveAndReleasedOwner(t *testing.T) {
	coordinate := scratchCoordinate(t)
	helper := startEpochHelper(t, coordinate.ProjectRoot, "before-open")
	requireContended(t, coordinate)
	helper.kill(t)
	authority, err := devscratch.Acquire(coordinate)
	if err != nil {
		t.Fatalf("acquire after process death: %v", err)
	}
	if err := authority.AbortBeforeStoreOpen(); err != nil {
		t.Fatal(err)
	}
}

func TestDevScratchEpochUsesDedicatedProjectLocalCoordinate(t *testing.T) {
	root := t.TempDir()
	coordinate, err := devscratch.Resolve(root)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, ".swarm", "stores", "dev-scratch.db")
	if coordinate.DatabasePath != want {
		t.Fatalf("database path = %q, want %q", coordinate.DatabasePath, want)
	}
	durable := filepath.Join(root, ".swarm", "stores", "dev.db")
	if err := os.MkdirAll(filepath.Dir(durable), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(durable, []byte("durable"), 0o600); err != nil {
		t.Fatal(err)
	}
	authority := acquirePrepared(t, coordinate)
	defer authority.AbortBeforeStoreOpen()
	got, err := os.ReadFile(durable)
	if err != nil || string(got) != "durable" {
		t.Fatalf("normal durable store changed: body=%q err=%v", got, err)
	}
}

func TestDevScratchEpochCanonicalPathAliasesConverge(t *testing.T) {
	parent := t.TempDir()
	realRoot := filepath.Join(parent, "real")
	if err := os.Mkdir(realRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(realRoot, alias); err != nil {
		t.Fatal(err)
	}
	realCoordinate, err := devscratch.Resolve(realRoot)
	if err != nil {
		t.Fatal(err)
	}
	aliasCoordinate, err := devscratch.Resolve(alias)
	if err != nil {
		t.Fatal(err)
	}
	if realCoordinate != aliasCoordinate {
		t.Fatalf("alias coordinate = %#v, want %#v", aliasCoordinate, realCoordinate)
	}
	authority, err := devscratch.Acquire(aliasCoordinate)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.AbortBeforeStoreOpen()
	if _, err := devscratch.Acquire(realCoordinate); err == nil {
		t.Fatal("canonical alias obtained a second epoch")
	}
}

func TestDevScratchEpochReplacementIsAllOrFailClosed(t *testing.T) {
	coordinate := scratchCoordinate(t)
	if err := os.MkdirAll(filepath.Dir(coordinate.DatabasePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(coordinate.DatabasePath, []byte("predecessor"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(coordinate.DatabasePath), coordinate.DatabasePath+"-wal"); err != nil {
		t.Fatal(err)
	}
	authority, err := devscratch.Acquire(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.AbortBeforeStoreOpen()
	if err := authority.PrepareFreshStore(); err == nil || !strings.Contains(err.Error(), "not one unaliased regular file") {
		t.Fatalf("PrepareFreshStore error = %v, want aliased sidecar refusal", err)
	}
	got, err := os.ReadFile(coordinate.DatabasePath)
	if err != nil || string(got) != "predecessor" {
		t.Fatalf("database was partially replaced: body=%q err=%v", got, err)
	}
}

func TestDevScratchEpochHandsOffToSQLitePossession(t *testing.T) {
	coordinate := scratchCoordinate(t)
	authority := acquirePrepared(t, coordinate)
	owner, err := storeselected.OpenRuntime(context.Background(), storeselected.RuntimeRequest{
		Selection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: coordinate.DatabasePath},
	})
	if err != nil {
		_ = authority.AbortBeforeStoreOpen()
		t.Fatal(err)
	}
	lifecycle, err := authority.BindOpenedStore(owner)
	if err != nil {
		_ = owner.CloseUnactivated()
		t.Fatal(err)
	}
	requireContended(t, coordinate)
	if err := lifecycle.CloseUnactivated(); err != nil {
		t.Fatal(err)
	}
	successor, err := devscratch.Acquire(coordinate)
	if err != nil {
		t.Fatalf("successor acquire after ordered close: %v", err)
	}
	_ = successor.AbortBeforeStoreOpen()
}

func TestDevScratchEpochReleaseRequiresClosedStore(t *testing.T) {
	coordinate := scratchCoordinate(t)
	authority := acquirePrepared(t, coordinate)
	store := &closeGateStore{failClose: true}
	lifecycle, err := authority.BindOpenedStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.CloseUnactivated(); err == nil || !strings.Contains(err.Error(), "injected close failure") {
		t.Fatalf("first close error = %v, want selected-store close failure", err)
	}
	requireContended(t, coordinate)
	store.failClose = false
	if err := lifecycle.CloseUnactivated(); err != nil {
		t.Fatal(err)
	}
	successor, err := devscratch.Acquire(coordinate)
	if err != nil {
		t.Fatalf("successor acquire after selected-store close: %v", err)
	}
	_ = successor.AbortBeforeStoreOpen()
}

type closeGateStore struct {
	failClose bool
}

func (s *closeGateStore) Activate(*worklifetime.Process) error { return nil }

func (s *closeGateStore) CloseUnactivated() error {
	if s.failClose {
		return errors.New("injected close failure")
	}
	return nil
}

func (s *closeGateStore) CloseActivated(*worklifetime.ProcessJoinReceipt) error {
	return s.CloseUnactivated()
}

func scratchCoordinate(t *testing.T) devscratch.Coordinate {
	t.Helper()
	coordinate, err := devscratch.Resolve(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return coordinate
}

func acquirePrepared(t *testing.T, coordinate devscratch.Coordinate) *devscratch.EpochAuthority {
	t.Helper()
	authority, err := devscratch.Acquire(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.PrepareFreshStore(); err != nil {
		_ = authority.AbortBeforeStoreOpen()
		t.Fatal(err)
	}
	return authority
}

func requireContended(t *testing.T, coordinate devscratch.Coordinate) {
	t.Helper()
	authority, err := devscratch.Acquire(coordinate)
	if err == nil {
		_ = authority.AbortBeforeStoreOpen()
		t.Fatal("acquired a live dev scratch epoch")
	}
}

type epochHelper struct {
	cmd *exec.Cmd
}

func startEpochHelper(t *testing.T, root, mode string) *epochHelper {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDevScratchEpochHelperProcess$", "-test.v")
	cmd.Env = append(os.Environ(), helperModeEnv+"="+mode, helperRootEnv+"="+root)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	helper := &epochHelper{cmd: cmd}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "READY" {
			return helper
		}
	}
	t.Fatalf("dev scratch helper exited before readiness: scan=%v stderr=%s", scanner.Err(), stderr.String())
	return nil
}

func (h *epochHelper) kill(t *testing.T) {
	t.Helper()
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		t.Fatal("dev scratch helper is not running")
	}
	if err := h.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := h.cmd.Wait(); err == nil {
		t.Fatal("killed helper exited successfully")
	}
	h.cmd.Process = nil
}
