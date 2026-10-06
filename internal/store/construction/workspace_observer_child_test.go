package construction_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/storetest"
)

type workspaceObserverResult struct {
	Observed        bool `json:"observed"`
	Finished        bool `json:"finished"`
	AgentDeliveries int  `json:"agent_deliveries"`
	Delivered       int  `json:"delivered"`
	Emitted         int  `json:"emitted"`
}

// Only the compiled release harness enters this process; ordinary owner tests
// exercise the observer below without claiming a public Docker journey.
func TestWorkspaceInvocationReadOnlyObserverChild(t *testing.T) {
	if os.Getenv("SWARM_TEST_WORKSPACE_OBSERVER_CHILD") != "1" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	result, err := runWorkspaceObserverChild(ctx, os.Getenv("SWARM_TEST_WORKSPACE_OBSERVER_ROOT"), json.NewEncoder(os.Stdout))
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	result.Finished = true
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func runWorkspaceObserverChild(ctx context.Context, root string, output *json.Encoder) (workspaceObserverResult, error) {
	if !filepath.IsAbs(root) {
		return workspaceObserverResult{}, errors.New("workspace observer requires an absolute private session root")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return workspaceObserverResult{}, errors.New("workspace observer requires an existing private session directory")
	}
	if err := output.Encode(struct {
		Ready bool `json:"ready"`
	}{true}); err != nil {
		return workspaceObserverResult{}, err
	}
	return observeWorkspaceInvocation(ctx, root, output)
}

func observeWorkspaceInvocation(ctx context.Context, root string, output *json.Encoder) (result workspaceObserverResult, retErr error) {
	var inspection storetest.ReleaseProcessReadOnlyInspection
	defer func() {
		if inspection != nil {
			retErr = errors.Join(retErr, inspection.Close())
			if retErr != nil {
				result = workspaceObserverResult{}
			}
		}
	}()
	var session string
	var lastError error
	var lastDiagnostic time.Time
	var diagnosticInspection storetest.ReleaseProcessReadOnlyInspection
	var diagnosticDisabled bool
	var diagnosticSamples int
	var diagnosticTotal, diagnosticMax time.Duration
	defer func() {
		if diagnosticInspection != nil {
			if err := diagnosticInspection.Close(); err != nil {
				fmt.Fprintln(os.Stderr, "optional workspace diagnostic close:", err)
			}
		}
	}()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			if !result.Observed {
				return workspaceObserverResult{}, errors.Join(errors.New("workspace observer did not obtain a private store snapshot"), lastError)
			}
			return result, nil
		case <-tick.C:
			paths, err := filepath.Glob(filepath.Join(root, "swarm-test-session-*", "state.db"))
			if err != nil || len(paths) > 1 {
				return workspaceObserverResult{}, errors.New("workspace observer requires exactly one private session store")
			}
			if len(paths) == 0 {
				continue
			}
			if session != "" && session != paths[0] {
				return workspaceObserverResult{}, errors.New("workspace observer encountered a different private session")
			}
			session = paths[0]
			// Stop is graceful: join an in-flight bounded read before publishing
			// its result rather than cancelling it into a fabricated zero snapshot.
			readCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if inspection == nil {
				inspection, err = storetest.OpenReleaseProcessReadOnlyInspection("sqlite", session)
			}
			var next storetest.WorkspaceMockInvocationStorage
			if err == nil {
				next, err = readWorkspaceInvocationSnapshot(readCtx, inspection)
			}
			cancel()
			if err != nil {
				if result.Observed {
					return workspaceObserverResult{}, err
				}
				lastError = err
				continue
			}
			first := !result.Observed
			result.Observed = true
			result.AgentDeliveries = max(result.AgentDeliveries, next.AgentDeliveries)
			result.Delivered = max(result.Delivered, next.Delivered)
			result.Emitted = max(result.Emitted, next.Emitted)
			if first {
				if err := output.Encode(result); err != nil {
					return workspaceObserverResult{}, err
				}
			}
			if path := os.Getenv("SWARM_TEST_WORKSPACE_OBSERVER_DIAGNOSTICS"); path != "" && !diagnosticDisabled && time.Since(lastDiagnostic) >= time.Second {
				started := time.Now()
				diagnosticCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				if diagnosticInspection == nil {
					diagnosticInspection, err = storetest.OpenReleaseProcessReadOnlyInspection("sqlite", session)
				}
				if err == nil {
					err = writeWorkspaceObserverSnapshot(diagnosticCtx, diagnosticInspection, path)
				}
				cancel()
				elapsed := time.Since(started)
				diagnosticSamples++
				diagnosticTotal += elapsed
				diagnosticMax = max(diagnosticMax, elapsed)
				if err != nil {
					fmt.Fprintln(os.Stderr, "optional workspace diagnostics unavailable:", err)
					diagnosticDisabled = true
				}
				metrics, marshalErr := json.Marshal(struct {
					Samples      int   `json:"samples"`
					TotalNanos   int64 `json:"total_nanos"`
					MaximumNanos int64 `json:"maximum_nanos"`
					Unavailable  bool  `json:"unavailable"`
				}{diagnosticSamples, int64(diagnosticTotal), int64(diagnosticMax), diagnosticDisabled})
				if err := errors.Join(marshalErr, os.WriteFile(path+".timing.json", metrics, 0o600)); err != nil {
					fmt.Fprintln(os.Stderr, "optional workspace diagnostic timing unavailable:", err)
					diagnosticDisabled = true
				}
				lastDiagnostic = time.Now()
			}
		}
	}
}

// Preserve exact native rows before the private command deletes its store. Only
// the latest bounded snapshot is kept, and the parent prints it only on failure.
func writeWorkspaceObserverSnapshot(ctx context.Context, inspection storetest.ReleaseProcessReadOnlyInspection, path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("workspace observer diagnostics require an absolute path")
	}
	var snapshot storetest.WorkspaceInvocationPhases
	err := inspection.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		snapshot, err = storetest.ReadWorkspaceInvocationPhases(scoped, inspection)
		return err
	})
	if err != nil {
		return err
	}
	data, err := json.Marshal(struct {
		At     time.Time                           `json:"at"`
		Phases storetest.WorkspaceInvocationPhases `json:"phases"`
	}{time.Now().UTC(), snapshot})
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return errors.New("workspace observer diagnostic snapshot exceeds 1 MiB")
	}
	if err := os.WriteFile(path+".partial", data, 0o600); err != nil {
		return err
	}
	return os.Rename(path+".partial", path)
}

func readWorkspaceInvocationSnapshot(ctx context.Context, inspection storetest.ReleaseProcessReadOnlyInspection) (storetest.WorkspaceMockInvocationStorage, error) {
	var result storetest.WorkspaceMockInvocationStorage
	err := inspection.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		result, err = storetest.ReadWorkspaceMockInvocationStorage(scoped, inspection)
		return err
	})
	if err != nil {
		return storetest.WorkspaceMockInvocationStorage{}, err
	}
	return result, nil
}
