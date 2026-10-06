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
		}
	}
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
