package tools

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

type toolLaunchCommitProbe struct {
	managedEffectCommittedProbe
	fault   error
	phase   runtimeeffects.MutationPhase
	foreign bool
	expired bool
	joined  bool
	cancel  context.CancelFunc
}

func (p *toolLaunchCommitProbe) AuthorizeExternalAttempt(ctx context.Context, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	attempt, err := p.managedEffectCommittedProbe.AuthorizeExternalAttempt(ctx, authority, req)
	if err == nil && p.expired {
		attempt.Authority.LeaseExpiresAt = time.Now().Add(-time.Second)
	}
	return attempt, err
}

func (p *toolLaunchCommitProbe) MarkExternalAttemptLaunched(ctx context.Context, attempt runtimeeffects.Attempt, at time.Time) error {
	if err := p.Harness.MarkExternalAttemptLaunched(ctx, attempt, at); err != nil {
		return err
	}
	if p.cancel != nil {
		p.cancel()
	}
	if p.foreign {
		attempt.AttemptID = uuid.NewString()
	}
	err := runtimeeffects.NewPostCommitMutationError(p.phase, attempt, p.fault)
	if p.joined {
		return errors.Join(err, context.Canceled)
	}
	return err
}

func toolLaunchCommitContext(harness *effecttest.Harness, identity string, phase runtimeeffects.MutationPhase, cancelAfterCommit, foreign, expired bool, joined ...bool) context.Context {
	ctx := managedEffectCommittedContext(harness, identity)
	probe := &toolLaunchCommitProbe{
		managedEffectCommittedProbe: managedEffectCommittedProbe{Harness: harness},
		fault:                       errors.New("injected launch cleanup failure"), phase: phase, foreign: foreign, expired: expired,
	}
	if len(joined) != 0 {
		probe.joined = joined[0]
	}
	if cancelAfterCommit {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		probe.cancel = cancel
	}
	controller := runtimeeffects.NewCompletionController(probe, probe, probe, probe).WithExecutionPosture(executionposture.Live)
	return runtimeeffects.WithController(ctx, controller)
}

type toolLaunchTransport struct {
	calls *int
	body  string
}

func (tr toolLaunchTransport) RoundTrip(*http.Request) (*http.Response, error) {
	*tr.calls++
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(tr.body)),
	}, nil
}

func toolLaunchHostTarget(root string) *workspace.Target {
	return &workspace.Target{
		Backend: workspace.BackendHost, Workdir: root,
		Mounts: []workspace.ExecutionMount{{LogicalPath: workspace.LogicalWorkspaceMount, HostPath: root, Access: workspace.MountAccessReadWrite}},
	}
}

func TestManagedToolLaunchPostCommitCleanupExecutesOnePrimitive(t *testing.T) {
	for _, tc := range []struct {
		name    string
		adapter string
		run     func(*testing.T, context.Context, *int) error
	}{
		{name: "authored_http", adapter: "authored_http_tool", run: func(t *testing.T, ctx context.Context, calls *int) error {
			tool := admittedExecutionToolForTest(t, "launch-ack", runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.ToolInputSchema{}))
			executor := &Executor{httpClient: &http.Client{Transport: toolLaunchTransport{calls: calls, body: `{"ok":true}`}}}
			_, err := executor.execHTTPRequestOnce(ctx, http.MethodPost, "http://effect.test/tool", nil, bytes.NewReader([]byte(`{}`)), time.Second, tool, nil)
			return err
		}},
		{name: "native_search", adapter: "native_web_search", run: func(t *testing.T, ctx context.Context, calls *int) error {
			executor := &Executor{httpClient: &http.Client{Transport: toolLaunchTransport{calls: calls, body: `{"results":[]}`}}}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://effect.test/search", bytes.NewReader([]byte(`{}`)))
			if err != nil {
				return err
			}
			_, err = executor.doNormalizedSearch(ctx, req, "results", map[string]string{"title": "title", "url": "url", "snippet": "snippet"}, externalDispatchAdmissionPolicy{})
			return err
		}},
		{name: "native_command", adapter: "native_bash", run: func(t *testing.T, ctx context.Context, calls *int) error {
			marker := filepath.Join(t.TempDir(), "command-ran")
			executor := &Executor{}
			_, _, code, err := executor.runWorkspaceCommand(ctx, toolLaunchHostTarget(t.TempDir()), "native_bash", time.Second, "", "sh", "-lc", "printf x >> "+marker)
			if err != nil || code != 0 {
				return err
			}
			raw, err := os.ReadFile(marker)
			if err == nil && string(raw) != "x" {
				return errors.New("native command executed more than once")
			}
			*calls = len(raw)
			return err
		}},
		{name: "native_host_write", adapter: "native_write_file", run: func(t *testing.T, ctx context.Context, calls *int) error {
			root := t.TempDir()
			_, err := execNativeHostWriteFile(ctx, toolLaunchHostTarget(root).ExecutionTarget(), "/workspace/result.txt", "x")
			if err != nil {
				return err
			}
			raw, err := os.ReadFile(filepath.Join(root, "result.txt"))
			*calls = len(raw)
			return err
		}},
		{name: "tool_result_relay", adapter: "tool_result_relay", run: func(t *testing.T, ctx context.Context, calls *int) error {
			root := t.TempDir()
			target := toolLaunchHostTarget(root)
			if err := (&Executor{}).writeToolResultRelayFile(ctx, target, target.ExecutionTarget(), "/workspace/relay.txt", []byte("x")); err != nil {
				return err
			}
			raw, err := os.ReadFile(filepath.Join(root, "relay.txt"))
			*calls = len(raw)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness := effecttest.New()
			ctx := toolLaunchCommitContext(harness, tc.name, runtimeeffects.MutationLaunch, false, false, false)
			calls := 0
			if err := tc.run(t, ctx, &calls); err != nil || calls != 1 {
				t.Fatalf("primitive calls=%d err=%v, want one successful dispatch", calls, err)
			}
			if err := harness.RequireState(tc.adapter, runtimeeffects.StateSettled); err != nil {
				t.Fatal(err)
			}
			if len(harness.Settlements) != 1 {
				t.Fatalf("settlements=%d, want one", len(harness.Settlements))
			}
		})
	}
}

func TestManagedToolLaunchPostCommitErrorDoesNotBypassCancellationOrIdentity(t *testing.T) {
	for _, tc := range []struct {
		name    string
		phase   runtimeeffects.MutationPhase
		cancel  bool
		foreign bool
		expired bool
		joined  bool
	}{
		{name: "canceled", phase: runtimeeffects.MutationLaunch, cancel: true},
		{name: "wrong_phase", phase: runtimeeffects.MutationObservation},
		{name: "wrong_attempt", phase: runtimeeffects.MutationLaunch, foreign: true},
		{name: "expired_authority", phase: runtimeeffects.MutationLaunch, expired: true},
		{name: "joined_with_cancellation", phase: runtimeeffects.MutationLaunch, joined: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness := effecttest.New()
			ctx := toolLaunchCommitContext(harness, tc.name, tc.phase, tc.cancel, tc.foreign, tc.expired, tc.joined)
			calls := 0
			tool := admittedExecutionToolForTest(t, "launch-ack", runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.ToolInputSchema{}))
			executor := &Executor{httpClient: &http.Client{Transport: toolLaunchTransport{calls: &calls, body: `{}`}}}
			_, err := executor.execHTTPRequestOnce(ctx, http.MethodPost, "http://effect.test/tool", nil, bytes.NewReader([]byte(`{}`)), time.Second, tool, nil)
			if err == nil || calls != 0 {
				t.Fatalf("blocked launch calls=%d err=%v", calls, err)
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled launch error=%v", err)
			}
			if tc.cancel || tc.expired {
				attempt := onlyToolAttempt(t, harness)
				var committed *runtimeeffects.PostCommitMutationError
				if !errors.As(err, &committed) || committed.Phase != runtimeeffects.MutationLaunch ||
					committed.OperationID != attempt.OperationID || committed.AttemptID != attempt.AttemptID ||
					runtimeeffects.CommittedMutationPhase(err, runtimeeffects.MutationLaunch, attempt) {
					t.Fatalf("blocked launch must retain typed diagnostic without granting continuation: %v", err)
				}
				if err := harness.RequireState("authored_http_tool", runtimeeffects.StateTerminalFailure); err != nil {
					t.Fatal(err)
				}
				if len(harness.Settlements) != 1 {
					t.Fatalf("blocked launch settlements=%d, want one known no-dispatch", len(harness.Settlements))
				}
				for _, settlement := range harness.Settlements {
					if settlement.Failure == nil || settlement.Failure.Detail.Code != "effect_launch_dispatch_not_attempted" ||
						settlement.Evidence["dispatch_attempted"] != false || settlement.Evidence["launch_rejected"] != true {
						t.Fatalf("blocked launch settlement=%+v, want known no-dispatch", settlement)
					}
				}
				return
			}
			if tc.joined && (!errors.Is(err, context.Canceled) || runtimeeffects.CommittedMutationPhase(err, runtimeeffects.MutationLaunch, onlyToolAttempt(t, harness))) {
				t.Fatalf("joined cancellation granted continuation: %v", err)
			}
			if err := harness.RequireState("authored_http_tool", runtimeeffects.StateLaunched); err != nil {
				t.Fatal(err)
			}
			if len(harness.Settlements) != 0 {
				t.Fatalf("blocked launch settled %d attempts", len(harness.Settlements))
			}
		})
	}
}

func onlyToolAttempt(t *testing.T, harness *effecttest.Harness) runtimeeffects.Attempt {
	t.Helper()
	if len(harness.Attempts) != 1 {
		t.Fatalf("attempts=%d, want one", len(harness.Attempts))
	}
	for _, attempt := range harness.Attempts {
		return attempt
	}
	return runtimeeffects.Attempt{}
}

func TestManagedToolLaunchNoDispatchSettlementFailurePreservesErrors(t *testing.T) {
	harness := effecttest.New()
	settleErr := errors.New("injected no-dispatch settlement failure")
	harness.SettleErr = settleErr
	ctx := toolLaunchCommitContext(harness, "launch-settle-error", runtimeeffects.MutationLaunch, true, false, false)
	calls := 0
	tool := admittedExecutionToolForTest(t, "launch-ack", runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.ToolInputSchema{}))
	executor := &Executor{httpClient: &http.Client{Transport: toolLaunchTransport{calls: &calls, body: `{}`}}}
	_, err := executor.execHTTPRequestOnce(ctx, http.MethodPost, "http://effect.test/tool", nil, bytes.NewReader([]byte(`{}`)), time.Second, tool, nil)
	var committed *runtimeeffects.PostCommitMutationError
	attempt := onlyToolAttempt(t, harness)
	if calls != 0 || !errors.Is(err, context.Canceled) || !errors.Is(err, settleErr) ||
		!errors.As(err, &committed) || committed.Phase != runtimeeffects.MutationLaunch ||
		committed.OperationID != attempt.OperationID || committed.AttemptID != attempt.AttemptID ||
		runtimeeffects.CommittedMutationPhase(err, runtimeeffects.MutationLaunch, attempt) {
		t.Fatalf("blocked launch calls=%d err=%v, want cleanup, cancellation, and settlement errors", calls, err)
	}
	if err := harness.RequireState("authored_http_tool", runtimeeffects.StateLaunched); err != nil {
		t.Fatal(err)
	}
	if len(harness.Settlements) != 0 {
		t.Fatalf("failed no-dispatch settlement persisted %d attempts", len(harness.Settlements))
	}
}
