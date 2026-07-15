package tools

import (
	"bytes"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type managedEffectRoundTripper struct {
	t       *testing.T
	harness *effecttest.Harness
	adapter string
	calls   *int
}

func (r managedEffectRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.t.Helper()
	if r.calls != nil {
		(*r.calls)++
	}
	if err := r.harness.RequireState(r.adapter, runtimeeffects.StateLaunched); err != nil {
		r.t.Fatal(err)
	}
	return nil, errors.New("injected transport failure")
}

func TestManagedToolEffectOutcomes(t *testing.T) {
	t.Run("authored_http_tool", func(t *testing.T) {
		harness := effecttest.New()
		executor := &Executor{httpClient: &http.Client{Transport: managedEffectRoundTripper{t: t, harness: harness, adapter: "authored_http_tool"}}}
		_, err := executor.execHTTPRequestOnce(harness.CompletionContext("authored-http"), http.MethodPost, "http://effect.test/tool", nil, bytes.NewReader([]byte(`{"x":1}`)), time.Second, RegisteredTool{Name: "effect-test"}, nil)
		if err == nil {
			t.Fatal("authored HTTP transport failure returned nil")
		}
		if err := harness.RequireState("authored_http_tool", runtimeeffects.StateOutcomeUncertain); err != nil {
			t.Fatal(err)
		}
		stale := effecttest.New()
		stale.AuthorizeErr = errors.New("superseded generation")
		staleExecutor := &Executor{httpClient: &http.Client{Transport: managedEffectRoundTripper{t: t, harness: stale, adapter: "authored_http_tool"}}}
		if _, err := staleExecutor.execHTTPRequestOnce(stale.CompletionContext("authored-http-stale"), http.MethodPost, "http://effect.test/tool", nil, bytes.NewReader([]byte(`{"x":1}`)), time.Second, RegisteredTool{Name: "effect-test"}, nil); err == nil {
			t.Fatal("stale authored HTTP effect was admitted")
		}
		if _, launched := stale.StateForAdapter("authored_http_tool"); launched {
			t.Fatal("stale authored HTTP effect reached dispatch")
		}

		supersededAtLaunch := effecttest.New()
		supersededAtLaunch.MarkErr = runtimefailures.New(runtimefailures.ClassSupersededGeneration, "superseded_generation", "external-effects", "launch_attempt", nil)
		dispatches := 0
		launchFencedExecutor := &Executor{httpClient: &http.Client{Transport: managedEffectRoundTripper{
			t: t, harness: supersededAtLaunch, adapter: "authored_http_tool", calls: &dispatches,
		}}}
		if _, err := launchFencedExecutor.execHTTPRequestOnce(supersededAtLaunch.CompletionContext("authored-http-launch-fence"), http.MethodPost, "http://effect.test/tool", nil, bytes.NewReader([]byte(`{"x":1}`)), time.Second, RegisteredTool{Name: "effect-test"}, nil); err == nil {
			t.Fatal("superseded launch boundary admitted authored HTTP dispatch")
		}
		if dispatches != 0 {
			t.Fatalf("superseded launch boundary dispatched HTTP %d times", dispatches)
		}
		if err := supersededAtLaunch.RequireState("authored_http_tool", runtimeeffects.StateAuthorized); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("native_web_search", func(t *testing.T) {
		harness := effecttest.New()
		executor := &Executor{httpClient: &http.Client{Transport: managedEffectRoundTripper{t: t, harness: harness, adapter: "native_web_search"}}}
		req, err := http.NewRequest(http.MethodPost, "http://effect.test/search", bytes.NewReader([]byte(`{"query":"x"}`)))
		if err != nil {
			t.Fatal(err)
		}
		_, err = executor.doNormalizedSearch(harness.CompletionContext("native-search"), req, "results", map[string]string{"title": "title", "url": "url", "snippet": "snippet"}, externalDispatchAdmissionPolicy{})
		if err == nil {
			t.Fatal("native search transport failure returned nil")
		}
		if err := harness.RequireState("native_web_search", runtimeeffects.StateOutcomeUncertain); err != nil {
			t.Fatal(err)
		}
		stale := effecttest.New()
		stale.AuthorizeErr = errors.New("superseded generation")
		staleExecutor := &Executor{httpClient: &http.Client{Transport: managedEffectRoundTripper{t: t, harness: stale, adapter: "native_web_search"}}}
		staleReq, _ := http.NewRequest(http.MethodPost, "http://effect.test/search", bytes.NewReader([]byte(`{"query":"x"}`)))
		if _, err := staleExecutor.doNormalizedSearch(stale.CompletionContext("native-search-stale"), staleReq, "results", map[string]string{"title": "title", "url": "url", "snippet": "snippet"}, externalDispatchAdmissionPolicy{}); err == nil {
			t.Fatal("stale native search was admitted")
		}
		if _, launched := stale.StateForAdapter("native_web_search"); launched {
			t.Fatal("stale native search reached dispatch")
		}
	})
}

func TestManagedNativeEffectOutcomes(t *testing.T) {
	hostTarget := func(root string) *workspace.Target {
		return &workspace.Target{
			Backend: workspace.BackendHost, Workdir: root,
			Mounts: []workspace.ExecutionMount{{LogicalPath: workspace.LogicalWorkspaceMount, HostPath: root, Access: workspace.MountAccessReadWrite}},
		}
	}

	t.Run("bash_start_rejection", func(t *testing.T) {
		harness := effecttest.New()
		executor := &Executor{}
		_, _, _, err := executor.runWorkspaceCommand(harness.CompletionContext("native-bash"), hostTarget(t.TempDir()), "native_bash", time.Second, "", "/definitely/missing/swarm-command")
		if err == nil {
			t.Fatal("missing native command returned nil")
		}
		if err := harness.RequireState("native_bash", runtimeeffects.StateTerminalFailure); err != nil {
			t.Fatal(err)
		}
		stale := effecttest.New()
		stale.AuthorizeErr = errors.New("superseded generation")
		marker := filepath.Join(t.TempDir(), "started")
		if _, _, _, err := executor.runWorkspaceCommand(stale.CompletionContext("native-bash-stale"), hostTarget(t.TempDir()), "native_bash", time.Second, "", "sh", "-lc", "touch "+marker); err == nil {
			t.Fatal("stale native command was admitted")
		}
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("stale native command reached start: %v", err)
		}
	})

	t.Run("read_start_rejection", func(t *testing.T) {
		harness := effecttest.New()
		executor := &Executor{}
		_, _, _, err := executor.runWorkspaceCommand(harness.CompletionContext("native-read"), hostTarget(t.TempDir()), "native_read_file", time.Second, "", "/definitely/missing/swarm-command")
		if err == nil {
			t.Fatal("missing native read command returned nil")
		}
		if err := harness.RequireState("native_read_file", runtimeeffects.StateTerminalFailure); err != nil {
			t.Fatal(err)
		}
		stale := effecttest.New()
		stale.AuthorizeErr = errors.New("superseded generation")
		if _, _, _, err := executor.runWorkspaceCommand(stale.CompletionContext("native-read-stale"), hostTarget(t.TempDir()), "native_read_file", time.Second, "", "sh", "-lc", "cat /dev/null"); err == nil {
			t.Fatal("stale native read was admitted")
		}
		if _, launched := stale.StateForAdapter("native_read_file"); launched {
			t.Fatal("stale native read reached process start")
		}
	})

	t.Run("host_write", func(t *testing.T) {
		harness := effecttest.New()
		root := t.TempDir()
		target := hostTarget(root).ExecutionTarget()
		if _, err := execNativeHostWriteFile(harness.CompletionContext("native-write"), target, "/workspace/result.txt", "written"); err != nil {
			t.Fatalf("host write: %v", err)
		}
		if err := harness.RequireState("native_write_file", runtimeeffects.StateSettled); err != nil {
			t.Fatal(err)
		}
		if raw, err := os.ReadFile(filepath.Join(root, "result.txt")); err != nil || string(raw) != "written" {
			t.Fatalf("written file = %q err=%v", raw, err)
		}
		stale := effecttest.New()
		stale.AuthorizeErr = errors.New("superseded generation")
		if _, err := execNativeHostWriteFile(stale.CompletionContext("native-write-stale"), target, "/workspace/stale.txt", "forbidden"); err == nil {
			t.Fatal("stale native write was admitted")
		}
		if _, err := os.Stat(filepath.Join(root, "stale.txt")); !os.IsNotExist(err) {
			t.Fatalf("stale native write mutated filesystem: %v", err)
		}
	})
}

func TestManagedRelayEffectOutcomes(t *testing.T) {
	harness := effecttest.New()
	root := t.TempDir()
	target := &workspace.Target{
		Backend: workspace.BackendHost, Workdir: root,
		Mounts: []workspace.ExecutionMount{{LogicalPath: workspace.LogicalWorkspaceMount, HostPath: root, Access: workspace.MountAccessReadWrite}},
	}
	executor := &Executor{}
	if err := executor.writeToolResultRelayFile(harness.CompletionContext("tool-relay"), target, target.ExecutionTarget(), "/workspace/relay.txt", []byte("relay")); err != nil {
		t.Fatalf("write relay: %v", err)
	}
	if err := harness.RequireState("tool_result_relay", runtimeeffects.StateSettled); err != nil {
		t.Fatal(err)
	}
	stale := effecttest.New()
	stale.AuthorizeErr = errors.New("superseded generation")
	if err := executor.writeToolResultRelayFile(stale.CompletionContext("tool-relay-stale"), target, target.ExecutionTarget(), "/workspace/stale-relay.txt", []byte("relay")); err == nil {
		t.Fatal("stale tool relay was admitted")
	}
	if _, err := os.Stat(filepath.Join(root, "stale-relay.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale relay mutated filesystem: %v", err)
	}
}
