package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

const completion2270StartupPrompt = "Startup validation probe. Do not call any tools. Reply with the exact text ok."

func TestClaudeCLICompletion2270ProtocolFixture(t *testing.T) {
	for _, tc := range []struct {
		name, prompt, model, refusal string
		remove                       string
		valid, business              bool
	}{
		{name: "exact_startup", prompt: completion2270StartupPrompt, valid: true},
		{name: "business_missing_model", prompt: "business request"},
		{name: "business_wrong_model", prompt: "business request", model: "wrong-model"},
		{name: "changed_startup_prompt", prompt: completion2270StartupPrompt + "\n"},
		{name: "changed_startup_shape", prompt: completion2270StartupPrompt, remove: "--verbose"},
		{name: "valid_business", prompt: "business request", model: "request-A", valid: true, business: true},
		{name: "startup_auth_refusal", prompt: completion2270StartupPrompt, refusal: "auth"},
		{name: "startup_discovery_refusal", prompt: completion2270StartupPrompt, refusal: "discovery"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, args, attemptID := completion2270ProtocolFixture(t, tc.refusal)
			if tc.remove != "" {
				var filtered []string
				for _, arg := range args {
					if arg != tc.remove {
						filtered = append(filtered, arg)
					}
				}
				args = filtered
			}
			if tc.model != "" {
				args = append(args, "--model", tc.model)
			}
			stdout, stderr, err := runCompletion2270Protocol(t, root, args, tc.prompt)
			if (err == nil) != tc.valid {
				t.Fatalf("protocol result=%v, expected success=%v, stderr=%s", err, tc.valid, stderr)
			}
			if !tc.valid {
				if stdout != "" {
					t.Fatalf("refused invocation produced a capability/result: %s", stdout)
				}
				return
			}
			var frames []map[string]any
			decoder := json.NewDecoder(strings.NewReader(stdout))
			for {
				var frame map[string]any
				if err := decoder.Decode(&frame); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				frames = append(frames, frame)
			}
			if len(frames) == 0 || frames[0]["type"] != "system" || frames[0]["subtype"] != "init" || frames[0]["model"] != "initialization-not-execution" || frames[0]["session_id"] != attemptID {
				t.Fatalf("native init observation=%+v", frames)
			}
			tools := frames[0]["tools"].([]any)
			if len(tools) != 2 || tools[0] != "Read" || tools[1] != "mcp__swarm__call" {
				t.Fatalf("capabilities did not consume authenticated discovery: %+v", tools)
			}
			if tc.business {
				if len(frames) != 2 || frames[1]["type"] != "result" || frames[1]["model"] != "reported-B" {
					t.Fatalf("business evidence=%+v", frames)
				}
				rows := readCompletion2270ProtocolInvocations(t, root)
				if err := validateCompletion2270SingleInvocation(rows, attemptID); err != nil {
					t.Fatal(err)
				}
			} else {
				if len(frames) != 1 {
					t.Fatalf("startup invented a completion or cost: %+v", frames)
				}
				if _, err := os.Stat(filepath.Join(root, completion2270InvocationFile)); !os.IsNotExist(err) {
					t.Fatalf("startup created business invocation evidence: %v", err)
				}
				if _, err := os.Stat(filepath.Join(root, "claude", "projects")); !os.IsNotExist(err) {
					t.Fatalf("startup created a confirmed business transcript: %v", err)
				}
			}
		})
	}
}

func TestClaudeCLICompletion2270InvocationOracleRejectsRedispatch(t *testing.T) {
	root, args, attemptID := completion2270ProtocolFixture(t, "")
	args = append(args, "--model", "request-A")
	for i := 0; i < 2; i++ {
		_, stderr, err := runCompletion2270Protocol(t, root, args, "business request")
		if err != nil {
			t.Fatalf("business invocation %d failed: %v %s", i, err, stderr)
		}
		rows := readCompletion2270ProtocolInvocations(t, root)
		if err := validateCompletion2270SingleInvocation(rows, attemptID); (err == nil) != (i == 0) {
			t.Fatalf("oracle accepted redispatch or refused the original invocation: rows=%+v err=%v", rows, err)
		}
		if i == 0 && validateCompletion2270SingleInvocation(rows, uuid.NewString()) == nil {
			t.Fatal("oracle accepted a foreign admitted attempt")
		}
		if err := os.RemoveAll(filepath.Join(root, "private-invocation")); err != nil {
			t.Fatal(err)
		}
		if preserved := readCompletion2270ProtocolInvocations(t, root); !reflect.DeepEqual(preserved, rows) {
			t.Fatal("disposable invocation cleanup erased physical business evidence")
		}
	}
}

func TestClaudeCLICompletion2270InvocationReaderUsesPublishedProjectionWorkspaces(t *testing.T) {
	ctx := t.Context()
	attemptID := uuid.NewString()
	contexts, err := runtimepkg.NewRuntimeContextManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	var proof completion2270InvocationProof
	proof.observe(contexts)
	var reads int
	var selectedWorkspace *workspace.DockerManager
	var selectedOwner *worklifetime.RuntimeOccurrence
	for _, name := range []string{"original", "selected"} {
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: "+name+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		artifact, err := sourceartifact.AdmitDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		projection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = projection.Release() })
		template := workspace.NewDockerManager()
		var bound *workspace.DockerManager
		template.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
			containers := bound.SystemWorkspaceContainers()
			if len(args) != 7 || args[0] != "exec" || (args[1] != containers[0] && args[1] != containers[1]) || args[6] != completion2270InvocationFile {
				return "", fmt.Errorf("invocation reader used template or foreign workspace: %v", args)
			}
			reads++
			if args[1] != containers[0] {
				return "", nil
			}
			row, err := json.Marshal(completion2270Invocation{AttemptID: attemptID, InvocationID: name, Model: "request-A"})
			return string(row), err
		})
		lifecycle, err := template.RebindSourceProjection(projection, semanticview.Wrap(&contracts.WorkflowContractBundle{}))
		if err != nil {
			t.Fatal(err)
		}
		bound = lifecycle.(*workspace.DockerManager)
		selectedWorkspace = bound
		t.Cleanup(func() { _ = bound.ReleaseSourceProjection(context.Background()) })
		if reflect.DeepEqual(template.SystemWorkspaceContainers(), bound.SystemWorkspaceContainers()) {
			t.Fatal("fixture did not distinguish template from projection-bound runtime")
		}
		fact := mustServeTestEphemeralSourceArtifactFact(artifact.BundleHash())
		owner := newSupervisorTestRuntimeOccurrence(t, artifact.BundleHash())
		selectedOwner = owner
		source := semanticviewtest.WrapRootAgents(&contracts.WorkflowContractBundle{})
		eventBus, err := bus.NewEphemeralEventBusWithOptions(nil, bus.EventBusOptions{
			SourceArtifactFact: fact, WorkOwner: owner, ContractBundle: source, ReceiverExecution: eventreceiver.NormalExecution(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := contexts.Register(runtimepkg.BundleContext{
			SourceArtifactFact: fact, Source: source, WorkOwner: owner,
			Runtime: &runtimepkg.Runtime{Bus: eventBus, Workspace: &completion2270ObservedWorkspace{DockerManager: bound, proof: &proof}},
		}); err != nil {
			t.Fatal(err)
		}
		if contexts.LoadedContexts()[0].Runtime != nil {
			t.Fatal("fixture omitted the production readback/runtime-lease distinction")
		}
		rows, err := proof.read(ctx)
		if owner.ActiveCount() != 0 {
			t.Fatal("invocation read retained an executable context lease")
		}
		if name == "original" && (err != nil || reads != 2 || validateCompletion2270SingleInvocation(rows, attemptID) != nil) {
			t.Fatalf("original published workspace was not read: rows=%+v reads=%d err=%v", rows, reads, err)
		}
		if name == "selected" && (err != nil || reads != 6 || len(rows) != 2 || validateCompletion2270SingleInvocation(rows, attemptID) == nil) {
			t.Fatalf("reader omitted a selected runtime redispatch: rows=%+v reads=%d err=%v", rows, reads, err)
		}
	}
	seen := map[string]bool{}
	if _, err := readCompletion2270WorkspaceInvocations(ctx, selectedWorkspace, seen); err != nil {
		t.Fatal(err)
	}
	before := reads
	if rows, err := readCompletion2270WorkspaceInvocations(ctx, selectedWorkspace, seen); err != nil || len(rows) != 0 || reads != before {
		t.Fatalf("duplicate-container read was not deduplicated: rows=%+v reads=%d/%d err=%v", rows, reads, before, err)
	}
	selectedWorkspace.SetRunDockerFnForTest(func(context.Context, ...string) (string, error) {
		return "", fmt.Errorf("expected owned container disappeared")
	})
	if _, err := proof.read(ctx); err == nil || !strings.Contains(err.Error(), "expected owned container disappeared") || selectedOwner.ActiveCount() != 0 {
		t.Fatalf("missing owned container became zero invocations or retained its lease: %v", err)
	}
}

func TestClaudeCLICompletion2270InvocationReaderRefusesMissingOwners(t *testing.T) {
	var proof completion2270InvocationProof
	if _, err := proof.read(t.Context()); err == nil {
		t.Fatal("reader accepted an unpublished owner")
	}
	contexts, err := runtimepkg.NewRuntimeContextManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	proof.observe(contexts)
	if _, err := proof.read(t.Context()); err == nil {
		t.Fatal("reader accepted an empty loaded context set")
	}
	if _, err := readCompletion2270WorkspaceInvocations(t.Context(), workspace.NewHostManager(), map[string]bool{}); err == nil {
		t.Fatal("reader accepted a non-Docker owner")
	}
}

func completion2270ProtocolFixture(t *testing.T, refusal string) (string, []string, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "completion-proof-cli"), []byte(completion2270CLIProgram), 0o755); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refusal == "auth" || r.Header.Get("Authorization") != "Bearer protocol-proof" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var request struct {
			Method string `json:"method"`
			ID     int    `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Method == "notifications/initialized" {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		response := map[string]any{"jsonrpc": "2.0", "id": request.ID}
		if request.Method == "initialize" {
			response["result"] = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{"tools": map[string]any{}}}
		} else if request.Method == "tools/list" && refusal != "discovery" {
			response["result"] = map[string]any{"tools": []map[string]any{{"name": "call"}}}
		} else {
			response["error"] = map[string]any{"code": -32603, "message": "refused discovery"}
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	configuration, err := json.Marshal(map[string]any{"mcpServers": map[string]any{
		"swarm": map[string]any{"url": server.URL, "headers": map[string]string{"Authorization": "Bearer protocol-proof"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	attemptID := uuid.NewString()
	args := []string{"-p", "--session-id", attemptID, "--output-format", "stream-json", "--verbose", "--include-partial-messages",
		"--tools", "Read", "--allowedTools", "Read,mcp__swarm__call", "--mcp-config", string(configuration), "--strict-mcp-config"}
	return root, args, attemptID
}

func runCompletion2270Protocol(t *testing.T, root string, args []string, prompt string) (string, string, error) {
	t.Helper()
	return runCompletion2270ProtocolContext(t, t.Context(), root, args, prompt)
}

func runCompletion2270ProtocolContext(t *testing.T, parent context.Context, root string, args []string, prompt string) (string, string, error) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatalf("first-party CLI protocol regression requires Node: %v", err)
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, append([]string{filepath.Join(root, "completion-proof-cli")}, args...)...)
	command.Dir = root
	scratch := filepath.Join(root, "private-invocation")
	if err := os.MkdirAll(scratch, 0o700); err != nil {
		t.Fatal(err)
	}
	command.Env = append(os.Environ(), "TMPDIR="+scratch, "CLAUDE_CONFIG_DIR="+filepath.Join(root, "claude"))
	command.Stdin = strings.NewReader(prompt)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	if ctx.Err() != nil || parent.Err() != nil {
		t.Fatalf("protocol fixture context terminated: command=%v context=%v parent=%v stdout=%q stderr=%q",
			err, ctx.Err(), parent.Err(), stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String(), err
}

func readCompletion2270ProtocolInvocations(t *testing.T, root string) []completion2270Invocation {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, completion2270InvocationFile))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := decodeCompletion2270Invocations(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	return rows
}
