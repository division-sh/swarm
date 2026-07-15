package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCLIOutputModesForLocalConsumers(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"version", "--json"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("version --json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	versionMetadata := currentTestVersionMetadata(t)
	versionJSON := decodeOutputJSON[map[string]any](t, stdout.String())
	if versionJSON["binary_version"] != versionMetadata.BinaryVersion ||
		versionJSON["module_version"] != versionMetadata.ModuleVersion ||
		versionJSON["platform_version"] != versionMetadata.PlatformVersion ||
		versionJSON["commit"] != versionMetadata.Commit {
		t.Fatalf("version json = %#v, want local metadata", versionJSON)
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("version --json stderr = %q, want empty", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"version", "--quiet"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("version --quiet code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if got := stdout.String(); got != versionMetadata.BinaryVersion+"\n" {
		t.Fatalf("version --quiet stdout = %q, want binary version", got)
	}

	setCLIAPITestToken(t, "test-token")
	for _, mode := range []string{"--json", "--quiet"} {
		server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
			if req.Method != "health.check" {
				t.Fatalf("method = %q, want health.check", req.Method)
			}
			return validVersionHealthResult()
		})
		defer server.Close()

		stdout.Reset()
		stderr.Reset()
		code = executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"version", "--server", mode}, &stdout, &stderr, testRootCommandOptions(server))
		if code != 0 {
			t.Fatalf("version --server %s code = %d stdout=%s stderr=%s", mode, code, stdout.String(), stderr.String())
		}
		if len(*requests) != 1 {
			t.Fatalf("version --server %s requests = %d, want 1", mode, len(*requests))
		}
		if mode == "--json" {
			result := decodeOutputJSON[versionOutputResult](t, stdout.String())
			if result.Server == nil || result.Server.Bundle.Fingerprint != "sha256:server" {
				t.Fatalf("version --server json = %#v, want server identity", result)
			}
		} else if got := stdout.String(); got != versionMetadata.BinaryVersion+"\nsha256:server\n" {
			t.Fatalf("version --server quiet stdout = %q, want binary and fingerprint", got)
		}
		assertEmptyStderr(t, stderr.String())
	}

	verifyFixture := outputModeVerifyFixture(t)
	verifyConfig := writeTestVerifyRuntimeConfig(t)

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommand(context.Background(), RepoRoot(), []string{"verify", "--contracts", verifyFixture, "--config", verifyConfig, "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("verify --json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	verifyJSON := decodeOutputJSON[verifyCommandResult](t, stdout.String())
	if !verifyJSON.OK || strings.TrimSpace(verifyJSON.Contracts) == "" {
		t.Fatalf("verify json = %#v, want ok contracts", verifyJSON)
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("verify --json stderr = %q, want empty", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommand(context.Background(), RepoRoot(), []string{"verify", "--contracts", verifyFixture, "--config", verifyConfig, "--quiet"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("verify --quiet code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if got := stdout.String(); got != "ok\n" {
		t.Fatalf("verify --quiet stdout = %q, want ok", got)
	}
}

func TestCLITableRendererAlignsAndRendersEmptyValues(t *testing.T) {
	var out bytes.Buffer
	writeCLITable(&out, cliTable{
		Columns: []cliTableColumn{
			{Header: "KEY", KeyColumn: true},
			{Header: "VALUE"},
		},
		Rows: [][]string{
			{"abc", ""},
			{"longer", "ok"},
		},
	})
	want := "KEY     VALUE\nabc     -\nlonger  ok\n"
	if out.String() != want {
		t.Fatalf("table output = %q, want %q", out.String(), want)
	}
}

func TestFormatCLIHumanCodePreservesUnknownValue(t *testing.T) {
	const raw = "  future_status_code  "
	if got := formatCLIHumanCode(cliHumanCodeRunStatus, raw); got != raw {
		t.Fatalf("unknown CLI projection = %q, want exact raw value %q", got, raw)
	}
}

func TestCLITableRendererDoesNotImplicitlyTruncateIdentifierColumns(t *testing.T) {
	var out bytes.Buffer
	id := "run_0123456789abcdef0123456789abcdef"
	writeCLITable(&out, cliTable{
		Columns: []cliTableColumn{
			{Header: "RUN ID", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyRun},
			{Header: "STATUS"},
		},
		Rows: [][]string{{id, "completed"}},
	})
	if !strings.Contains(out.String(), id) {
		t.Fatalf("table output = %q, want full id %q", out.String(), id)
	}
}

func TestCLITableRendererUsesActionableEmptyState(t *testing.T) {
	var out bytes.Buffer
	writeCLITable(&out, cliTable{
		Columns:      []cliTableColumn{{Header: "ID"}},
		EmptyMessage: "No runs found. Start one: swarm run start --event <event>",
	})
	want := "No runs found. Start one: swarm run start --event <event>\n"
	if out.String() != want {
		t.Fatalf("empty table output = %q, want %q", out.String(), want)
	}
}

func TestCLITextWriterCarriesTTYGatedDisplayPolicy(t *testing.T) {
	var out bytes.Buffer
	writer := cliOutputOptions{}.textWriter(&out)
	policy := cliWriterDisplayPolicy(writer)
	if policy.Color || policy.Emoji {
		t.Fatalf("non-tty display policy = %#v, want color and emoji disabled", policy)
	}

	out.Reset()
	noColorWriter := cliOutputOptions{noColor: true}.textWriter(&out)
	if _, err := fmt.Fprint(noColorWriter, "\x1b[31mred\x1b[0m"); err != nil {
		t.Fatalf("write no-color text: %v", err)
	}
	if out.String() != "red" {
		t.Fatalf("no-color writer output = %q, want ANSI stripped", out.String())
	}
}

func TestCLIOutputModesForDiagnosticConsumers(t *testing.T) {
	setCLIAPITestToken(t, "test-token")

	t.Run("runs", func(t *testing.T) {
		for _, tc := range []struct {
			mode      string
			wantQuiet string
		}{
			{mode: "--json"},
			{mode: "--quiet", wantQuiet: "run-1\nrun-2\n"},
		} {
			server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
				if req.Method != "run.list" {
					t.Fatalf("method = %q, want run.list", req.Method)
				}
				return map[string]any{"runs": []any{validDiagnosticRunHeader("run-1"), validDiagnosticRunHeader("run-2")}}
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "list", tc.mode}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("runs %s code = %d stdout=%s stderr=%s", tc.mode, code, stdout.String(), stderr.String())
			}
			if len(*requests) != 1 {
				t.Fatalf("runs %s requests = %d, want 1", tc.mode, len(*requests))
			}
			if tc.mode == "--json" {
				result := decodeOutputJSON[diagnosticRunListResult](t, stdout.String())
				if len(result.Runs) != 2 || result.Runs[0].RunID != "run-1" {
					t.Fatalf("runs json = %#v, want run list", result)
				}
			} else if stdout.String() != tc.wantQuiet {
				t.Fatalf("runs quiet stdout = %q, want %q", stdout.String(), tc.wantQuiet)
			}
			assertEmptyStderr(t, stderr.String())
		}
	})

	t.Run("health", func(t *testing.T) {
		for _, mode := range []string{"--json", "--quiet"} {
			server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
				if req.Method != "health.check" {
					t.Fatalf("method = %q, want health.check", req.Method)
				}
				return validVersionHealthResult()
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"health", mode}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("health %s code = %d stdout=%s stderr=%s", mode, code, stdout.String(), stderr.String())
			}
			if len(*requests) != 1 {
				t.Fatalf("health %s requests = %d, want 1", mode, len(*requests))
			}
			if mode == "--json" {
				result := decodeOutputJSON[diagnosticHealthCheckResult](t, stdout.String())
				if !BoolPointerValue(result.Alive) || result.Bundle.Fingerprint != "sha256:server" {
					t.Fatalf("health json = %#v, want health.check result", result)
				}
			} else if got := stdout.String(); got != "unhealthy\n" {
				t.Fatalf("health quiet stdout = %q, want unhealthy", got)
			}
			assertEmptyStderr(t, stderr.String())
		}
	})

	t.Run("status", func(t *testing.T) {
		for _, tc := range []struct {
			args      []string
			method    string
			quiet     string
			assertion func(*testing.T, string)
		}{
			{
				args:   []string{"run", "status", "run-1", "--json"},
				method: "run.diagnose",
				assertion: func(t *testing.T, raw string) {
					result := decodeOutputJSON[DiagnosticRunDiagnosisResult](t, raw)
					if result.Run.RunID != "run-1" || stringPointerValue(result.OperationalState) != "stalled" {
						t.Fatalf("status json = %#v, want diagnosis", result)
					}
				},
			},
			{
				args:   []string{"run", "status", "run-1", "--quiet"},
				method: "run.diagnose",
				quiet:  "run-1 stalled\n",
			},
			{
				args:   []string{"run", "status", "run-1", "--no-diagnose", "--json"},
				method: "run.get",
				assertion: func(t *testing.T, raw string) {
					result := decodeOutputJSON[diagnosticRunGetResult](t, raw)
					if result.Run.RunID != "run-1" || result.Run.Status != "running" {
						t.Fatalf("status --no-diagnose json = %#v, want run header", result)
					}
				},
			},
			{
				args:   []string{"run", "status", "run-1", "--no-diagnose", "--quiet"},
				method: "run.get",
				quiet:  "run-1 running\n",
			},
		} {
			server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
				if req.Method != tc.method {
					t.Fatalf("method = %q, want %s", req.Method, tc.method)
				}
				if tc.method == "run.get" {
					return map[string]any{"run": validDiagnosticRunHeader("run-1")}
				}
				return validDiagnosticRunDiagnosis("run-1", "stalled", "delivery_lifecycle", "no_active_deliveries", []any{"dead letters exist for this run"})
			})
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("status %v code = %d stdout=%s stderr=%s", tc.args, code, stdout.String(), stderr.String())
			}
			if len(*requests) != 1 {
				t.Fatalf("status %v requests = %d, want 1", tc.args, len(*requests))
			}
			if tc.assertion != nil {
				tc.assertion(t, stdout.String())
			} else if got := stdout.String(); got != tc.quiet {
				t.Fatalf("status quiet stdout = %q, want %q", got, tc.quiet)
			}
			assertEmptyStderr(t, stderr.String())
		}
	})
}

func TestCLIOutputNoColorStripsDefaultText(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts cliOutputOptions
		env  string
		want string
	}{
		{name: "default preserves ansi", want: "\x1b[32mok\x1b[0m\n"},
		{name: "flag strips ansi", opts: cliOutputOptions{noColor: true}, want: "ok\n"},
		{name: "environment strips ansi", env: "1", want: "ok\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NO_COLOR", tc.env)

			var stdout, stderr bytes.Buffer
			err := renderCLIOutput(&stdout, &stderr, tc.opts, map[string]string{"status": "ok"}, func(w io.Writer) {
				_, _ = io.WriteString(w, "\x1b[32mok\x1b[0m\n")
			}, nil)
			if err != nil {
				t.Fatalf("renderCLIOutput returned error: %v", err)
			}
			if got := stdout.String(); got != tc.want {
				t.Fatalf("stdout = %q, want %q", got, tc.want)
			}
			assertEmptyStderr(t, stderr.String())
		})
	}
}

func TestCLIOutputNoColorForSharedRendererConsumers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   func(*testing.T) []string
		repo   func(*testing.T) string
		method string
		result map[string]any
	}{
		{
			name: "version local",
			args: func(*testing.T) []string { return []string{"version"} },
			repo: func(*testing.T) string { return RepoRoot() },
		},
		{
			name:   "version server",
			args:   func(*testing.T) []string { return []string{"version", "--server"} },
			method: "health.check",
			result: validVersionHealthResult(),
		},
		{
			name: "verify",
			args: func(t *testing.T) []string {
				return []string{"verify", "--contracts", outputModeVerifyFixture(t), "--config", writeTestVerifyRuntimeConfig(t)}
			},
			repo: func(*testing.T) string { return RepoRoot() },
		},
		{
			name:   "health",
			args:   func(*testing.T) []string { return []string{"health"} },
			method: "health.check",
			result: validVersionHealthResult(),
		},
		{
			name:   "runs",
			args:   func(*testing.T) []string { return []string{"run", "list"} },
			method: "run.list",
			result: map[string]any{"runs": []any{validDiagnosticRunHeader("run-1"), validDiagnosticRunHeader("run-2")}},
		},
		{
			name:   "status",
			args:   func(*testing.T) []string { return []string{"run", "status", "run-1"} },
			method: "run.diagnose",
			result: validDiagnosticRunDiagnosis("run-1", "stalled", "delivery_lifecycle", "no_active_deliveries", []any{"dead letters exist for this run"}),
		},
		{
			name:   "conversations list",
			args:   func(*testing.T) []string { return []string{"conversation", "list"} },
			method: conversationListMethod,
			result: map[string]any{"conversations": []map[string]any{validConversationSummary("sess-1")}},
		},
		{
			name:   "conversation view",
			args:   func(*testing.T) []string { return []string{"conversation", "view", "sess-1"} },
			method: conversationListTurnsMethod,
			result: validConversationDetail("sess-1"),
		},
		{
			name:   "conversation turn",
			args:   func(*testing.T) []string { return []string{"conversation", "turn", "sess-1", "turn-2"} },
			method: conversationGetTurnMethod,
			result: validConversationTurnDetail("sess-1", 2),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []struct {
				name       string
				outputMode string
				env        string
				flag       bool
			}{
				{name: "default"},
				{name: "flag", flag: true},
				{name: "environment", env: "1"},
				{name: "json flag", outputMode: "--json", flag: true},
				{name: "json environment", outputMode: "--json", env: "1"},
				{name: "quiet flag", outputMode: "--quiet", flag: true},
				{name: "quiet environment", outputMode: "--quiet", env: "1"},
			} {
				t.Run(mode.name, func(t *testing.T) {
					setCLIAPITestToken(t, "test-token")
					t.Setenv("NO_COLOR", mode.env)

					args := append([]string{}, tc.args(t)...)
					if mode.outputMode != "" {
						args = append(args, mode.outputMode)
					}
					if mode.flag {
						args = append(args, "--no-color")
					}
					repo := t.TempDir()
					if tc.repo != nil {
						repo = tc.repo(t)
					}
					opts := defaultRootCommandOptions()
					var calls *atomic.Int32
					if tc.method != "" {
						server, rpcCalls := newCLIOutputColorPolicyRPCServer(t, tc.method, tc.result)
						defer server.Close()
						opts = testRootCommandOptions(server)
						calls = rpcCalls
					}

					var stdout, stderr bytes.Buffer
					code := executeRootCommandWithOptions(context.Background(), repo, args, &stdout, &stderr, opts)
					if code != 0 {
						t.Fatalf("%s code = %d stdout=%s stderr=%s", strings.Join(args, " "), code, stdout.String(), stderr.String())
					}
					if strings.TrimSpace(stdout.String()) == "" {
						t.Fatalf("%s stdout is empty, want successful output", strings.Join(args, " "))
					}
					assertNoANSI(t, stdout.String())
					if mode.outputMode == "--json" {
						decodeOutputJSON[map[string]any](t, stdout.String())
					}
					assertEmptyStderr(t, stderr.String())
					if calls != nil && calls.Load() != 1 {
						t.Fatalf("%s RPC calls = %d, want 1", strings.Join(args, " "), calls.Load())
					}
				})
			}
		})
	}
}

func TestCLIOutputNoColorDoesNotRewriteMachineModes(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  string
	}{
		{name: "json flag", args: []string{"version", "--json", "--no-color"}},
		{name: "json environment", args: []string{"version", "--json"}, env: "1"},
		{name: "quiet flag", args: []string{"version", "--quiet", "--no-color"}},
		{name: "quiet environment", args: []string{"version", "--quiet"}, env: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NO_COLOR", tc.env)

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), tc.args, &stdout, &stderr, defaultRootCommandOptions())
			if code != 0 {
				t.Fatalf("%s code = %d stdout=%s stderr=%s", strings.Join(tc.args, " "), code, stdout.String(), stderr.String())
			}
			assertNoANSI(t, stdout.String())
			assertEmptyStderr(t, stderr.String())
			if stringSliceContains(tc.args, "--json") {
				result := decodeOutputJSON[versionOutputResult](t, stdout.String())
				metadata := currentTestVersionMetadata(t)
				if result.BinaryVersion != metadata.BinaryVersion {
					t.Fatalf("version json = %#v, want binary version %q", result, metadata.BinaryVersion)
				}
			} else if got := stdout.String(); got != currentTestVersionMetadata(t).BinaryVersion+"\n" {
				t.Fatalf("version quiet stdout = %q, want binary version", got)
			}
		})
	}
}

func TestCLIOutputModeCollisionFailsBeforeSideEffects(t *testing.T) {
	setCLIAPITestToken(t, "test-token")

	for _, args := range [][]string{
		{"version", "--json", "--quiet", "--no-color"},
		{"version", "--server", "--json", "--quiet"},
		{"health", "--json", "--quiet"},
		{"run", "list", "--json", "--quiet"},
		{"run", "status", "run-1", "--json", "--quiet"},
		{"conversation", "list", "--json", "--quiet"},
		{"conversation", "view", "sess-1", "--json", "--quiet"},
		{"conversation", "turn", "sess-1", "1", "--json", "--quiet"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				writeJSONRPCResult(t, w, "unexpected", map[string]any{})
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != CLIExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), "--json and --quiet are mutually exclusive") {
				t.Fatalf("stderr = %q, want collision", stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if calls.Load() != 0 {
				t.Fatalf("RPC calls = %d, want 0", calls.Load())
			}
		})
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), RepoRoot(), []string{"verify", "--json", "--quiet"}, &stdout, &stderr)
	if code != CLIExitValidation {
		t.Fatalf("verify collision code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--json and --quiet are mutually exclusive") {
		t.Fatalf("verify collision stderr = %q, want collision", stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify collision stdout = %q, want empty", stdout.String())
	}
}

func TestCLIOutputModeExceptionRowsFailClosedBeforeSideEffects(t *testing.T) {
	setCLIAPITestToken(t, "test-token")

	var rpcCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcCalls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	var serveCalls atomic.Int32
	opts := testRootCommandOptions(server)
	opts.runServe = func(context.Context, string, ServeOptions) int {
		serveCalls.Add(1)
		return 0
	}

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "root", args: []string{"--json", "runs"}, wantStderr: "unknown flag: --json"},
		{name: "root no-color", args: []string{"--no-color", "runs"}, wantStderr: "unknown flag: --no-color"},
		{name: "completion", args: []string{"completion", "bash", "--json"}, wantStderr: "unknown flag"},
		{name: "completion no-color", args: []string{"completion", "bash", "--no-color"}, wantStderr: "unknown flag"},
		{name: "serve", args: []string{"serve", "--json"}, wantStderr: "unknown flag"},
		{name: "serve no-color", args: []string{"serve", "--no-color"}, wantStderr: "unknown flag"},
		{name: "serve log-level", args: []string{"serve", "--log-level", "debug"}, wantStderr: "unknown flag"},
		{name: "retired investigate", args: []string{"investigate", "runs", "--json"}, wantStderr: "retired in CLI v2"},
		{name: "retired investigate no-color", args: []string{"investigate", "runs", "--no-color"}, wantStderr: "retired in CLI v2"},
		{name: "forkchat parent", args: []string{"forkchat", "--json"}, wantStderr: "unknown flag: --json"},
		{name: "forkchat parent no-color", args: []string{"forkchat", "--no-color"}, wantStderr: "unknown flag: --no-color"},
		{name: "split log-level", args: []string{"logs", "--log-level", "debug"}, wantStderr: "unknown flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeRPC := rpcCalls.Load()
			beforeServe := serveCalls.Load()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, opts)
			if code != CLIExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.wantStderr)
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if rpcCalls.Load() != beforeRPC {
				t.Fatalf("RPC calls changed from %d to %d", beforeRPC, rpcCalls.Load())
			}
			if serveCalls.Load() != beforeServe {
				t.Fatalf("serve calls changed from %d to %d", beforeServe, serveCalls.Load())
			}
		})
	}
}

func decodeOutputJSON[T any](t *testing.T, raw string) T {
	t.Helper()
	var out T
	dec := json.NewDecoder(strings.NewReader(raw))
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("decode JSON output %q: %v", raw, err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		t.Fatalf("JSON output contains more than one document: %q", raw)
	}
	return out
}

func assertEmptyStderr(t *testing.T, raw string) {
	t.Helper()
	if strings.TrimSpace(raw) != "" {
		t.Fatalf("stderr = %q, want empty", raw)
	}
}

func assertNoANSI(t *testing.T, raw string) {
	t.Helper()
	if cliANSISequencePattern.MatchString(raw) {
		t.Fatalf("output contains ANSI escape sequence: %q", raw)
	}
}

func newCLIOutputColorPolicyRPCServer(t *testing.T, wantMethod string, result map[string]any) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.Method != wantMethod {
			t.Errorf("method = %q, want %s", req.Method, wantMethod)
		}
		writeJSONRPCResult(t, w, req.ID, result)
	}))
	return server, &calls
}

func outputModeVerifyFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyOutputModeVerify(t)
}
