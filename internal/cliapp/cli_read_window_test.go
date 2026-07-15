package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cli/readwindow"
)

var cliReadWindowSupportedCommands = []struct {
	name       string
	args       []string
	wantMethod string
}{
	{name: "run list", args: []string{"run", "list"}, wantMethod: "run.list"},
	{name: "run trace", args: []string{"run", "trace", "run-1"}, wantMethod: "run.trace"},
	{name: "event list", args: []string{"event", "list"}, wantMethod: "event.list"},
	{name: "logs", args: []string{"logs"}, wantMethod: "runtime.logs"},
}

func TestParseCLIReadWindowDuration(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  time.Duration
	}{
		{input: "250ms", want: 250 * time.Millisecond},
		{input: "45s", want: 45 * time.Second},
		{input: "90m", want: 90 * time.Minute},
		{input: "2h", want: 2 * time.Hour},
		{input: "1h30m15s250ms", want: time.Hour + 30*time.Minute + 15*time.Second + 250*time.Millisecond},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, ok := readwindow.ParseDuration(tc.input)
			if !ok || got != tc.want {
				t.Fatalf("readwindow.ParseDuration(%q) = %s, %t; want %s, true", tc.input, got, ok, tc.want)
			}
		})
	}

	for _, input := range []string{
		"", " ", "0h", "1h0m", "0h1m", "-1h", "+1h", "1.5h", "1", "1H", "1M",
		"1d", "1w", "1us", "1ns", "1hm", "1ms2", "ms", "999999999999999999999999999999h",
		"2562047788015216h",
	} {
		t.Run("reject_"+strings.ReplaceAll(input, " ", "space"), func(t *testing.T) {
			if got, ok := readwindow.ParseDuration(input); ok {
				t.Fatalf("readwindow.ParseDuration(%q) = %s, true; want rejection", input, got)
			}
		})
	}
}

func TestCLIReadWindowEveryPublicBoundAcceptsRelativeInput(t *testing.T) {
	reference := time.Date(2026, 7, 11, 13, 0, 0, 123456789, time.UTC)
	for _, command := range cliReadWindowSupportedCommands {
		for _, bound := range []struct {
			flag string
			raw  string
			want string
		}{
			{flag: "--since", raw: "2h", want: "2026-07-11T11:00:00.123456789Z"},
			{flag: "--until", raw: "30m", want: "2026-07-11T12:30:00.123456789Z"},
		} {
			t.Run(command.name+"_"+bound.flag, func(t *testing.T) {
				request, calls, code, stderr := executeCLIReadWindowCommand(t, append(append([]string{}, command.args...), bound.flag, bound.raw), reference)
				if code != 0 {
					t.Fatalf("code = %d stderr=%s", code, stderr)
				}
				if calls != 1 || request.Method != command.wantMethod {
					t.Fatalf("calls/method = %d/%q, want 1/%q", calls, request.Method, command.wantMethod)
				}
				key := strings.TrimPrefix(bound.flag, "--")
				if got := request.Params[key]; got != bound.want {
					t.Fatalf("%s = %#v, want %q", key, got, bound.want)
				}
				other := "since"
				if key == "since" {
					other = "until"
				}
				if _, ok := request.Params[other]; ok {
					t.Fatalf("params = %#v, want %s omitted", request.Params, other)
				}
			})
		}
	}
}

func TestCLIReadWindowSupportedSurfacesCanonicalizeMixedBounds(t *testing.T) {
	reference := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	for _, command := range cliReadWindowSupportedCommands {
		t.Run(command.name, func(t *testing.T) {
			args := append(append([]string{}, command.args...),
				"--since", "2026-07-11T08:00:00-04:00",
				"--until", "30m",
			)
			request, calls, code, stderr := executeCLIReadWindowCommand(t, args, reference)
			if code != 0 || calls != 1 {
				t.Fatalf("code/calls = %d/%d stderr=%s", code, calls, stderr)
			}
			if got := request.Params["since"]; got != "2026-07-11T12:00:00Z" {
				t.Fatalf("since = %#v, want canonical UTC", got)
			}
			if got := request.Params["until"]; got != "2026-07-11T12:30:00Z" {
				t.Fatalf("until = %#v, want relative UTC", got)
			}
		})
	}
}

func TestCLIReadWindowSupportedSurfacesAcceptEqualBounds(t *testing.T) {
	reference := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	for _, command := range cliReadWindowSupportedCommands {
		t.Run(command.name, func(t *testing.T) {
			args := append(append([]string{}, command.args...),
				"--since", "2026-07-11T12:00:00Z",
				"--until", "1h",
			)
			request, calls, code, stderr := executeCLIReadWindowCommand(t, args, reference)
			if code != 0 || calls != 1 {
				t.Fatalf("code/calls = %d/%d stderr=%s", code, calls, stderr)
			}
			if request.Params["since"] != request.Params["until"] {
				t.Fatalf("params = %#v, want equal canonical bounds", request.Params)
			}
		})
	}
}

func TestCLIReadWindowSupportedSurfacesRejectReversedBoundsWithoutRequest(t *testing.T) {
	reference := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	for _, command := range cliReadWindowSupportedCommands {
		t.Run(command.name, func(t *testing.T) {
			args := append(append([]string{}, command.args...), "--since", "30m", "--until", "2h")
			_, calls, code, stderr := executeCLIReadWindowCommand(t, args, reference)
			if code != 2 || calls != 0 {
				t.Fatalf("code/calls = %d/%d, want 2/0 stderr=%s", code, calls, stderr)
			}
			if !strings.Contains(stderr, "--until must be greater than or equal to --since") {
				t.Fatalf("stderr = %q", stderr)
			}
		})
	}
}

func TestCLIReadWindowEveryPublicBoundRejectsExplicitBlankWithoutRequest(t *testing.T) {
	reference := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	for _, command := range cliReadWindowSupportedCommands {
		for _, flag := range []string{"--since", "--until"} {
			t.Run(command.name+"_"+flag, func(t *testing.T) {
				args := append(append([]string{}, command.args...), flag, " ")
				_, calls, code, stderr := executeCLIReadWindowCommand(t, args, reference)
				if code != 2 || calls != 0 {
					t.Fatalf("code/calls = %d/%d, want 2/0 stderr=%s", code, calls, stderr)
				}
				if !strings.Contains(stderr, flag+" must not be empty") {
					t.Fatalf("stderr = %q", stderr)
				}
			})
		}
	}
}

func TestCLIReadWindowMalformedClassesFailBeforeRequest(t *testing.T) {
	reference := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	cases := []struct {
		command int
		flag    string
		value   string
	}{
		{command: 0, flag: "--since", value: "0h"},
		{command: 0, flag: "--until", value: "-1h"},
		{command: 1, flag: "--since", value: "+1h"},
		{command: 1, flag: "--until", value: "1.5h"},
		{command: 2, flag: "--since", value: "1d"},
		{command: 2, flag: "--until", value: "1H"},
		{command: 3, flag: "--since", value: "1hm"},
		{command: 3, flag: "--until", value: "999999999999999999999999999999h"},
	}
	for _, tc := range cases {
		command := cliReadWindowSupportedCommands[tc.command]
		t.Run(command.name+"_"+tc.flag+"_"+tc.value, func(t *testing.T) {
			args := append(append([]string{}, command.args...), tc.flag, tc.value)
			_, calls, code, stderr := executeCLIReadWindowCommand(t, args, reference)
			if code != 2 || calls != 0 {
				t.Fatalf("code/calls = %d/%d, want 2/0 stderr=%s", code, calls, stderr)
			}
			if !strings.Contains(stderr, "positive relative duration") {
				t.Fatalf("stderr = %q", stderr)
			}
		})
	}
}

func executeCLIReadWindowCommand(t *testing.T, args []string, reference time.Time) (jsonRPCRequest, int, int, string) {
	t.Helper()
	setCLIAPITestToken(t, "test-token")
	var request jsonRPCRequest
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		result := map[string]any{}
		switch request.Method {
		case "run.list":
			result["runs"] = []any{}
		case "run.trace":
			result["trace"] = []any{}
		case "event.list":
			result["events"] = []any{}
		case "runtime.logs":
			result["logs"] = []any{}
		default:
			t.Errorf("unexpected method %q", request.Method)
		}
		writeJSONRPCResult(t, w, request.ID, result)
	}))
	defer server.Close()

	nowCalls := 0
	opts := testRootCommandOptions(server)
	opts.now = func() time.Time {
		nowCalls++
		return reference
	}
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, opts)
	if nowCalls != 1 {
		t.Fatalf("invocation clock calls = %d, want 1", nowCalls)
	}
	return request, calls, code, stderr.String()
}
