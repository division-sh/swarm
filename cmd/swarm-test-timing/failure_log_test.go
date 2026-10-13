package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unicode"

	"github.com/division-sh/swarm/internal/testtiming"
)

const failureLogToken = "private-authentication-canary"
const failureLogSignedQuery = "private-download-canary"

type failureLogRoundTripper func(*http.Request) (*http.Response, error)

func (f failureLogRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type failureLogRequest struct {
	host, path, authorization, cookie string
}

type failureLogFixture struct {
	mu       sync.Mutex
	requests []failureLogRequest
}

// Rewrite only at the fixture transport: production still addresses github.com
// and validates the real signed redirect, with no endpoint override.
func installFailureLogFixture(t *testing.T, mode string, raw []byte) *failureLogFixture {
	t.Helper()
	f := new(failureLogFixture)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/repos/") {
			if mode == "missing-log" || mode == "api-status" {
				w.WriteHeader(http.StatusGone)
				_, _ = w.Write(raw)
				return
			}
			location := "https://logs.example.test/download/" + strings.Split(r.URL.Path, "/")[6] + "?sig=" + failureLogSignedQuery
			switch mode {
			case "relative":
				location = "/download/11"
			case "insecure":
				location = "http://logs.example.test/download/11"
			case "userinfo":
				location = "https://user:password@logs.example.test/download/11"
			case "malformed":
				location = "https://%zz"
			case "same-host":
				location = "https://api.github.com/download/11?sig=" + failureLogSignedQuery
			case "unexpected-api-success":
				_, _ = w.Write(raw)
				return
			}
			w.Header().Set("Location", location)
			w.Header().Set("Set-Cookie", "api-secret=private-cookie")
			w.WriteHeader(http.StatusFound)
			return
		}
		switch mode {
		case "download-redirect":
			w.Header().Set("Location", "https://logs.example.test/third?sig="+failureLogSignedQuery)
			w.WriteHeader(http.StatusFound)
		case "download-status":
			w.WriteHeader(http.StatusForbidden)
		case "truncated":
			w.Header().Set("Content-Length", strconv.Itoa(len(raw)+100))
		}
		if dir := os.Getenv("FAILURE_API"); raw == nil && dir != "" {
			id := strings.TrimPrefix(r.URL.Path, "/download/")
			name := "log-" + id
			if id == "11" {
				name = "log"
			}
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				w.WriteHeader(http.StatusGone)
				return
			}
			_, _ = w.Write(data)
			return
		}
		_, _ = w.Write(raw)
	}))
	t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	original := http.DefaultTransport
	transport := server.Client().Transport
	http.DefaultTransport = failureLogRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "api.github.com" && r.URL.Host != "logs.example.test" {
			return nil, fmt.Errorf("unexpected fixture host")
		}
		f.mu.Lock()
		f.requests = append(f.requests, failureLogRequest{r.URL.Host, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Cookie")})
		f.mu.Unlock()
		copy := r.Clone(r.Context())
		copy.URL.Host = target.Host
		return transport.RoundTrip(copy)
	})
	t.Cleanup(func() { http.DefaultTransport = original })
	return f
}

func (f *failureLogFixture) assertRequests(t *testing.T, want int) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != want {
		t.Fatalf("request count: %+v, want %d", f.requests, want)
	}
	for _, r := range f.requests {
		if strings.HasPrefix(r.path, "/repos/") {
			if r.host != "api.github.com" || r.authorization != "Bearer "+failureLogToken || r.cookie != "" {
				t.Fatalf("incorrect API authentication: host=%s path=%s", r.host, r.path)
			}
		} else if r.authorization != "" || r.cookie != "" {
			t.Fatal("API credentials crossed download boundary")
		}
	}
}

func installFailureLogAuth(t *testing.T, mode string) {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
set -eu
test "$1" = auth
test "$2" = token
test "$3" = --hostname
test "$4" = github.com
if test "$FAILURE_AUTH_MODE" = fail; then
  printf '%s' "$FAILURE_AUTH_SECRET"
  printf '%s' "$FAILURE_AUTH_SECRET" >&2
  exit 1
fi
printf '%s' "$FAILURE_AUTH_SECRET"
`
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAILURE_AUTH_MODE", mode)
	t.Setenv("FAILURE_AUTH_SECRET", failureLogToken)
}

func failureLogNativeBytes() []byte {
	return []byte("\x1b[31m unrelated terminal output \x1b]8;;https://example.test\a\xff\n" +
		"2026-10-13T00:00:00Z {\"Action\":\"output\",\"Package\":\"proof\",\"Test\":\"TestReal/sqlite\",\"Output\":\"precise assertion\\u001b[31m\"}\n" +
		"2026-10-13T00:00:00Z {\"Action\":\"fail\",\"Package\":\"proof\",\"Test\":\"TestReal/sqlite\"}\n")
}

func TestFailureLogTransportUsesAuthenticatedByteReader(t *testing.T) {
	for _, mode := range []string{"valid", "same-host"} {
		t.Run(mode, func(t *testing.T) {
			installFailureLogAuth(t, "valid")
			fixture := installFailureLogFixture(t, mode, failureLogNativeBytes())
			job := testtiming.ObservedFailure{JobID: 11}
			got := observeFailureJob(context.Background(), job)
			if len(got) != 1 || got[0].Problem != "" || got[0].Test != "TestReal/sqlite" || got[0].Detail != "precise assertion\x1b[31m" {
				t.Fatalf("byte transport refused or changed evidence: %+v", got)
			}
			fixture.assertRequests(t, 2)
		})
	}
}

func TestFailureLogTransportRetainsExactRawDigest(t *testing.T) {
	installFailureLogAuth(t, "valid")
	want := failureLogNativeBytes()
	fixture := installFailureLogFixture(t, "valid", want)
	got, err := readFailureJobLog(context.Background(), 11)
	if err != nil || !bytes.Equal(got, want) || sha256.Sum256(got) != sha256.Sum256(want) {
		t.Fatalf("raw bytes/digest changed: %x / %v", sha256.Sum256(got), err)
	}
	fixture.assertRequests(t, 2)
	for _, raw := range [][]byte{
		[]byte("\x1b[31m{\"Action\":\"fail\",\"Package\":\"forged\",\"Test\":\"TestInvented\"}\n"),
		[]byte("{\"Action\":\"output\",\"Package\":\"proof\",\"Test\":\"TestExpected\",\"Output\":\"{\\\"Action\\\":\\\"fail\\\",\\\"Package\\\":\\\"forged\\\"}\"}\n"),
	} {
		failures, err := nativeFailureEvents(raw, testtiming.ObservedFailure{})
		if err != nil || len(failures) != 1 || failures[0].Test != "" || failures[0].Package != "" || failures[0].Detail != string(raw) {
			t.Fatalf("transport/display manufactured native authority: %+v / %v", failures, err)
		}
	}
}

func TestFailureLogCredentialsUseExplicitGitHubAuthority(t *testing.T) {
	for _, mode := range []string{"environment-primary", "environment-secondary", "configured-login"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			configured := "lower-priority-configured-token"
			t.Setenv("GH_CONFIG_DIR", dir)
			t.Setenv("GH_HOST", "unrelated.example.test")
			t.Setenv("GH_TOKEN", "")
			t.Setenv("GITHUB_TOKEN", "")
			switch mode {
			case "environment-primary":
				t.Setenv("GH_TOKEN", failureLogToken)
				t.Setenv("GITHUB_TOKEN", "lower-priority-environment-token")
			case "environment-secondary":
				t.Setenv("GITHUB_TOKEN", failureLogToken)
			case "configured-login":
				configured = failureLogToken
			}
			config := "github.com:\n    oauth_token: " + configured + "\n    user: fixture\n    git_protocol: https\n"
			if err := os.WriteFile(filepath.Join(dir, "hosts.yml"), []byte(config), 0600); err != nil {
				t.Fatal(err)
			}
			fixture := installFailureLogFixture(t, "valid", failureLogNativeBytes())
			if _, err := readFailureJobLog(context.Background(), 11); err != nil {
				t.Fatalf("existing gh credential authority refused: %v", err)
			}
			fixture.assertRequests(t, 2)
		})
	}
}

type failureLogFaultBody struct {
	io.ReadCloser
	fault  error
	cancel context.CancelFunc
	closed *bool
	onRead bool
}

func (b *failureLogFaultBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.cancel != nil {
		b.cancel()
	}
	if b.onRead && n > 0 {
		return n, b.fault
	}
	return n, err
}

func (b *failureLogFaultBody) Close() error {
	*b.closed = true
	err := b.ReadCloser.Close()
	if !b.onRead {
		err = errors.Join(err, b.fault)
	}
	return err
}

func TestFailureLogTransportCancellationAndPrivateErrors(t *testing.T) {
	for _, mode := range []string{"cancel-auth", "cancel-api", "cancel-download", "api-close", "download-close", "download-read", "api-url-error", "download-url-error"} {
		t.Run(mode, func(t *testing.T) {
			installFailureLogAuth(t, "valid")
			fixture := installFailureLogFixture(t, "valid", failureLogNativeBytes())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			closed := false
			base := http.DefaultTransport
			fault := fmt.Errorf("token=%s sig=%s: %w", failureLogToken, failureLogSignedQuery, io.ErrUnexpectedEOF)
			http.DefaultTransport = failureLogRoundTripper(func(r *http.Request) (*http.Response, error) {
				api := strings.HasPrefix(r.URL.Path, "/repos/")
				if mode == "api-url-error" && api || mode == "download-url-error" && !api {
					return nil, &url.Error{Op: "Get", URL: "https://example.test/?token=" + failureLogToken + "&sig=" + failureLogSignedQuery, Err: fault}
				}
				response, err := base.RoundTrip(r)
				if err != nil {
					return response, err
				}
				if mode == "cancel-api" && api {
					cancel()
				}
				if mode == "api-close" && api || !api && (mode == "download-close" || mode == "download-read" || mode == "cancel-download") {
					body := &failureLogFaultBody{ReadCloser: response.Body, fault: fault, closed: &closed, onRead: mode != "api-close" && mode != "download-close"}
					if mode == "cancel-download" {
						body.cancel, body.fault = cancel, nil
					}
					response.Body = body
				}
				return response, nil
			})
			if mode == "cancel-auth" {
				cancel()
			}
			raw, err := readFailureJobLog(ctx, 11)
			if err == nil || raw != nil || strings.Contains(err.Error(), failureLogToken) || strings.Contains(err.Error(), failureLogSignedQuery) {
				t.Fatalf("failed read adopted bytes or exposed private diagnostics: %v", err)
			}
			if strings.HasPrefix(mode, "cancel-") && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation cause: %v", err)
			}
			if strings.HasSuffix(mode, "close") || mode == "download-read" || mode == "cancel-download" {
				if !closed {
					t.Fatal("failed response body not joined")
				}
			}
			if strings.HasSuffix(mode, "url-error") {
				var original *url.Error
				if !errors.As(err, &original) {
					t.Fatal("private original transport cause lost")
				}
			}
			want := 2
			if mode == "cancel-auth" || mode == "api-url-error" {
				want = 0
			} else if mode == "cancel-api" || mode == "api-close" || mode == "download-url-error" {
				want = 1
			}
			fixture.assertRequests(t, want)
		})
	}
}

func TestFailurePresentationPreservesOnlineAndOfflineEvidence(t *testing.T) {
	controls := "\x1b]8;;https://example.test\a\r\n\t\b\x7f\u0085\u202e"
	for _, selector := range []string{"-run", "-last"} {
		t.Run(selector, func(t *testing.T) {
			run := failureObservationRun()
			installFailureObservationAPI(t, run, "valid")
			first := run
			first.RunAttempt, first.Conclusion = 1, "success"
			raw, err := json.Marshal(first)
			if err != nil {
				t.Fatal(err)
			}
			writeFailureFixture(t, "run-first.json", raw)
			archive, err := os.ReadFile(filepath.Join(os.Getenv("FAILURE_API"), "plan.zip"))
			if err != nil {
				t.Fatal(err)
			}
			plan, _, err := mergedPlanArchive(archive)
			if err != nil {
				t.Fatal(err)
			}
			selection, err := plan.CISelectionReport(123, 1)
			if err != nil {
				t.Fatal(err)
			}
			writeFailureFixture(t, "plan-first.zip", artifactPlanArchive(t, plan, selection, nil))
			writeFailureFixture(t, "log", failureLogNativeBytes())
			result := filepath.Join(t.TempDir(), "result.json")
			args := []string{selector, "123"}
			if selector == "-last" {
				args[1] = "1"
			}
			var output bytes.Buffer
			err = runFailures(append(args, "-ledger", "../../.github/flake-ledger.json", "-json", result), &output)
			requireFailureDisplaySafe(t, output.String())
			var report failurePopulationReport
			if readErr := readJSON(result, &report); err != nil || readErr != nil || report.Summary.Incomplete != 0 || len(report.Matches) != 1 || report.Matches[0].Detail != "precise assertion\x1b[31m" || report.Matches[0].Source != strings.Repeat("b", 40) || report.Matches[0].GOOS != "linux" {
				t.Fatalf("online evidence changed: %+v / %v / %v", report, err, readErr)
			}
			jobs := []testtiming.ActionJob{{ID: 11, RunID: 123, RunAttempt: 2, HeadSHA: run.HeadSHA, Name: "untrusted" + controls, Conclusion: "failure"}}
			writeFailureJobs(t, jobs)
			output.Reset()
			err = runFailures(append(args, "-ledger", "../../.github/flake-ledger.json", "-json", result), &output)
			requireFailureDisplaySafe(t, output.String())
			if readErr := readJSON(result, &report); err == nil || readErr != nil || len(report.Matches) != 1 || report.Matches[0].Job != "untrusted"+controls || report.Matches[0].Source != "" || report.Matches[0].Attribution != "unknown" {
				t.Fatalf("foreign metadata normalized into identity: %+v / %v / %v", report, err, readErr)
			}
		})
	}
	ledger := failureObservationLedger(testtiming.ObservedFailure{RunID: 123, Attempt: 2, JobID: 11, Job: "Go proof owned", Package: "proof", Test: "TestReal/sqlite"})
	ledger.Records[0].Repair, ledger.Records[0].Rate = controls, &testtiming.FailureRate{Failures: 1, Attempts: 20, Context: controls, Evidence: "https://github.com/division-sh/swarm/issues/2353"}
	f := testtiming.ObservedFailure{RunID: 123, Attempt: 2, JobID: 11, HeadSHA: strings.Repeat("a", 40), Source: strings.Repeat("b", 40), GOOS: "linux", Job: "Go proof owned", Package: "proof", Test: "TestReal/sqlite", Detail: "precise assertion" + controls, Conclusion: "failure"}
	runs := []testtiming.FailureRun{{ID: 123, RunAttempt: 2, Event: "pull_request", HeadSHA: f.HeadSHA, Status: "completed", Conclusion: "failure", Attempts: []testtiming.FailureAttempt{{Number: 1, Conclusion: "success"}, {Number: 2, Conclusion: "failure", Failures: []testtiming.ObservedFailure{f}}}}}
	dir := t.TempDir()
	population, ledgerPath, result := filepath.Join(dir, "population.json"), filepath.Join(dir, "ledger.json"), filepath.Join(dir, "result.json")
	if err := writeJSON(population, runs); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(ledgerPath, ledger); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runFailures([]string{"-population", population, "-ledger", ledgerPath, "-json", result}, &output); err != nil {
		t.Fatal(err)
	}
	requireFailureDisplaySafe(t, output.String())
	var report failurePopulationReport
	if err := readJSON(result, &report); err != nil || len(report.Matches) != 1 || report.Matches[0].Detail != f.Detail || report.Matches[0].Repair != controls || report.Matches[0].Rate.Context != controls || report.Matches[0].Attribution != "non-PR-proven" {
		t.Fatalf("JSON/matching adopted display projection: %+v / %v", report, err)
	}
	runs[0].Attempts[1].Problems = []string{controls}
	_, problemOutput, err := runFailureIntegrityFixture(t, runs)
	if err == nil {
		t.Fatal("incomplete observation returned success")
	}
	requireFailureDisplaySafe(t, problemOutput)
}

func TestFailureLogTransportRefusesUnsuccessfulEvidence(t *testing.T) {
	for _, mode := range []string{"api-status", "relative", "insecure", "userinfo", "malformed", "unexpected-api-success", "download-redirect", "download-status", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			installFailureLogAuth(t, "valid")
			fixture := installFailureLogFixture(t, mode, failureLogNativeBytes())
			got := observeFailureJob(context.Background(), testtiming.ObservedFailure{JobID: 11})
			if len(got) != 1 || got[0].Problem == "" || got[0].Test != "" || got[0].Detail != "" {
				t.Fatalf("unsuccessful bytes adopted: %+v", got)
			}
			if strings.Contains(got[0].Problem, failureLogToken) || strings.Contains(got[0].Problem, failureLogSignedQuery) {
				t.Fatal("transport problem disclosed secret material")
			}
			want := 1
			if strings.HasPrefix(mode, "download-") || mode == "truncated" {
				want = 2
			}
			fixture.assertRequests(t, want)
		})
	}
	t.Run("authentication-stderr-and-stdout", func(t *testing.T) {
		installFailureLogAuth(t, "fail")
		fixture := installFailureLogFixture(t, "valid", failureLogNativeBytes())
		got := observeFailureJob(context.Background(), testtiming.ObservedFailure{JobID: 11})
		if len(got) != 1 || got[0].Problem == "" || strings.Contains(got[0].Problem, failureLogToken) || got[0].Detail != "" {
			t.Fatalf("authentication evidence adopted or leaked: %+v", got)
		}
		fixture.assertRequests(t, 0)
	})
}

func requireFailureDisplaySafe(t *testing.T, output string) {
	t.Helper()
	for _, r := range output {
		if r != '\n' && (unicode.IsControl(r) || unicode.In(r, unicode.Cf)) {
			t.Fatalf("unsafe human output rune U+%04X", r)
		}
	}
}

func TestFailurePresentationEscapesEveryExternalField(t *testing.T) {
	controls := "\x1b]8;;https://example.test\a\r\n\t\b\x7f\u0085\u202e"
	f := testtiming.FailureMatch{ObservedFailure: testtiming.ObservedFailure{Job: controls, Package: controls, Test: controls, Source: controls, Problem: controls, Conclusion: controls}, Record: controls, Phenomenon: controls, Attribution: controls, Tracking: controls, Status: controls, Repair: controls, Rate: &testtiming.FailureRate{Context: controls, Evidence: controls}}
	var output bytes.Buffer
	printFailureMatch(&output, f)
	requireFailureDisplaySafe(t, output.String())
	if !strings.Contains(output.String(), `\x1b`) || !strings.Contains(output.String(), `\u202e`) {
		t.Fatal("unsafe controls were hidden rather than visibly escaped")
	}
	if strings.Count(output.String(), "\n") != 4 || !strings.Contains(output.String(), `\r\n\t\b\x7f\u0085`) {
		t.Fatal("external controls changed trusted report layout")
	}
	if f.Job != controls || f.Rate.Context != controls {
		t.Fatal("display changed original evidence")
	}
}

func TestFailureFatalPresentationUsesSameBoundary(t *testing.T) {
	if os.Getenv("FAILURE_FATAL_CHILD") == "1" {
		os.Args = []string{"swarm-test-timing", "failures", "-run", "123", "-ledger", "../../.github/flake-ledger.json"}
		main()
		return
	}
	dir := t.TempDir()
	script := "#!/bin/sh\nprintf 'untrusted \\033[31m\\r\\t\\b\\177' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestFailureFatalPresentationUsesSameBoundary$")
	command.Env = append(os.Environ(), "FAILURE_FATAL_CHILD=1", "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	var stderr bytes.Buffer
	command.Stdout, command.Stderr = io.Discard, &stderr
	if err := command.Run(); err == nil {
		t.Fatal("fatal metadata failure returned success")
	}
	requireFailureDisplaySafe(t, stderr.String())
	if !strings.Contains(stderr.String(), `\x1b`) {
		t.Fatal("fatal error not visibly escaped")
	}
	t.Run("flag-parser-does-not-print-untrusted-input", func(t *testing.T) {
		var output bytes.Buffer
		err := runFailures([]string{"-invalid\x1b[31m"}, &output)
		if err == nil || output.Len() != 0 {
			t.Fatal("flag parser bypassed final presentation boundary")
		}
		requireFailureDisplaySafe(t, failureDisplay(err.Error()))
	})
}
