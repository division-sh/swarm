package cliapp

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCLIAPIResponseBudgetErrorNamesMethodAndBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"swarm-cli:event.list","result":{"events":[{`))
		_, _ = w.Write(bytes.Repeat([]byte("a"), cliAPIResponseBudget))
		_, _ = w.Write([]byte(`}]}}`))
	}))
	defer server.Close()

	client := &cliAPIClient{endpoint: server.URL, token: "test-token", httpClient: server.Client()}
	var result eventListResult
	err := client.call(context.Background(), "event.list", map[string]any{"filter": map[string]any{"run_id": "run-1"}}, &result)
	if err == nil {
		t.Fatal("over-budget response must fail")
	}
	var budgetErr *cliAPIResponseBudgetError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("error = %T %v, want cliAPIResponseBudgetError", err, err)
	}
	if budgetErr.Method != "event.list" || budgetErr.Transport != "http" || budgetErr.Budget != cliAPIResponseBudget {
		t.Fatalf("budget error = %#v", budgetErr)
	}
	for _, want := range []string{"event.list", "1 MiB", "limit", "cursor"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error missing %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "unexpected end of JSON input") {
		t.Fatalf("over-budget response must never surface as a JSON parse error: %v", err)
	}
}

func TestCLIAPIResponseBudgetErrorRendersThroughFormatCLIAPIError(t *testing.T) {
	err := &cliAPIResponseBudgetError{
		Method: "uncatalogued.example", Surface: "runtime API", Transport: "http",
		Budget: cliAPIResponseBudget, Overrun: cliAPIResponseBudget + 1,
	}
	rendered := FormatCLIAPIError(err)
	for _, want := range []string{"uncatalogued.example", "1 MiB", "unbounded by contract", "absent from the method catalog"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered error missing %q: %s", want, rendered)
		}
	}
}

func TestResponseBudgetRemediationDerivesFromCatalog(t *testing.T) {
	bounded := responseBudgetRemediation("event.list")
	for _, want := range []string{"limit", "cursor"} {
		if !strings.Contains(bounded, want) {
			t.Fatalf("bounded remediation missing %q: %s", want, bounded)
		}
	}
	unbounded := responseBudgetRemediation("uncatalogued.example")
	if !strings.Contains(unbounded, "unbounded by contract") || !strings.Contains(unbounded, "absent from the method catalog") {
		t.Fatalf("unbounded remediation = %q", unbounded)
	}
}

func TestEventListOverBudgetResponseExitsRuntimeWithTeachingText(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"swarm-cli:event.list","result":{"events":[{`))
		_, _ = w.Write(bytes.Repeat([]byte("a"), cliAPIResponseBudget))
		_, _ = w.Write([]byte(`}]}}`))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"event", "list", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != CLIExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitRuntime, stdout.String(), stderr.String())
	}
	for _, want := range []string{"event.list", "1 MiB", "limit", "cursor"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
	if strings.Contains(stderr.String(), "unexpected end of JSON input") {
		t.Fatalf("stderr surfaced a JSON parse error:\n%s", stderr.String())
	}
}
