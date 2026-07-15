package cliapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
)

type fakeRuntimeIdentityCaller struct {
	result apiv1.RuntimeIdentityResult
	err    error
	calls  int
}

func (f *fakeRuntimeIdentityCaller) callRuntimeIdentity(context.Context, string, string) (apiv1.RuntimeIdentityResult, error) {
	f.calls++
	if f.err != nil {
		return apiv1.RuntimeIdentityResult{}, f.err
	}
	return f.result, nil
}

func TestLocalContextRegistryAtomicWriteCurrentAndIdentityValidation(t *testing.T) {
	registry := newLocalContextRegistry(t.TempDir())
	desc := testLocalContextDescriptor("local", "runtime-a")
	if err := registry.WriteDescriptor(desc); err != nil {
		t.Fatalf("WriteDescriptor() error = %v", err)
	}
	if err := registry.SetCurrent(desc.Name); err != nil {
		t.Fatalf("SetCurrent() error = %v", err)
	}
	caller := &fakeRuntimeIdentityCaller{result: apiv1.RuntimeIdentityResult{
		RuntimeInstanceID:   "runtime-a",
		StartedAt:           "2026-07-02T00:00:00Z",
		APIVersion:          "v1",
		SupportedTransports: []string{"tcp"},
	}}
	report, err := registry.Inspect(context.Background(), caller)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if report.Status != localContextStatusOK {
		t.Fatalf("report status = %q, want ok detail=%s", report.Status, report.Detail)
	}
	if report.Current == nil || report.Current.Descriptor.Name != "local" {
		t.Fatalf("current = %#v, want local", report.Current)
	}
	if caller.calls != 1 {
		t.Fatalf("runtime.identity calls = %d, want 1", caller.calls)
	}
}

func TestLocalContextRegistryLockContentionFailsClosed(t *testing.T) {
	registry := newLocalContextRegistry(t.TempDir())
	if err := os.MkdirAll(registry.dir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(registry.lockPath(), []byte("held\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := registry.WriteDescriptor(testLocalContextDescriptor("local", "runtime-a"))
	if err == nil || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("WriteDescriptor() error = %v, want lock failure", err)
	}
	if _, statErr := os.Stat(filepath.Join(registry.dir(), "local.json")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("descriptor stat error = %v, want not exist", statErr)
	}
}

func TestLocalContextDescriptorRejectsInlineTokenAndCorruptJSON(t *testing.T) {
	registry := newLocalContextRegistry(t.TempDir())
	if err := os.MkdirAll(registry.dir(), 0o700); err != nil {
		t.Fatal(err)
	}
	inlineToken := `{"version":1,"name":"inline","runtime_instance_id":"runtime-a","transport":"tcp","api_server":"http://127.0.0.1:8081","auth":{"mode":"token_file","token":"secret"},"created_at":"2026-07-02T00:00:00Z","updated_at":"2026-07-02T00:00:00Z"}`
	if err := os.WriteFile(filepath.Join(registry.dir(), "inline.json"), []byte(inlineToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(registry.dir(), "bad.json"), []byte(`{"version":`), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := registry.Inspect(context.Background(), &fakeRuntimeIdentityCaller{})
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	statuses := map[string]string{}
	for _, entry := range report.Entries {
		statuses[entry.Descriptor.Name] = entry.Status
	}
	if statuses["inline"] != localContextStatusCorruptDescriptor {
		t.Fatalf("inline status = %q, want corrupt_descriptor", statuses["inline"])
	}
	if statuses["bad"] != localContextStatusCorruptDescriptor {
		t.Fatalf("bad status = %q, want corrupt_descriptor", statuses["bad"])
	}
}

func TestLocalContextValidationTaxonomy(t *testing.T) {
	tests := []struct {
		name       string
		desc       localContextDescriptor
		caller     *fakeRuntimeIdentityCaller
		wantStatus string
		wantDetail string
		wantCalls  int
	}{
		{
			name:       "identity mismatch",
			desc:       testLocalContextDescriptor("mismatch", "runtime-a"),
			caller:     &fakeRuntimeIdentityCaller{result: apiv1.RuntimeIdentityResult{RuntimeInstanceID: "runtime-b"}},
			wantStatus: localContextStatusIdentityMismatch,
			wantCalls:  1,
		},
		{
			name:       "auth failure",
			desc:       testLocalContextDescriptor("auth", "runtime-a"),
			caller:     &fakeRuntimeIdentityCaller{err: &cliAPIHTTPError{statusCode: 401, message: "unauthorized"}},
			wantStatus: localContextStatusAuthFailure,
			wantCalls:  1,
		},
		{
			name:       "no server",
			desc:       testLocalContextDescriptor("down", "runtime-a"),
			caller:     &fakeRuntimeIdentityCaller{err: &cliAPITransportError{surface: "runtime API", endpoint: "http://127.0.0.1:19001/v1/rpc", operation: "request", err: errors.New("connection refused")}},
			wantStatus: localContextStatusNoServer,
			wantDetail: "cannot reach the Swarm runtime at 127.0.0.1:19001.",
			wantCalls:  1,
		},
		{
			name: "unsupported unix",
			desc: func() localContextDescriptor {
				desc := testLocalContextDescriptor("unix", "runtime-a")
				desc.Transport = localContextTransportUnix
				desc.APIServer = ""
				desc.SocketPath = "/tmp/swarm.sock"
				desc.Auth = localContextDescriptorAuth{Mode: localContextAuthTokenFile, TokenFile: "/tmp/token"}
				return desc
			}(),
			caller:     &fakeRuntimeIdentityCaller{},
			wantStatus: localContextStatusUnsupportedTransport,
			wantCalls:  0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := localContextEntry{Descriptor: tt.desc, Status: localContextStatusOK}
			got := validateLocalContextEntry(context.Background(), entry, tt.caller)
			if got.Status != tt.wantStatus {
				t.Fatalf("status = %q, want %q detail=%s", got.Status, tt.wantStatus, got.Detail)
			}
			if tt.wantDetail != "" && !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("detail = %q, want substring %q", got.Detail, tt.wantDetail)
			}
			for _, forbidden := range []string{"v1 RPC", "/v1/rpc", "Post ", "dial tcp", "connection refused"} {
				if strings.Contains(got.Detail, forbidden) {
					t.Fatalf("detail = %q, must not leak %q", got.Detail, forbidden)
				}
			}
			if tt.caller.calls != tt.wantCalls {
				t.Fatalf("calls = %d, want %d", tt.caller.calls, tt.wantCalls)
			}
		})
	}
}

func TestLocalContextPruneRemovesOnlyPruneableStatuses(t *testing.T) {
	registry := newLocalContextRegistry(t.TempDir())
	for _, desc := range []localContextDescriptor{
		testLocalContextDescriptor("mismatch", "runtime-a"),
		testLocalContextDescriptor("auth", "runtime-a"),
	} {
		if err := registry.WriteDescriptor(desc); err != nil {
			t.Fatalf("WriteDescriptor(%s) error = %v", desc.Name, err)
		}
	}
	if err := registry.SetCurrent("mismatch"); err != nil {
		t.Fatal(err)
	}
	caller := &switchingRuntimeIdentityCaller{
		results: map[string]apiv1.RuntimeIdentityResult{
			"mismatch": {RuntimeInstanceID: "runtime-b"},
		},
		errs: map[string]error{
			"auth": &cliAPIHTTPError{statusCode: 401, message: "unauthorized"},
		},
	}
	result, err := registry.Prune(context.Background(), caller)
	if err != nil {
		t.Fatalf("Prune() error = %v", err)
	}
	if len(result.Removed) != 1 || result.Removed[0].Descriptor.Name != "mismatch" {
		t.Fatalf("removed = %#v, want only mismatch", result.Removed)
	}
	if len(result.Kept) != 1 || result.Kept[0].Descriptor.Name != "auth" {
		t.Fatalf("kept = %#v, want only auth", result.Kept)
	}
	if current, err := registry.CurrentName(); err != nil || current != "" {
		t.Fatalf("current = %q err=%v, want cleared", current, err)
	}
}

func TestLocalContextPruneClearsDanglingCurrentSelector(t *testing.T) {
	registry := newLocalContextRegistry(t.TempDir())
	if err := registry.SetCurrent("missing"); err != nil {
		t.Fatal(err)
	}
	result, err := registry.Prune(context.Background(), &fakeRuntimeIdentityCaller{})
	if err != nil {
		t.Fatalf("Prune() error = %v", err)
	}
	if len(result.Removed) != 1 || result.Removed[0].Descriptor.Name != "missing" {
		t.Fatalf("removed = %#v, want dangling current", result.Removed)
	}
	if result.Removed[0].Status != localContextStatusCorruptDescriptor {
		t.Fatalf("removed status = %q, want corrupt_descriptor", result.Removed[0].Status)
	}
	if current, err := registry.CurrentName(); err != nil || current != "" {
		t.Fatalf("current = %q err=%v, want cleared", current, err)
	}
}

func TestLocalContextPermissionClassifier(t *testing.T) {
	if got := classifyLocalContextTokenError(os.ErrPermission); got != localContextStatusPermissionDenied {
		t.Fatalf("permission token error classified as %q", got)
	}
}

type switchingRuntimeIdentityCaller struct {
	results map[string]apiv1.RuntimeIdentityResult
	errs    map[string]error
}

func (c *switchingRuntimeIdentityCaller) callRuntimeIdentity(_ context.Context, rpcEndpoint, _ string) (apiv1.RuntimeIdentityResult, error) {
	for name, err := range c.errs {
		if strings.Contains(rpcEndpoint, name) {
			return apiv1.RuntimeIdentityResult{}, err
		}
	}
	for name, result := range c.results {
		if strings.Contains(rpcEndpoint, name) {
			return result, nil
		}
	}
	return apiv1.RuntimeIdentityResult{RuntimeInstanceID: "runtime-a"}, nil
}

func testLocalContextDescriptor(name, runtimeID string) localContextDescriptor {
	now := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	return localContextDescriptor{
		Version:           localContextDescriptorVersion,
		Name:              name,
		RuntimeInstanceID: runtimeID,
		Transport:         localContextTransportTCP,
		APIServer:         "http://127.0.0.1:8081/" + name,
		Auth:              localContextDescriptorAuth{Mode: localContextAuthBuiltinLoopback},
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}
