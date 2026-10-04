package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
)

func TestLinuxWorkerIdentityRequiresExactCurrentArtifactContract(t *testing.T) {
	current := Identity{ABI: ABI, OS: "darwin", Arch: "arm64", BinaryDigest: "sha256:host", Version: "0.7", Revision: "clean-revision", Interpreter: pythonmodule.RuntimeIdentity()}
	linux := current
	linux.OS, linux.Arch, linux.BinaryDigest = "linux", "amd64", "sha256:"+strings.Repeat("a", 64)
	if err := ValidateLinuxArtifact(current, linux); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		alter func(*Identity)
	}{
		{"darwin_bytes", func(v *Identity) { v.OS = "darwin" }},
		{"wrong_abi", func(v *Identity) { v.ABI += "-foreign" }},
		{"wrong_version", func(v *Identity) { v.Version = "0.6" }},
		{"wrong_revision", func(v *Identity) { v.Revision = "foreign" }},
		{"missing_digest", func(v *Identity) { v.BinaryDigest = "" }},
		{"dirty_linux", func(v *Identity) { v.Modified = true }},
		{"unknown_arch", func(v *Identity) { v.Arch = "" }},
		{"wrong_interpreter", func(v *Identity) { v.Interpreter = pythonmodule.Identity{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := linux
			test.alter(&candidate)
			if ValidateLinuxArtifact(current, candidate) == nil {
				t.Fatal("foreign worker admitted")
			}
		})
	}
	current.Modified = true
	if ValidateLinuxArtifact(current, linux) == nil {
		t.Fatal("dirty host accepted a cross-platform worker")
	}
}

func TestWorkerIdentityMismatchAndInvalidBoundsNeverStartModel(t *testing.T) {
	identity, err := ExecutableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		request Request
		code    string
	}{
		{"foreign_identity", Request{Mode: "model", Expected: Identity{}}, "workspace_worker_incompatible"},
		{"missing_module", Request{Mode: "model", Expected: identity}, "mock_worker_bounds_invalid"},
		{"invalid_mode", Request{Mode: "foreign", Expected: identity}, "workspace_worker_mode_invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, _ := json.Marshal(test.request)
			var output bytes.Buffer
			if exit := Run(context.Background(), bytes.NewReader(raw), &output); exit != 0 {
				t.Fatalf("exit %d: %s", exit, output.String())
			}
			var result Result
			if json.Unmarshal(output.Bytes(), &result) != nil || result.Identity != identity || result.ModelStarted || result.Failure == nil || result.Failure.Detail.Code != test.code {
				t.Fatalf("model refusal = %+v", result)
			}
		})
	}
}

func TestWorkerProtocolRejectsAmbiguousAndUnboundedInput(t *testing.T) {
	for _, raw := range []string{
		`null`, `[]`, `{`, `{"mode":"identity"} {}`,
		`{"mode":"identity","mode":"model"}`,
		`{"mode":"identity","unexpected":true}`,
		`{"mode":"model","module":{"unexpected":true}}`,
		`{"mode":"identity","gateway":{"headers":{"Authorization":"one","Authorization":"two"}}}`,
		strings.Repeat(" ", MaxBytes+1),
	} {
		var output bytes.Buffer
		exit := Run(context.Background(), strings.NewReader(raw), &output)
		if exit == 0 {
			var result Result
			if json.Unmarshal(output.Bytes(), &result) != nil || result.ModelStarted || result.Failure == nil {
				t.Fatalf("invalid envelope accepted: %.120s -> %s", raw, output.String())
			}
		} else if output.Len() != 0 {
			t.Fatalf("protocol error produced uncontrolled output: %.120s", raw)
		}
	}
}

func TestWorkerResultRequiresClosedUnambiguousIdentity(t *testing.T) {
	identity, err := ExecutableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(Result{Identity: identity})
	if result, err := DecodeResult(raw); err != nil || result.Identity != identity {
		t.Fatalf("valid result = %+v: %v", result, err)
	}
	for _, raw := range []string{`{}`, `null`, `{"identity":{"abi":"x"},"foreign":true}`, `{"identity":{"abi":"x","abi":"y"}}`, `{"identity":{"abi":"x"}} {}`} {
		if _, err := DecodeResult([]byte(raw)); err == nil {
			t.Fatalf("ambiguous result accepted: %s", raw)
		}
	}
}

func TestWorkerCanceledCallPreservesUncertainEnvelope(t *testing.T) {
	entered, retired := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
		close(retired)
	}))
	defer server.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	identity, err := ExecutableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(Request{
		Mode: "call", Expected: identity, Tool: "commit_once", Occurrence: "one-call", Arguments: json.RawMessage(`{}`),
		Gateway: toolgateway.HTTPObservation{URL: server.URL, Headers: map[string]string{"Authorization": "Bearer native-cancel-proof", "X-SWARM-Context-Token": "one-turn"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- Run(ctx, bytes.NewReader(request), &output) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not dispatch the held call")
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("native worker exited %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("native worker did not join after cancellation")
	}
	select {
	case <-retired:
	case <-time.After(5 * time.Second):
		t.Fatal("native worker left the held HTTP request alive")
	}
	result, err := DecodeResult(output.Bytes())
	if err != nil || result.Failure == nil || result.Failure.Class != failures.ClassOutcomeUncertain || result.Failure.Detail.Code != "workspace_tool_outcome_uncertain" || calls.Load() != 1 {
		t.Fatalf("canceled call lost possible-commit evidence: %+v calls=%d err=%v", result, calls.Load(), err)
	}
}
