package channelonboarding

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/lib/pq"
)

type signingDiagnosticCanary struct{}

func (signingDiagnosticCanary) Error() string { panic("CANARY_PROVIDER_CREDENTIAL") }

type signingDiagnosticCycle struct{}

func (c *signingDiagnosticCycle) Error() string { panic("CANARY_ERROR_TEXT") }
func (c *signingDiagnosticCycle) Unwrap() error { return c }

func TestSigningDiagnosticControls(t *testing.T) {
	t.Run("wrapped-joined-cause-and-secret-canaries", func(t *testing.T) {
		native := &pq.Error{Code: "40P01", Message: "CANARY_SQL_TEXT", Detail: "CANARY_DSN", Where: "CANARY_TOKEN"}
		original := errors.Join(fmt.Errorf("CANARY_PRIVATE_WRAPPER: %w", native), context.Canceled,
			credentials.ErrCredentialValueUnusable, operatorchannel.ErrRevisionConflict, signingDiagnosticCanary{})
		probe := NewSigningDiagnostic(nil)
		service := &Service{testSigningDiagnostic: probe}
		ctx, call := service.beginSigningDiagnostic(context.Background(), "start")
		SigningDiagnosticPhase(ctx, "readiness.native_qualification_read")
		signingDiagnosticError(ctx, "blocked_original", original)
		SigningDiagnosticPhase(ctx, "result.identity_read")
		signingDiagnosticError(ctx, "result_projection", ErrNotFound)
		call.finish(errors.Join(original, ErrNotFound))
		rows, complete := probe.Snapshot()
		if !complete || len(rows) != 3 || rows[0].Phase != "readiness.native_qualification_read" || rows[1].Phase != "result.identity_read" || rows[0].Call != rows[1].Call {
			t.Fatal("original and projection provenance was lost")
		}
		raw, err := json.Marshal(rows)
		if err != nil || strings.Contains(string(raw), "CANARY_") {
			t.Fatal("private diagnostic material was serialized")
		}
		for _, want := range []string{"40P01", "context_canceled", "credential_unusable", "identity_revision", "opaque_error_type"} {
			if !strings.Contains(string(raw), want) {
				t.Fatalf("safe classification missing: %s", want)
			}
		}
		if !errors.Is(original, context.Canceled) || !errors.Is(original, credentials.ErrCredentialValueUnusable) {
			t.Fatal("observing changed the cause chain")
		}
		var got *pq.Error
		if !errors.As(original, &got) || got != native {
			t.Fatal("observing changed native error identity")
		}
		failure := runtimefailures.FromError(original, "api", "channel.onboarding_start")
		if failure.Failure.Class != runtimefailures.ClassInternalFailure || failure.Failure.Detail.Code != "unclassified_runtime_error" {
			t.Fatal("canonical public failure changed")
		}
		wire, err := json.Marshal(failure.Failure)
		if err != nil || strings.Contains(string(wire), "CANARY_") || !probe.Dispose() {
			t.Fatal("canonical output or disposal failed")
		}
	})
	t.Run("entire-start-return-before-reservation", func(t *testing.T) {
		probe := NewSigningDiagnostic(nil)
		service := &Service{testSigningDiagnostic: probe}
		_, err := service.Start(context.Background(), StartInput{})
		if !errors.Is(err, ErrInvalidRequest) {
			t.Fatal("invalid start changed")
		}
		rows, complete := probe.Snapshot()
		if !complete || len(rows) != 1 || rows[0].Phase != "start.validate_input" || rows[0].Provenance != "return" || !probe.Dispose() {
			t.Fatal("pre-reservation Start return was not captured exactly once")
		}
	})
	t.Run("entire-retry-return-before-rebind", func(t *testing.T) {
		probe := NewSigningDiagnostic(nil)
		service := &Service{store: &cancellationTestStore{}, testSigningDiagnostic: probe}
		_, err := service.Retry(context.Background(), RetryInput{OperationID: "missing"})
		if !errors.Is(err, ErrNotFound) {
			t.Fatal("missing retry changed")
		}
		rows, complete := probe.Snapshot()
		if !complete || len(rows) != 1 || rows[0].Phase != "retry.load_operation" || !probe.Dispose() {
			t.Fatal("pre-rebind Retry return was not captured exactly once")
		}
	})
	t.Run("disabled-context-identity-and-no-work", func(t *testing.T) {
		ctx := context.Background()
		service := &Service{}
		got, call := service.beginSigningDiagnostic(ctx, "start")
		if got != ctx || call != nil {
			t.Fatal("disabled observer changed context or allocated a call")
		}
		SigningDiagnosticPhase(ctx, "readiness.native_qualification_read")
		signingDiagnosticError(ctx, "return", context.Canceled)
		call.finish(context.Canceled)
	})
	t.Run("disposal-and-capture-cardinality", func(t *testing.T) {
		emitted := 0
		probe := NewSigningDiagnostic(func(SigningDiagnosticRecord) { emitted++ })
		service := &Service{testSigningDiagnostic: probe}
		_, call := service.beginSigningDiagnostic(context.Background(), "start")
		call.finish(nil)
		if emitted != 1 || !probe.Dispose() {
			t.Fatal("return cardinality or disposal failed")
		}
		_, late := service.beginSigningDiagnostic(context.Background(), "start")
		_, complete := probe.Snapshot()
		if late != nil || emitted != 1 || complete {
			t.Fatal("disposed collector adopted late work")
		}
	})
	t.Run("bounded-traversal-and-buffer-are-incomplete", func(t *testing.T) {
		if _, truncated := signingDiagnosticCauses(&signingDiagnosticCycle{}); !truncated {
			t.Fatal("cyclic traversal was accepted")
		}
		wide := make([]error, signingDiagnosticCauseLimit+1)
		for i := range wide {
			wide[i] = context.Canceled
		}
		if _, truncated := signingDiagnosticCauses(errors.Join(wide...)); !truncated {
			t.Fatal("wide traversal was accepted")
		}
		probe := NewSigningDiagnostic(nil)
		service := &Service{testSigningDiagnostic: probe}
		_, call := service.beginSigningDiagnostic(context.Background(), "start")
		for range signingDiagnosticRecordLimit {
			call.record("return", context.Canceled)
		}
		call.finish(nil)
		rows, complete := probe.Snapshot()
		if complete || len(rows) != signingDiagnosticRecordLimit || probe.Dispose() {
			t.Fatal("buffer overflow was accepted")
		}
	})
	t.Run("unknown-phase-and-invalid-sqlstate-are-not-disclosed", func(t *testing.T) {
		probe := NewSigningDiagnostic(nil)
		service := &Service{testSigningDiagnostic: probe}
		ctx, call := service.beginSigningDiagnostic(context.Background(), "start")
		SigningDiagnosticPhase(ctx, "CANARY_PHASE")
		call.finish(&pq.Error{Code: "CANARY_SQLSTATE"})
		rows, complete := probe.Snapshot()
		raw, err := json.Marshal(rows)
		if complete || err != nil || strings.Contains(string(raw), "CANARY_") || probe.Dispose() {
			t.Fatal("unclassified private fields were disclosed or granted completeness")
		}
	})
}
