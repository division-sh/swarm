package apiv1

import (
	"testing"
	"time"

	"swarm/internal/runtime/bundledelete"
	"swarm/internal/store"
)

func TestOperatorBundleDeleteForceUsesOwnerChainAndIdempotency(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	executor := &recordingBundleDeleteExecutor{bundleHash: runStartTestBundleHash}
	idempotency := newRecordingAPIIdempotencyStore()
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:          func() time.Time { return now },
			Ready:        func() bool { return true },
			Database:     fakePinger{},
			Idempotency:  idempotency,
			BundleDelete: executor,
		}),
	})

	body := `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"` + runStartTestBundleHash + `","force":true,"idempotency_key":"force-1"}}`
	resp := rpcCall(t, handler, body)
	if resp.Error != nil {
		t.Fatalf("bundle.delete force error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["ok"] != true || result["status"] != "completed" || result["bundle_hash"] != runStartTestBundleHash || result["force"] != true || result["deleted"] != true {
		t.Fatalf("bundle.delete force result = %#v", result)
	}
	if len(executor.calls) != 1 {
		t.Fatalf("bundle.delete executor calls = %d, want 1", len(executor.calls))
	}
	if executor.calls[0].BundleHash != runStartTestBundleHash || !executor.calls[0].Force || executor.calls[0].DryRun {
		t.Fatalf("bundle.delete request = %#v", executor.calls[0])
	}

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("bundle.delete replay error = %#v", replay.Error)
	}
	if len(executor.calls) != 1 {
		t.Fatalf("bundle.delete executor calls after replay = %d, want unchanged 1", len(executor.calls))
	}

	conflict := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","force":true,"dry_run":true,"idempotency_key":"force-1"}}`)
	if conflict.Error == nil {
		t.Fatal("bundle.delete idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("bundle.delete conflict data = %#v, want %s", data, IdempotencyConflictCode)
	}
}

func TestOperatorBundleDeleteForceErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code string
	}{
		{name: "missing bundle", err: store.ErrBundleNotFound, code: BundleNotFoundCode},
		{name: "busy", err: bundledelete.ErrOperationInProgress, code: BundleDeleteInProgressCode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &recordingBundleDeleteExecutor{bundleHash: runStartTestBundleHash, err: tt.err}
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					Ready:        func() bool { return true },
					Database:     fakePinger{},
					Idempotency:  newRecordingAPIIdempotencyStore(),
					BundleDelete: executor,
				}),
			})

			resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","force":true,"idempotency_key":"force-error"}}`)
			if resp.Error == nil {
				t.Fatalf("bundle.delete %s error = nil", tt.name)
			}
			if data := asMap(t, resp.Error.Data); data["code"] != tt.code {
				t.Fatalf("bundle.delete %s data = %#v, want %s", tt.name, data, tt.code)
			}
			if len(executor.calls) != 1 {
				t.Fatalf("bundle.delete executor calls = %d, want 1", len(executor.calls))
			}
		})
	}
}

func TestOperatorBundleDeleteNonForceFailsClosedBeforeOwner(t *testing.T) {
	executor := &recordingBundleDeleteExecutor{bundleHash: runStartTestBundleHash}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Ready:        func() bool { return true },
			Database:     fakePinger{},
			Idempotency:  newRecordingAPIIdempotencyStore(),
			BundleDelete: executor,
		}),
	})

	for _, body := range []string{
		`{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"` + runStartTestBundleHash + `"}}`,
		`{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"` + runStartTestBundleHash + `","force":false}}`,
	} {
		resp := rpcCall(t, handler, body)
		if resp.Error == nil {
			t.Fatal("bundle.delete non-force error = nil")
		}
		if resp.Error.Code != codeInvalidParams {
			t.Fatalf("bundle.delete non-force code = %d, want %d", resp.Error.Code, codeInvalidParams)
		}
	}
	if len(executor.calls) != 0 {
		t.Fatalf("bundle.delete owner calls after non-force = %d, want 0", len(executor.calls))
	}
}
