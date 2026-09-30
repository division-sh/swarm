package apiv1

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

type dataShowReadCall struct {
	method string
	args   []any
}

type dataShowTraceStore struct {
	DurableDataStore
	calls    []dataShowReadCall
	failure  error
	versions []durabledata.VersionSummary
	history  []durabledata.HeadHistory
}

func (s *dataShowTraceStore) record(method string, args ...any) error {
	s.calls = append(s.calls, dataShowReadCall{method, args})
	return s.failure
}

func (s *dataShowTraceStore) ListDataDeclarationSummaries(ctx context.Context, bundle string) ([]durabledata.DeclarationSummary, error) {
	if err := s.record("declarations", bundle); err != nil {
		return nil, err
	}
	return s.DurableDataStore.ListDataDeclarationSummaries(ctx, bundle)
}

func (s *dataShowTraceStore) GetDeclarationImportShape(ctx context.Context, bundle string, ref durabledata.DeclarationRef) (durabledata.ImportShape, error) {
	if err := s.record("shape", bundle, ref); err != nil {
		return durabledata.ImportShape{}, err
	}
	return s.DurableDataStore.GetDeclarationImportShape(ctx, bundle, ref)
}

func (s *dataShowTraceStore) ListDataVersionSummaries(ctx context.Context, ref durabledata.DeclarationRef, after uint64, limit int) ([]durabledata.VersionSummary, error) {
	if err := s.record("versions", ref, after, limit); err != nil {
		return nil, err
	}
	if s.versions != nil {
		return s.versions, nil
	}
	return s.DurableDataStore.ListDataVersionSummaries(ctx, ref, after, limit)
}

func (s *dataShowTraceStore) ListDataHeadHistory(ctx context.Context, ref durabledata.DeclarationRef, after uint64, limit int) ([]durabledata.HeadHistory, error) {
	if err := s.record("history", ref, after, limit); err != nil {
		return nil, err
	}
	if s.history != nil {
		return s.history, nil
	}
	return s.DurableDataStore.ListDataHeadHistory(ctx, ref, after, limit)
}

func (s *dataShowTraceStore) ResolveDataVersionSummary(ctx context.Context, ref durabledata.DeclarationRef, selector durabledata.VersionSelector) (durabledata.VersionSummary, error) {
	if err := s.record("summary", ref, selector); err != nil {
		return durabledata.VersionSummary{}, err
	}
	return s.DurableDataStore.ResolveDataVersionSummary(ctx, ref, selector)
}

func (s *dataShowTraceStore) ResolveDataVersionPayload(ctx context.Context, ref durabledata.DeclarationRef, selector durabledata.VersionSelector) (durabledata.VersionSummary, durabledata.Version, error) {
	if err := s.record("payload", ref, selector); err != nil {
		return durabledata.VersionSummary{}, durabledata.Version{}, err
	}
	return s.DurableDataStore.ResolveDataVersionPayload(ctx, ref, selector)
}

func (s *dataShowTraceStore) ListDataVersionProvenance(ctx context.Context, version durabledata.VersionID, after uint64, limit int) ([]durabledata.Provenance, error) {
	if err := s.record("provenance", version, after, limit); err != nil {
		return nil, err
	}
	return s.DurableDataStore.ListDataVersionProvenance(ctx, version, after, limit)
}

func (s *dataShowTraceStore) ListDataPins(ctx context.Context, version durabledata.VersionID, after string, limit int) ([]durabledata.Pin, error) {
	if err := s.record("pins", version, after, limit); err != nil {
		return nil, err
	}
	return s.DurableDataStore.ListDataPins(ctx, version, after, limit)
}

func (s *dataShowTraceStore) LoadDataSourceOperation(ctx context.Context, id string) (durabledata.SourceOperationRecord, error) {
	if err := s.record("source", id); err != nil {
		return durabledata.SourceOperationRecord{}, err
	}
	return s.DurableDataStore.LoadDataSourceOperation(ctx, id)
}

func (s *dataShowTraceStore) LoadDataPruneOperation(ctx context.Context, id string) (durabledata.PruneOperationResult, error) {
	if err := s.record("prune", id); err != nil {
		return durabledata.PruneOperationResult{}, err
	}
	return s.DurableDataStore.LoadDataPruneOperation(ctx, id)
}

func (s *dataShowTraceStore) LoadDataPruneOperationPins(ctx context.Context, id string) ([]durabledata.Pin, error) {
	if err := s.record("prune_pins", id); err != nil {
		return nil, err
	}
	return s.DurableDataStore.LoadDataPruneOperationPins(ctx, id)
}

func (s *dataShowTraceStore) LoadDataRunCreationOperation(ctx context.Context, id string) (durabledata.RunCreationOperationRecord, error) {
	if err := s.record("run_creation", id); err != nil {
		return durabledata.RunCreationOperationRecord{}, err
	}
	return s.DurableDataStore.LoadDataRunCreationOperation(ctx, id)
}

func dataShowResourceParams(view string) map[string]any {
	params := map[string]any{"view": view, "declaration": dataProbeDeclarationParams()}
	if view != "versions" && view != "head_history" {
		params["selector"] = map[string]any{"kind": "head"}
	}
	if view != "version" && view != "row" {
		params["page"] = map[string]any{"limit": 1}
	}
	if view == "row" {
		params["row_selector"] = map[string]any{"key": "alpha"}
	}
	return params
}

func dataShowOperationParams(kind, id, detail string) map[string]any {
	field := map[string]string{"source": "source_invocation_id", "prune": "prune_invocation_id", "run_creation": "run_id"}[kind]
	params := map[string]any{"view": "operation", "detail": detail, "operation_ref": map[string]any{"kind": kind, field: id}}
	if detail != "summary" && detail != "request_binding" {
		params["page"] = map[string]any{"limit": 1}
	}
	return params
}

func TestDataShowValidationPrecedenceAndStoreTrace(t *testing.T) {
	invalid := func(field, reason string) error {
		return NewInvalidParamsError(map[string]any{"field": field, "reason": reason})
	}
	missing := durabledata.NewDomainError(durabledata.CodeDeclarationMissing, "trace missing")
	for _, view := range []string{"versions", "head_history", "version", "provenance", "pins", "rows", "row", "export_chunk"} {
		t.Run(view, func(t *testing.T) {
			base := newDataRuntimeProbeStore(t)
			params := dataShowResourceParams(view)
			if view == "row" {
				params["row_selector"] = nil
			} else if view != "version" {
				params["page"] = nil
			}
			trace := &dataShowTraceStore{DurableDataStore: base, failure: missing}
			_, err := OperatorDataHandlers(DataHandlerOptions{Store: trace})["data.show"](context.Background(), Request{Params: params})
			var want error = dataApplicationError(missing)
			var calls []dataShowReadCall
			if view == "versions" || view == "head_history" {
				want = invalid("page", "must be an object")
			} else {
				method := "summary"
				if view == "rows" || view == "row" || view == "export_chunk" {
					method = "payload"
				}
				calls = []dataShowReadCall{{method, []any{base.declaration.Ref, durabledata.VersionSelector{Kind: "head"}}}}
			}
			if !reflect.DeepEqual(err, want) || !reflect.DeepEqual(trace.calls, calls) {
				t.Fatalf("error/trace = %#v/%#v, want %#v/%#v", err, trace.calls, want, calls)
			}
			if view != "version" {
				trace.failure, trace.calls = nil, nil
				_, err = OperatorDataHandlers(DataHandlerOptions{Store: trace})["data.show"](context.Background(), Request{Params: params})
				field := "page"
				if view == "row" {
					field = "row_selector"
				}
				if !reflect.DeepEqual(err, invalid(field, "must be an object")) || !reflect.DeepEqual(trace.calls, calls) {
					t.Fatalf("selected then malformed = %#v/%#v", err, trace.calls)
				}
			}
		})
	}
	for _, kind := range []string{"source", "prune", "run_creation"} {
		for _, detail := range []string{"summary", "defects", "request_binding", "unknown"} {
			t.Run(kind+"/"+detail, func(t *testing.T) {
				base := newDataRuntimeProbeStore(t)
				trace := &dataShowTraceStore{DurableDataStore: base, failure: missing}
				params := dataShowOperationParams(kind, dataProbeSourceInvocationID, detail)
				if detail != "summary" && detail != "request_binding" {
					params["page"] = nil
				}
				_, err := OperatorDataHandlers(DataHandlerOptions{Store: trace})["data.show"](context.Background(), Request{Params: params})
				want := dataApplicationError(missing)
				calls := []dataShowReadCall{{kind, []any{dataProbeSourceInvocationID}}}
				if detail == "request_binding" && kind != "run_creation" {
					want = invalid("detail", "incompatible operation detail")
					calls = nil
				}
				if !reflect.DeepEqual(err, want) || !reflect.DeepEqual(trace.calls, calls) {
					t.Fatalf("receipt precedence = %#v/%#v, want %#v/%#v", err, trace.calls, want, calls)
				}
			})
		}
	}
	for _, view := range []string{"declarations", "import_shape"} {
		t.Run(view, func(t *testing.T) {
			trace := &dataShowTraceStore{DurableDataStore: newDataRuntimeProbeStore(t), failure: missing}
			params := map[string]any{"view": view, "bundle_hash": nil}
			if view == "declarations" {
				params["page"] = nil
			} else {
				params["declaration"], params["schema_digest"] = nil, nil
			}
			_, err := OperatorDataHandlers(DataHandlerOptions{Store: trace})["data.show"](context.Background(), Request{Params: params})
			if !reflect.DeepEqual(err, invalid("bundle_hash", "must be a non-empty string without surrounding whitespace")) || len(trace.calls) != 0 {
				t.Fatalf("inventory precedence = %#v/%#v", err, trace.calls)
			}
		})
	}
	for _, view := range []string{"rows", "row", "export_chunk"} {
		for _, state := range []string{"pruned", "corrupt"} {
			t.Run(view+"/"+state+"_before_presentation", func(t *testing.T) {
				base := newDataRuntimeProbeStore(t)
				params := dataShowResourceParams(view)
				if view == "row" {
					params["row_selector"] = nil
				} else {
					params["page"] = nil
				}
				code := durabledata.CodeIntegrity
				if state == "pruned" {
					base.version.PrunedAt = &base.now
					base.version.CanonicalJSONL = nil
					code = durabledata.CodePayloadPruned
				} else {
					base.version.CanonicalJSONL = []byte("not JSONL")
				}
				trace := &dataShowTraceStore{DurableDataStore: base}
				_, err := OperatorDataHandlers(DataHandlerOptions{Store: trace})["data.show"](context.Background(), Request{Params: params})
				want := NewApplicationError(string(code), false, map[string]any{"version_id": base.version.VersionID})
				calls := []dataShowReadCall{{"payload", []any{base.declaration.Ref, durabledata.VersionSelector{Kind: "head"}}}}
				if !reflect.DeepEqual(err, want) || !reflect.DeepEqual(trace.calls, calls) {
					t.Fatalf("payload precedence = %#v/%#v, want %#v/%#v", err, trace.calls, want, calls)
				}
			})
		}
	}
}

func TestDataShowLiveCursorWireAndQueryBinding(t *testing.T) {
	// Captured through the live handler before factoring, not recomputed by a codec.
	wantHash := map[string]string{
		"declarations": "eca364b4b7cd75bc0335d9902449a629eecf07cb7cd50472e987260e2275408f",
		"versions":     "75e965adf6b23ae35dbea8d6ceafa99496aa9897265e87e6e127aa4ba04a001a",
		"head_history": "2419e0989b3c81a6983e485d3f7cc11c84a210f7c7d70a958fc7375564a85995",
		"provenance":   "5ade49e7ada879af81822f818a3f93322b69633a3dc127e4621e0f8c9c71bfd4",
		"pins":         "1b0d58b093bd14cf55ec76f88038ea8682fa7bdf5af528057e1ab941efa111c3",
		"rows":         "829121b22499ad660cefb35b88dc5245e9ad12eb86695cfe0d640a789b8ba9bd",
		"export_chunk": "dfb11c618756d5f2bf0d20a57bf6b4179ccbb6d4d8ec17e1771b5efa7525a20f",
	}
	base := newDataRuntimeProbeStore(t)
	base.version.CanonicalJSONL = []byte("{\"slug\":\"alpha\"}\n{\"slug\":\"beta\"}\n")
	compiled, defects := durabledata.CompileJSONL(base.declaration.Ref, mustDataSchema(base.version.CanonicalSchema), "slug", base.version.CanonicalJSONL)
	if len(defects) != 0 {
		t.Fatal(defects)
	}
	base.version = durabledata.Version{VersionID: compiled.VersionID, SequenceAlias: 1, Manifest: compiled.Manifest, BusinessKey: "slug", CanonicalSchema: compiled.CanonicalSchema, CanonicalJSONL: compiled.CanonicalJSONL}
	base.declaration.SchemaDigest = compiled.Manifest.SchemaDigest
	base.summaries, _ = base.ListDataDeclarationSummaries(context.Background(), dataProbeBundleHash)
	base.summaries = append(base.summaries, base.summaries[0])
	for i := 1; i <= 2; i++ {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		ref, err := durabledata.NewProvenanceRef("import", id)
		if err != nil {
			t.Fatal(err)
		}
		base.version.Provenance = append(base.version.Provenance, durabledata.Provenance{Sequence: uint64(i), VersionID: compiled.VersionID, ProducerRef: ref, Actor: "operator", CommittedAt: base.now})
		base.pins = append(base.pins, durabledata.Pin{RunID: id, RunState: "running", Declaration: base.declaration.Ref, SchemaDigest: compiled.Manifest.SchemaDigest, VersionID: compiled.VersionID, Selection: "explicit"})
	}
	second := base.version.Summary()
	second.Alias = "v2"
	trace := &dataShowTraceStore{DurableDataStore: base, versions: []durabledata.VersionSummary{base.version.Summary(), second}, history: []durabledata.HeadHistory{
		{Revision: 1, Before: durabledata.AbsentHead(), After: durabledata.VersionHead(compiled.VersionID), OperationID: dataProbeSourceInvocationID, CommittedAt: time.Unix(1_700_000_000, 0).UTC()},
		{Revision: 2, Before: durabledata.VersionHead(compiled.VersionID), After: durabledata.VersionHead(compiled.VersionID), OperationID: dataProbePruneInvocationID, CommittedAt: time.Unix(1_700_000_001, 0).UTC()},
	}}
	for _, view := range []string{"declarations", "versions", "head_history", "provenance", "pins", "rows", "export_chunk"} {
		t.Run(view, func(t *testing.T) {
			params := dataShowResourceParams(view)
			if view == "declarations" {
				params = map[string]any{"view": view, "bundle_hash": dataProbeBundleHash, "page": map[string]any{"limit": 1}}
			}
			result, err := OperatorDataHandlers(DataHandlerOptions{Store: trace})["data.show"](context.Background(), Request{Params: params})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Continuation durabledata.PageContinuation `json:"continuation"`
			}
			if err := json.Unmarshal(raw, &wire); err != nil {
				t.Fatal(err)
			}
			if wire.Continuation.State != "more" {
				t.Fatalf("no emitted cursor: %s", raw)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256([]byte(wire.Continuation.Cursor))); got != wantHash[view] {
				t.Fatalf("cursor bytes changed: hash %s, want %s", got, wantHash[view])
			}
			decoded, err := base64.RawURLEncoding.DecodeString(wire.Continuation.Cursor)
			if err != nil {
				t.Fatal(err)
			}
			var wrongChecksum, foreign []byte
			if view == "declarations" || view == "rows" || view == "export_chunk" {
				parts := strings.Split(string(decoded), ":")
				parts[2] = strings.Repeat("f", 64)
				wrongChecksum = []byte(strings.Join(parts, ":"))
				parts[1] = "foreign query"
				foreign = []byte(strings.Join(parts, ":"))
			} else {
				var envelope map[string]any
				if err := json.Unmarshal(decoded, &envelope); err != nil {
					t.Fatal(err)
				}
				envelope["checksum"] = strings.Repeat("f", 64)
				wrongChecksum, err = canonicaljson.Bytes(envelope)
				if err != nil {
					t.Fatal(err)
				}
				envelope["payload"].(map[string]any)["fingerprint"] = "foreign query"
				foreign, err = canonicaljson.Bytes(envelope)
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, cursor := range []string{"!", wire.Continuation.Cursor + "x", base64.RawURLEncoding.EncodeToString(append(decoded, '\n')), base64.RawURLEncoding.EncodeToString(wrongChecksum), base64.RawURLEncoding.EncodeToString(foreign), strings.Repeat("x", 4097)} {
				params["page"] = map[string]any{"limit": 1, "cursor": cursor}
				if _, err := OperatorDataHandlers(DataHandlerOptions{Store: trace})["data.show"](context.Background(), Request{Params: params}); err == nil {
					t.Fatal("malformed cursor accepted")
				}
			}
		})
	}
}
