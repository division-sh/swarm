package apiv1

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const (
	dataProbeSourceInvocationID = "1c36924e-b33e-44e4-b228-73dfb4c9d52a"
	dataProbePruneInvocationID  = "e827bd86-4148-479c-8142-9e5048a647f4"
	dataProbeEventName          = "startup.loaded"
	dataProbeBundleHash         = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

type dataRuntimeProbeStore struct {
	declaration durabledata.Declaration
	version     durabledata.Version
	pruneResult durabledata.PruneOperationResult
	prunePins   []durabledata.Pin
	pins        []durabledata.Pin
	summaries   []durabledata.DeclarationSummary
	showCalls   int
	now         time.Time
}

type hostileDataRuntimeProbeStore struct {
	*dataRuntimeProbeStore
	mutateSource func(*durabledata.SourceOperationResult)
	mutatePrune  func(*durabledata.PruneOperationResult)
}

func (s *hostileDataRuntimeProbeStore) ExecuteDataSourceOperation(ctx context.Context, command durabledata.SourceCommand) (durabledata.SourceOperationResult, error) {
	result, err := s.dataRuntimeProbeStore.ExecuteDataSourceOperation(ctx, command)
	if err == nil && s.mutateSource != nil {
		s.mutateSource(&result)
	}
	return result, err
}

func (s *hostileDataRuntimeProbeStore) PruneDataResource(ctx context.Context, command durabledata.PruneCommand) (durabledata.PruneOperationResult, error) {
	result, err := s.dataRuntimeProbeStore.PruneDataResource(ctx, command)
	if err == nil && s.mutatePrune != nil {
		s.mutatePrune(&result)
	}
	return result, err
}

func newDataRuntimeProbeStore(t *testing.T) *dataRuntimeProbeStore {
	t.Helper()
	ref, err := durabledata.ParseDeclarationRef(".", dataProbeEventName)
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"slug": map[string]any{"type": "string"},
		},
		"required":             []any{"slug"},
		"additionalProperties": false,
	}
	compiled, defects := durabledata.CompileJSONL(ref, schema, "slug", []byte("{\"slug\":\"alpha\"}\n"))
	if len(defects) != 0 {
		t.Fatalf("compile data probe version: %#v", defects)
	}
	return &dataRuntimeProbeStore{
		declaration: durabledata.Declaration{
			Name: "startup.loaded", Ref: ref, OwnerFlowID: "", BusinessKey: "slug",
			SchemaDigest:    compiled.Manifest.SchemaDigest,
			CanonicalSchema: compiled.CanonicalSchema,
		},
		version: durabledata.Version{
			VersionID: compiled.VersionID, SequenceAlias: 1, Manifest: compiled.Manifest,
			BusinessKey: "slug", CanonicalSchema: compiled.CanonicalSchema, CanonicalJSONL: compiled.CanonicalJSONL,
		},
		now: time.Unix(1_700_000_000, 0).UTC(),
	}
}

func (s *dataRuntimeProbeStore) sourceResult(command durabledata.SourceCommand) durabledata.SourceOperationResult {
	head := durabledata.HeadResult{Before: durabledata.AbsentHead(), After: durabledata.AbsentHead()}
	candidateState := "candidate"
	alias := ""
	if command.Operation == "import" {
		head = durabledata.HeadResult{Before: durabledata.AbsentHead(), After: durabledata.VersionHead(s.version.VersionID), Changed: true, Revision: 1}
		candidateState = "version"
		alias = "v1"
	}
	manifest := s.version.Manifest
	return durabledata.SourceOperationResult{
		SourceInvocationID: command.SourceInvocationID,
		Operation:          command.Operation,
		Outcome:            "accepted",
		BundleHash:         dataProbeBundleHash,
		SchemaDigest:       s.declaration.SchemaDigest,
		Declaration:        s.declaration.Ref,
		ExpectedHead:       durabledata.AbsentHead(),
		ObservedHead:       durabledata.AbsentHead(),
		Candidate: durabledata.CandidateVersion{
			State: candidateState, VersionID: s.version.VersionID, Alias: alias, Manifest: &manifest,
		},
		Head:  head,
		Delta: durabledata.ComputedDelta(durabledata.AbsentHead(), durabledata.DeltaSummary{Added: 1}, durabledata.DeltaRowIdentityBusinessKey),
		Defects: durabledata.PageResult[durabledata.ValidationDefect]{
			Items: []durabledata.ValidationDefect{}, ItemCount: 0, EncodedItemsBytes: 2,
			Continuation: durabledata.EndContinuation(),
		},
		CompletedAt: s.now,
	}
}

func (s *dataRuntimeProbeStore) ExecuteDataSourceOperation(_ context.Context, command durabledata.SourceCommand) (durabledata.SourceOperationResult, error) {
	return s.sourceResult(command), nil
}

func (s *dataRuntimeProbeStore) PruneDataResource(_ context.Context, command durabledata.PruneCommand) (durabledata.PruneOperationResult, error) {
	return durabledata.PruneOperationResult{
		Outcome: "already_pruned", PruneInvocationID: command.PruneInvocationID,
		Declaration: command.Declaration, VersionID: command.VersionID,
		ExpectedHead: command.ExpectedHead, ObservedHead: durabledata.AbsentHead(),
		PayloadBefore: "pruned", PayloadAfter: "pruned", CompletedAt: s.now,
	}, nil
}

func (s *dataRuntimeProbeStore) ShowDataResource(_ context.Context, _ string, ref durabledata.DeclarationRef) (durabledata.ResourceSnapshot, error) {
	s.showCalls++
	return durabledata.ResourceSnapshot{
		Declaration: s.declaration,
		Head: durabledata.HeadResult{
			Before: durabledata.VersionHead(s.version.VersionID), After: durabledata.VersionHead(s.version.VersionID), Revision: 1,
		},
		Versions: []durabledata.Version{s.version},
	}, nil
}

func (s *dataRuntimeProbeStore) ListDataDeclarationSummaries(context.Context, string) ([]durabledata.DeclarationSummary, error) {
	if s.summaries != nil {
		return append([]durabledata.DeclarationSummary(nil), s.summaries...), nil
	}
	return []durabledata.DeclarationSummary{{
		Declaration: s.declaration.Ref, LocalName: s.declaration.Name, SchemaDigest: s.declaration.SchemaDigest,
		Head: durabledata.VersionHead(s.version.VersionID), VersionCount: 1, MaterializedVersionCount: 1,
		MaterializedBytes: len(s.version.CanonicalJSONL),
	}}, nil
}

func (s *dataRuntimeProbeStore) LoadDataSourceOperation(_ context.Context, id string) (durabledata.SourceOperationRecord, error) {
	return durabledata.SourceOperationRecord{
		Result: s.sourceResult(durabledata.SourceCommand{SourceInvocationID: id, Operation: "check"}),
		Evidence: durabledata.SourceEvidence{
			DeltaAdded: []durabledata.DeltaKey{{Key: durabledata.BusinessKey(`"alpha"`)}},
		},
	}, nil
}

func (s *dataRuntimeProbeStore) LoadDataPruneOperation(_ context.Context, id string) (durabledata.PruneOperationResult, error) {
	if s.pruneResult.PruneInvocationID != "" {
		return s.pruneResult, nil
	}
	return s.PruneDataResource(context.Background(), durabledata.PruneCommand{
		PruneInvocationID: id, Declaration: s.declaration.Ref, VersionID: s.version.VersionID, ExpectedHead: durabledata.AbsentHead(),
	})
}

func (s *dataRuntimeProbeStore) LoadDataPruneOperationPins(context.Context, string) ([]durabledata.Pin, error) {
	return append([]durabledata.Pin(nil), s.prunePins...), nil
}

func (s *dataRuntimeProbeStore) LoadDataPins(context.Context, durabledata.VersionID) ([]durabledata.Pin, error) {
	pins := append([]durabledata.Pin(nil), s.pins...)
	durabledata.SortPins(pins)
	return pins, nil
}

func (s *dataRuntimeProbeStore) LoadDataHeadHistory(context.Context, durabledata.DeclarationRef) ([]durabledata.HeadHistory, error) {
	return []durabledata.HeadHistory{}, nil
}

func (s *dataRuntimeProbeStore) LoadDataRunCreationOperation(context.Context, string) (durabledata.RunCreationOperationRecord, error) {
	return durabledata.RunCreationOperationRecord{}, nil
}

func approvedPermanentOperationHTTPRuntimeMethods() []string {
	return []string{"data.check", "data.import", "data.prune", "data.show"}
}

func successfulDataRuntimeResult(t *testing.T, methodName string) any {
	t.Helper()
	store := newDataRuntimeProbeStore(t)
	handlers := OperatorDataHandlers(DataHandlerOptions{Store: store})
	handler, ok := handlers[methodName]
	if !ok {
		t.Fatalf("OperatorDataHandlers missing %s", methodName)
	}
	params := dataProbeParams(methodName, store)
	result, err := handler(context.Background(), Request{Method: methodName, Params: params, ActorTokenID: actorTokenID(testToken)})
	if err != nil {
		t.Fatalf("%s successful data runtime result: %v", methodName, err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("%s encode successful data runtime result: %v", methodName, err)
	}
	var wire any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatalf("%s decode successful data runtime result: %v", methodName, err)
	}
	return wire
}

func dataProbeParams(methodName string, store *dataRuntimeProbeStore) map[string]any {
	params := map[string]any{}
	switch methodName {
	case "data.check", "data.import":
		params = map[string]any{
			"source_invocation_id": dataProbeSourceInvocationID,
			"bundle_hash":          dataProbeBundleHash,
			"declaration":          dataProbeDeclarationParams(),
			"expected_head":        map[string]any{"state": "absent"},
			"input": map[string]any{
				"format": "jsonl", "content_base64": base64.StdEncoding.EncodeToString([]byte("{\"slug\":\"alpha\"}\n")),
			},
		}
	case "data.prune":
		params = map[string]any{
			"prune_invocation_id": dataProbePruneInvocationID,
			"declaration":         dataProbeDeclarationParams(),
			"version_id":          string(store.version.VersionID),
			"expected_head":       map[string]any{"state": "absent"},
		}
	case "data.show":
		params = map[string]any{"view": "declarations", "bundle_hash": dataProbeBundleHash, "page": map[string]any{}}
	default:
		panic("unknown permanent operation method " + methodName)
	}
	return params
}

func dataProbeDeclarationParams() map[string]any {
	return map[string]any{"package_key": ".", "event": dataProbeEventName}
}

func TestOperatorDataHandlersRejectUnknownAndMissingFieldsBeforeStoreExecution(t *testing.T) {
	store := newDataRuntimeProbeStore(t)
	handler := OperatorDataHandlers(DataHandlerOptions{Store: store})["data.check"]
	base := map[string]any{
		"source_invocation_id": dataProbeSourceInvocationID,
		"bundle_hash":          dataProbeBundleHash,
		"declaration":          dataProbeDeclarationParams(),
		"expected_head":        map[string]any{"state": "absent"},
		"input": map[string]any{
			"format": "jsonl", "content_base64": base64.StdEncoding.EncodeToString([]byte("{\"slug\":\"alpha\"}\n")),
		},
	}
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "unknown", mutate: func(params map[string]any) { params["path"] = "/tmp/legacy.jsonl" }},
		{name: "missing", mutate: func(params map[string]any) { delete(params, "bundle_hash") }},
		{name: "null", mutate: func(params map[string]any) { params["input"] = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			params := make(map[string]any, len(base))
			for key, value := range base {
				params[key] = value
			}
			test.mutate(params)
			if _, err := handler(context.Background(), Request{Method: "data.check", Params: params, ActorTokenID: "actor"}); err == nil {
				t.Fatal("hostile request succeeded")
			}
		})
	}
}

func TestOperatorDataHandlersRejectContradictoryStoreResults(t *testing.T) {
	for _, test := range []struct {
		method string
		store  func(*dataRuntimeProbeStore) DurableDataStore
	}{
		{
			method: "data.check",
			store: func(base *dataRuntimeProbeStore) DurableDataStore {
				return &hostileDataRuntimeProbeStore{dataRuntimeProbeStore: base, mutateSource: func(result *durabledata.SourceOperationResult) {
					result.Outcome = "validation_rejected"
				}}
			},
		},
		{
			method: "data.prune",
			store: func(base *dataRuntimeProbeStore) DurableDataStore {
				return &hostileDataRuntimeProbeStore{dataRuntimeProbeStore: base, mutatePrune: func(result *durabledata.PruneOperationResult) {
					result.PayloadAfter = "materialized"
				}}
			},
		},
	} {
		t.Run(test.method, func(t *testing.T) {
			base := newDataRuntimeProbeStore(t)
			handler := OperatorDataHandlers(DataHandlerOptions{Store: test.store(base)})[test.method]
			_, err := handler(context.Background(), Request{
				Method: test.method, Params: dataProbeParams(test.method, base), ActorTokenID: actorTokenID(testToken),
			})
			var applicationErr *ApplicationError
			if !errors.As(err, &applicationErr) || applicationErr.Code != string(durabledata.CodeIntegrity) {
				t.Fatalf("contradictory %s result error = %#v, want %s", test.method, err, durabledata.CodeIntegrity)
			}
		})
	}
}

func TestDataShowDeclarationsConsumesBoundedStoreSummary(t *testing.T) {
	store := newDataRuntimeProbeStore(t)
	store.summaries = []durabledata.DeclarationSummary{{
		Declaration: store.declaration.Ref, LocalName: store.declaration.Name, SchemaDigest: store.declaration.SchemaDigest,
		Head: durabledata.VersionHead(store.version.VersionID), VersionCount: 100_000, MaterializedVersionCount: 75_000,
		MaterializedBytes: 16 * 1024 * 1024,
	}}
	handler := OperatorDataHandlers(DataHandlerOptions{Store: store})["data.show"]
	result, err := handler(context.Background(), Request{Method: "data.show", Params: map[string]any{
		"view": "declarations", "bundle_hash": dataProbeBundleHash, "page": map[string]any{},
	}})
	if err != nil {
		t.Fatal(err)
	}
	page, ok := result.(durabledata.PageResult[durabledata.DeclarationSummary])
	if !ok || len(page.Items) != 1 || page.Items[0] != store.summaries[0] {
		t.Fatalf("declaration summary page = %#v", result)
	}
	if store.showCalls != 0 {
		t.Fatalf("declaration listing loaded %d full resource snapshots", store.showCalls)
	}
}

func TestOpenRPCPermanentOperationHTTPRuntimeProbes(t *testing.T) {
	store := newDataRuntimeProbeStore(t)
	methods := approvedPermanentOperationHTTPRuntimeMethods()
	all := OperatorDataHandlers(DataHandlerOptions{Store: store})
	calls := map[string]int{}
	handlers := make(map[string]MethodHandler, len(methods))
	for _, methodName := range methods {
		methodName, method := methodName, all[methodName]
		if method == nil {
			t.Fatalf("OperatorDataHandlers missing %s", methodName)
		}
		handlers[methodName] = func(ctx context.Context, req Request) (any, error) {
			calls[methodName]++
			return method(ctx, req)
		}
	}
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: handlers})
	for _, methodName := range methods {
		t.Run(methodName+"/success", func(t *testing.T) {
			status, response, body := callReadOnlyProbeRPC(t, handler, methodName, dataProbeParams(methodName, store), "Bearer "+testToken)
			if status != 200 || response.Error != nil || response.Result == nil {
				t.Fatalf("%s response = status:%d response:%#v body:%s", methodName, status, response, body)
			}
			if calls[methodName] != 1 {
				t.Fatalf("%s calls = %d, want 1", methodName, calls[methodName])
			}
		})
		t.Run(methodName+"/unknown_field", func(t *testing.T) {
			before := calls[methodName]
			params := dataProbeParams(methodName, store)
			params["legacy_path"] = "/tmp/data.jsonl"
			_, response, _ := callReadOnlyProbeRPC(t, handler, methodName, params, "Bearer "+testToken)
			if response.Error == nil || response.Error.Code != codeInvalidParams {
				t.Fatalf("%s unknown-field response = %#v", methodName, response)
			}
			if calls[methodName] != before {
				t.Fatalf("%s dispatched an unknown-field request", methodName)
			}
		})
		t.Run(methodName+"/auth", func(t *testing.T) {
			before := calls[methodName]
			status, _, _ := callReadOnlyProbeRPC(t, handler, methodName, dataProbeParams(methodName, store), "")
			if status != 401 || calls[methodName] != before {
				t.Fatalf("%s unauthenticated status/calls = %d/%d", methodName, status, calls[methodName])
			}
		})
	}
}

func TestOperatorDataHandlersMapTypedErrors(t *testing.T) {
	for _, code := range []durabledata.ErrorCode{
		durabledata.CodeInvocationConflict,
		durabledata.CodeContractNotFound,
		durabledata.CodeDeclarationMissing,
		durabledata.CodeVersionMissing,
		durabledata.CodePayloadPruned,
		durabledata.CodeOperationMissing,
		durabledata.CodeIntegrity,
	} {
		mapped := dataApplicationError(durabledata.NewDomainErrorWithDetails(code, map[string]any{"owner": "durable-data"}, "rejected"))
		application, ok := mapped.(*ApplicationError)
		if !ok || application.Code != string(code) || application.Retryable {
			t.Fatalf("map %s = %#v", code, mapped)
		}
	}
}

func TestDataExportChunkHonorsRowLimitAndOneBasedOrdinals(t *testing.T) {
	store := newDataRuntimeProbeStore(t)
	compiled, defects := durabledata.CompileJSONL(store.declaration.Ref, map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"slug"},
		"properties": map[string]any{"slug": map[string]any{"type": "string"}},
	}, "slug", []byte("{\"slug\":\"alpha\"}\n{\"slug\":\"beta\"}\n{\"slug\":\"gamma\"}\n"))
	if len(defects) != 0 {
		t.Fatalf("compile export fixture: %#v", defects)
	}
	version := durabledata.Version{VersionID: compiled.VersionID, Manifest: compiled.Manifest}
	page, err := dataExportChunk(version, compiled.Rows, durabledata.PageRequest{Limit: 1, ByteLimit: durabledata.MaxPublicPageBytes})
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok := page.(durabledata.ExportChunk)
	if !ok || chunk.RowCount != 1 || chunk.FirstOrdinal != 1 || chunk.Continuation.State != "more" {
		t.Fatalf("first export chunk = %#v", page)
	}
	offset, err := dataCursorOffset(chunk.Continuation.Cursor, dataCursorFingerprint("export_chunk", string(version.VersionID)))
	if err != nil || offset != 1 {
		t.Fatalf("first export continuation = %q, offset %d, error %v", chunk.Continuation.Cursor, offset, err)
	}
}

func TestDataExportChunkUsesZeroOrdinalForCanonicalEmptyVersion(t *testing.T) {
	store := newDataRuntimeProbeStore(t)
	compiled, defects := durabledata.CompileJSONL(store.declaration.Ref, map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"slug"},
		"properties": map[string]any{"slug": map[string]any{"type": "string"}},
	}, "slug", nil)
	if len(defects) != 0 || len(compiled.Rows) != 0 {
		t.Fatalf("compile empty export fixture: rows=%#v defects=%#v", compiled.Rows, defects)
	}
	version := durabledata.Version{VersionID: compiled.VersionID, Manifest: compiled.Manifest}
	page, err := dataExportChunk(version, compiled.Rows, durabledata.PageRequest{Limit: 1, ByteLimit: durabledata.MaxPublicPageBytes})
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok := page.(durabledata.ExportChunk)
	if !ok || chunk.TotalRows != 0 || chunk.RowCount != 0 || chunk.FirstOrdinal != 0 || chunk.ChunkBase64 != "" ||
		chunk.ChunkBytes != 0 || chunk.Continuation != durabledata.EndContinuation() {
		t.Fatalf("empty export chunk = %#v", page)
	}
}

func TestDataShowPagesCompletePrunePinEvidenceBeyondBoundedSummary(t *testing.T) {
	store := newDataRuntimeProbeStore(t)
	for index := 0; index < 3; index++ {
		store.prunePins = append(store.prunePins, durabledata.Pin{
			RunID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1), RunState: "completed",
			Declaration: store.declaration.Ref, SchemaDigest: store.declaration.SchemaDigest,
			VersionID: store.version.VersionID, Selection: "explicit",
		})
	}
	firstOnly := durabledata.FirstEvidencePage(store.prunePins)
	store.pruneResult = durabledata.PruneOperationResult{
		Outcome: "refused_pinned", PruneInvocationID: dataProbePruneInvocationID,
		Declaration: store.declaration.Ref, VersionID: store.version.VersionID,
		ExpectedHead: durabledata.AbsentHead(), ObservedHead: durabledata.AbsentHead(),
		PinCount: len(store.prunePins), Pins: &firstOnly, CompletedAt: store.now,
	}
	handler := OperatorDataHandlers(DataHandlerOptions{Store: store})["data.show"]
	params := map[string]any{
		"view": "operation", "operation_ref": map[string]any{"kind": "prune", "prune_invocation_id": dataProbePruneInvocationID},
		"detail": "pins", "page": map[string]any{"limit": 1, "byte_limit": durabledata.MaxPublicPageBytes},
	}
	first, err := handler(context.Background(), Request{Method: "data.show", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	firstPage := first.(durabledata.PageResult[durabledata.Pin])
	if len(firstPage.Items) != 1 || firstPage.Items[0].RunID != store.prunePins[0].RunID || firstPage.Continuation.State != "more" {
		t.Fatalf("first prune pin page = %#v", firstPage)
	}
	params["page"] = map[string]any{"limit": 2, "byte_limit": durabledata.MaxPublicPageBytes, "cursor": firstPage.Continuation.Cursor}
	second, err := handler(context.Background(), Request{Method: "data.show", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	secondPage := second.(durabledata.PageResult[durabledata.Pin])
	if len(secondPage.Items) != 2 || secondPage.Items[0].RunID != store.prunePins[1].RunID || secondPage.Items[1].RunID != store.prunePins[2].RunID || secondPage.Continuation.State != "end" {
		t.Fatalf("second prune pin page = %#v", secondPage)
	}
}

func TestDataShowPinCursorIsStableAcrossConcurrentInsertions(t *testing.T) {
	store := newDataRuntimeProbeStore(t)
	pin := func(suffix int) durabledata.Pin {
		return durabledata.Pin{
			RunID: fmt.Sprintf("00000000-0000-4000-8000-%012d", suffix), RunState: "running",
			Declaration: store.declaration.Ref, SchemaDigest: store.declaration.SchemaDigest,
			VersionID: store.version.VersionID, Selection: "explicit",
		}
	}
	store.pins = []durabledata.Pin{pin(10), pin(20), pin(30)}
	handler := OperatorDataHandlers(DataHandlerOptions{Store: store})["data.show"]
	params := map[string]any{
		"view": "pins", "declaration": map[string]any{"package_key": ".", "event": dataProbeEventName},
		"selector": map[string]any{"kind": "version", "version_id": string(store.version.VersionID)},
		"page":     map[string]any{"limit": 1, "byte_limit": durabledata.MaxPublicPageBytes},
	}
	first, err := handler(context.Background(), Request{Method: "data.show", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	firstPage := first.(durabledata.PageResult[durabledata.Pin])
	if len(firstPage.Items) != 1 || firstPage.Items[0].RunID != pin(10).RunID || firstPage.Continuation.State != "more" {
		t.Fatalf("first live pin page = %#v", firstPage)
	}

	// One insertion sorts before the cursor and one after it. Keyset continuation
	// must neither repeat pin 10 nor skip pins shifted by the earlier insertion.
	store.pins = append(store.pins, pin(5), pin(15))
	params["page"] = map[string]any{
		"limit": 10, "byte_limit": durabledata.MaxPublicPageBytes, "cursor": firstPage.Continuation.Cursor,
	}
	second, err := handler(context.Background(), Request{Method: "data.show", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	secondPage := second.(durabledata.PageResult[durabledata.Pin])
	want := []string{pin(15).RunID, pin(20).RunID, pin(30).RunID}
	if len(secondPage.Items) != len(want) || secondPage.Continuation.State != "end" {
		t.Fatalf("second live pin page = %#v", secondPage)
	}
	for index := range want {
		if secondPage.Items[index].RunID != want[index] {
			t.Fatalf("second live pin page[%d] = %s, want %s", index, secondPage.Items[index].RunID, want[index])
		}
	}

	tampered := firstPage.Continuation.Cursor
	replacement := byte('A')
	if tampered[len(tampered)-1] == replacement {
		replacement = 'B'
	}
	params["page"] = map[string]any{
		"limit": 1, "byte_limit": durabledata.MaxPublicPageBytes,
		"cursor": tampered[:len(tampered)-1] + string(replacement),
	}
	if _, err := handler(context.Background(), Request{Method: "data.show", Params: params}); err == nil {
		t.Fatal("tampered pin cursor was accepted")
	}
}

func TestDataPinCursorRemainsBoundedForLongDeclaration(t *testing.T) {
	ref, err := durabledata.ParseDeclarationRef(".", strings.Repeat("a", 3072))
	if err != nil {
		t.Fatal(err)
	}
	compiled, defects := durabledata.CompileJSONL(ref, map[string]any{
		"type": "object", "additionalProperties": false,
	}, "", nil)
	if len(defects) != 0 {
		t.Fatalf("compile long declaration: %#v", defects)
	}
	version := durabledata.Version{VersionID: compiled.VersionID, Manifest: compiled.Manifest}
	pin := func(suffix int) durabledata.Pin {
		return durabledata.Pin{
			RunID: fmt.Sprintf("00000000-0000-4000-8000-%012d", suffix), RunState: "running",
			Declaration: ref, SchemaDigest: compiled.Manifest.SchemaDigest, VersionID: compiled.VersionID, Selection: "explicit",
		}
	}
	pins := []durabledata.Pin{pin(1), pin(2)}
	fingerprint := dataCursorFingerprint("pins", string(version.VersionID))
	firstRequest, err := (durabledata.PageRequest{Limit: 1}).WithDefaults()
	if err != nil {
		t.Fatal(err)
	}
	first, err := pageDataPins(pins, firstRequest, fingerprint, version)
	if err != nil || first.Continuation.State != "more" {
		t.Fatalf("first long-declaration pin page = %#v, %v", first, err)
	}
	if len(first.Continuation.Cursor) > durabledata.MaxBusinessKeyBytes {
		t.Fatalf("generated cursor bytes = %d, max %d", len(first.Continuation.Cursor), durabledata.MaxBusinessKeyBytes)
	}
	secondRequest, err := (durabledata.PageRequest{Limit: 1, Cursor: first.Continuation.Cursor}).WithDefaults()
	if err != nil {
		t.Fatalf("generated cursor rejected by public request owner: %v", err)
	}
	second, err := pageDataPins(pins, secondRequest, fingerprint, version)
	if err != nil || len(second.Items) != 1 || second.Items[0].RunID != pin(2).RunID || second.Continuation.State != "end" {
		t.Fatalf("second long-declaration pin page = %#v, %v", second, err)
	}
}

func TestDataProvenanceCursorSurvivesConcurrentLineageInsertion(t *testing.T) {
	versionID := durabledata.VersionID("resource-version-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	now := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	item := func(sequence uint64, kind string, producerSuffix int) durabledata.Provenance {
		ref, err := durabledata.NewProvenanceRef(kind, fmt.Sprintf("00000000-0000-4000-8000-%012d", producerSuffix))
		if err != nil {
			t.Fatal(err)
		}
		return durabledata.Provenance{Sequence: sequence, VersionID: versionID, ProducerRef: ref, Actor: "operator", CommittedAt: now}
	}
	fingerprint := dataCursorFingerprint("provenance", string(versionID))
	firstRequest, _ := (durabledata.PageRequest{Limit: 1}).WithDefaults()
	first, err := pageDataProvenance([]durabledata.Provenance{item(1, "import", 10), item(2, "normal_run", 30)}, firstRequest, fingerprint, versionID)
	if err != nil || len(first.Items) != 1 || first.Items[0].Sequence != 1 || first.Continuation.State != "more" {
		t.Fatalf("first provenance page = %#v, %v", first, err)
	}
	if len(first.Continuation.Cursor) > durabledata.MaxBusinessKeyBytes {
		t.Fatalf("provenance cursor bytes = %d, max %d", len(first.Continuation.Cursor), durabledata.MaxBusinessKeyBytes)
	}
	secondRequest, err := (durabledata.PageRequest{Limit: 10, Cursor: first.Continuation.Cursor}).WithDefaults()
	if err != nil {
		t.Fatal(err)
	}
	second, err := pageDataProvenance([]durabledata.Provenance{
		item(1, "import", 10), item(2, "normal_run", 30), item(3, "fork_candidate_promotion", 5), item(4, "import", 40),
	}, secondRequest, fingerprint, versionID)
	if err != nil || len(second.Items) != 3 || second.Items[0].Sequence != 2 || second.Items[1].Sequence != 3 || second.Items[2].Sequence != 4 || second.Continuation.State != "end" {
		t.Fatalf("continued provenance page = %#v, %v", second, err)
	}
	if _, err := pageDataProvenance(second.Items, secondRequest, dataCursorFingerprint("provenance", "other"), versionID); err == nil {
		t.Fatal("provenance cursor was accepted for another query")
	}
	raw, err := json.Marshal(second.Items)
	if err != nil || strings.Contains(string(raw), "sequence") {
		t.Fatalf("private provenance sequence leaked to public JSON: %s, %v", raw, err)
	}
}

func TestDataVersionAliasSelectorRejectsNoncanonicalSpellings(t *testing.T) {
	store := newDataRuntimeProbeStore(t)
	snapshot := durabledata.ResourceSnapshot{
		Declaration: store.declaration,
		Versions:    []durabledata.Version{store.version},
	}
	for _, alias := range []string{"v0", "v01", "v+1", "V1", "v1 "} {
		t.Run(alias, func(t *testing.T) {
			if _, err := selectDataVersion(snapshot, map[string]any{"kind": "alias", "alias": alias}); err == nil {
				t.Fatalf("selectDataVersion accepted noncanonical alias %q", alias)
			}
		})
	}
	version, err := selectDataVersion(snapshot, map[string]any{"kind": "alias", "alias": "v1"})
	if err != nil || version.VersionID != store.version.VersionID {
		t.Fatalf("selectDataVersion canonical alias = %#v, %v", version, err)
	}
}

type dataHTTPSelectedStore interface {
	DurableDataStore
	UpsertBundleCatalogWithData(context.Context, bundlecatalog.Upsert, durabledata.Catalog) (bundlecatalog.UpsertResult, error)
}

func TestDurableDataHTTPPublicSurfaceAcrossSelectedStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) dataHTTPSelectedStore
	}{
		{name: "sqlite", open: func(t *testing.T) dataHTTPSelectedStore { return storetest.StartSQLiteRuntimeStore(t) }},
		{name: "postgres", open: func(t *testing.T) dataHTTPSelectedStore {
			_, db, _ := testutil.StartPostgres(t)
			return storetest.AdmitPostgresRuntimeStore(t, db)
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			selected := backend.open(t)
			catalog, ref := dataHTTPProbeCatalog(t)
			if _, err := selected.UpsertBundleCatalogWithData(ctx, bundlecatalog.Upsert{
				BundleHash: catalog.BundleHash, ContentYAML: "api_version: swarm.bundle.catalog.test.v1\n",
				ParsedJSON: map[string]any{"projection_version": "swarm.bundle.catalog.v2", "agents": []any{}},
				Metadata:   map[string]any{"source": "data-http-proof"},
			}, catalog); err != nil {
				t.Fatalf("register catalog: %v", err)
			}
			handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorDataHandlers(DataHandlerOptions{Store: selected})})
			call := func(method string, params map[string]any) map[string]any {
				t.Helper()
				status, response, body := callReadOnlyProbeRPC(t, handler, method, params, "Bearer "+testToken)
				if status != 200 || response.Error != nil {
					t.Fatalf("%s = status:%d error:%#v body:%s", method, status, response.Error, body)
				}
				return asMap(t, response.Result)
			}
			callError := func(method string, params map[string]any) *rpcError {
				t.Helper()
				status, response, body := callReadOnlyProbeRPC(t, handler, method, params, "Bearer "+testToken)
				if status != 200 || response.Error == nil {
					t.Fatalf("%s hostile call = status:%d response:%#v body:%s", method, status, response, body)
				}
				return response.Error
			}
			input := func(slug string) map[string]any {
				return map[string]any{"format": "jsonl", "content_base64": base64.StdEncoding.EncodeToString([]byte("{\"slug\":\"" + slug + "\"}\n"))}
			}
			declaration := map[string]any{"package_key": ref.PackageKey, "event": ref.EventName}
			emptyCheck := call("data.check", map[string]any{
				"source_invocation_id": uuid.NewString(), "bundle_hash": dataProbeBundleHash,
				"declaration": declaration, "expected_head": map[string]any{"state": "absent"},
				"input": map[string]any{"format": "jsonl", "content_base64": ""},
			})
			emptyCandidate := asMap(t, emptyCheck["candidate"])
			emptyManifest := asMap(t, emptyCandidate["manifest"])
			if emptyCheck["outcome"] != "accepted" || emptyCandidate["state"] != "candidate" || emptyCandidate["alias"] != nil || emptyManifest["row_count"] != float64(0) {
				t.Fatalf("empty JSONL check = %#v", emptyCheck)
			}
			first := call("data.import", map[string]any{
				"source_invocation_id": uuid.NewString(), "bundle_hash": dataProbeBundleHash,
				"declaration": declaration, "expected_head": map[string]any{"state": "absent"}, "input": input("alpha"),
			})
			firstCandidate := asMap(t, first["candidate"])
			firstVersion := stringValue(t, firstCandidate["version_id"], "first version")
			if stringValue(t, first["outcome"], "first outcome") != "accepted" {
				t.Fatalf("first import = %#v", first)
			}
			staleCheck := call("data.check", map[string]any{
				"source_invocation_id": uuid.NewString(), "bundle_hash": dataProbeBundleHash,
				"declaration": declaration, "expected_head": map[string]any{"state": "absent"}, "input": input("stale"),
			})
			staleCandidate := asMap(t, staleCheck["candidate"])
			if staleCheck["outcome"] != "head_conflict" || staleCandidate["state"] != "candidate" || staleCandidate["alias"] != nil {
				t.Fatalf("stale uncommitted candidate = %#v", staleCheck)
			}
			rows := call("data.show", map[string]any{
				"view": "rows", "declaration": declaration,
				"selector": map[string]any{"kind": "version", "version_id": firstVersion}, "page": map[string]any{},
			})
			if len(asSlice(t, rows["items"])) != 1 {
				t.Fatalf("rows = %#v", rows)
			}
			keyedRow := call("data.show", map[string]any{
				"view": "row", "declaration": declaration,
				"selector": map[string]any{"kind": "version", "version_id": firstVersion}, "row_selector": map[string]any{"key": "alpha"},
			})
			if keyedRow["key"] != "alpha" || keyedRow["ordinal"] != float64(1) {
				t.Fatalf("keyed row = %#v", keyedRow)
			}
			if err := callError("data.show", map[string]any{
				"view": "row", "declaration": declaration,
				"selector": map[string]any{"kind": "version", "version_id": firstVersion}, "row_selector": map[string]any{"position": 1},
			}); err.Code != codeInvalidParams {
				t.Fatalf("keyed position selector error = %#v", err)
			}

			keylessDeclaration := map[string]any{"package_key": ".", "event": "score.observed"}
			keyless := call("data.import", map[string]any{
				"source_invocation_id": uuid.NewString(), "bundle_hash": dataProbeBundleHash,
				"declaration": keylessDeclaration, "expected_head": map[string]any{"state": "absent"},
				"input": map[string]any{"format": "jsonl", "content_base64": base64.StdEncoding.EncodeToString([]byte("{\"label\":\"same\"}\n{\"label\":\"other\"}\n{\"label\":\"same\"}\n"))},
			})
			keylessVersion := stringValue(t, asMap(t, keyless["candidate"])["version_id"], "keyless version")
			positionalRow := call("data.show", map[string]any{
				"view": "row", "declaration": keylessDeclaration,
				"selector": map[string]any{"kind": "version", "version_id": keylessVersion}, "row_selector": map[string]any{"position": 2},
			})
			if positionalRow["ordinal"] != float64(2) || positionalRow["key"] != nil || asMap(t, positionalRow["value"])["label"] != "other" {
				t.Fatalf("keyless positional row = %#v", positionalRow)
			}
			if err := callError("data.show", map[string]any{
				"view": "row", "declaration": keylessDeclaration,
				"selector": map[string]any{"kind": "version", "version_id": keylessVersion}, "row_selector": map[string]any{"key": "same"},
			}); err.Code != codeInvalidParams {
				t.Fatalf("keyless business-key selector error = %#v", err)
			}
			second := call("data.import", map[string]any{
				"source_invocation_id": uuid.NewString(), "bundle_hash": dataProbeBundleHash,
				"declaration":   declaration,
				"expected_head": map[string]any{"state": "version", "version_id": firstVersion}, "input": input("beta"),
			})
			secondVersion := stringValue(t, asMap(t, second["candidate"])["version_id"], "second version")
			pruned := call("data.prune", map[string]any{
				"prune_invocation_id": uuid.NewString(), "declaration": declaration, "version_id": firstVersion,
				"expected_head": map[string]any{"state": "version", "version_id": secondVersion},
			})
			if stringValue(t, pruned["outcome"], "prune outcome") != "pruned" {
				t.Fatalf("prune = %#v", pruned)
			}
		})
	}
}

func dataHTTPProbeCatalog(t *testing.T) (durabledata.Catalog, durabledata.DeclarationRef) {
	t.Helper()
	ref, err := durabledata.ParseDeclarationRef(".", dataProbeEventName)
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"slug"},
		"properties": map[string]any{"slug": map[string]any{"type": "string"}},
	}
	compiled, defects := durabledata.CompileJSONL(ref, schema, "slug", []byte("{\"slug\":\"probe\"}\n"))
	if len(defects) != 0 {
		t.Fatalf("compile data catalog schema: %#v", defects)
	}
	keylessRef, err := durabledata.ParseDeclarationRef(".", "score.observed")
	if err != nil {
		t.Fatal(err)
	}
	keylessSchema := map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"label"},
		"properties": map[string]any{"label": map[string]any{"type": "string"}},
	}
	keyless, defects := durabledata.CompileJSONL(keylessRef, keylessSchema, "", []byte("{\"label\":\"probe\"}\n"))
	if len(defects) != 0 {
		t.Fatalf("compile keyless data catalog schema: %#v", defects)
	}
	return durabledata.Catalog{BundleHash: dataProbeBundleHash, Declarations: []durabledata.Declaration{
		{
			Name: "startup.loaded", Ref: ref, BusinessKey: "slug",
			SchemaDigest: compiled.Manifest.SchemaDigest, CanonicalSchema: compiled.CanonicalSchema,
		},
		{
			Name: "score.observed", Ref: keylessRef,
			SchemaDigest: keyless.Manifest.SchemaDigest, CanonicalSchema: keyless.CanonicalSchema,
		},
	}}, ref
}
