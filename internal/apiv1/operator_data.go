package apiv1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
)

type DurableDataStore interface {
	ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	PruneDataResource(context.Context, durabledata.PruneCommand) (durabledata.PruneOperationResult, error)
	ListDataDeclarationSummaries(context.Context, string) ([]durabledata.DeclarationSummary, error)
	ListDataVersionSummaries(context.Context, durabledata.DeclarationRef, uint64, int) ([]durabledata.VersionSummary, error)
	ResolveDataVersionSummary(context.Context, durabledata.DeclarationRef, durabledata.VersionSelector) (durabledata.VersionSummary, error)
	LoadDataVersionPayload(context.Context, durabledata.DeclarationRef, durabledata.VersionID) (durabledata.Version, error)
	ListDataVersionProvenance(context.Context, durabledata.VersionID, uint64, int) ([]durabledata.Provenance, error)
	ListDataPins(context.Context, durabledata.VersionID, string, int) ([]durabledata.Pin, error)
	ListDataHeadHistory(context.Context, durabledata.DeclarationRef, uint64, int) ([]durabledata.HeadHistory, error)
	LoadDataSourceOperation(context.Context, string) (durabledata.SourceOperationRecord, error)
	LoadDataPruneOperation(context.Context, string) (durabledata.PruneOperationResult, error)
	LoadDataPruneOperationPins(context.Context, string) ([]durabledata.Pin, error)
	LoadDataRunCreationOperation(context.Context, string) (durabledata.RunCreationOperationRecord, error)
}

func OperatorDataHandlers(opts DataHandlerOptions) map[string]MethodHandler {
	if opts.Store == nil {
		return nil
	}
	return map[string]MethodHandler{
		"data.check": func(ctx context.Context, req Request) (any, error) {
			return executeDataSource(ctx, req, opts.Store, "check")
		},
		"data.import": func(ctx context.Context, req Request) (any, error) {
			return executeDataSource(ctx, req, opts.Store, "import")
		},
		"data.show": func(ctx context.Context, req Request) (any, error) {
			return executeDataShow(ctx, req, opts.Store)
		},
		"data.prune": func(ctx context.Context, req Request) (any, error) {
			return executeDataPrune(ctx, req, opts.Store)
		},
	}
}

func executeDataSource(ctx context.Context, req Request, store DurableDataStore, operation string) (any, error) {
	if err := exactDataParams(req.Params, "source_invocation_id", "bundle_hash", "declaration", "expected_head", "input"); err != nil {
		return nil, err
	}
	id, err := canonicalDataUUID(req.Params["source_invocation_id"], "source_invocation_id")
	if err != nil {
		return nil, err
	}
	bundleHash, err := dataString(req.Params["bundle_hash"], "bundle_hash")
	if err != nil {
		return nil, err
	}
	declaration, err := dataDeclarationRef(req.Params["declaration"])
	if err != nil {
		return nil, err
	}
	expected, err := dataExpectedHead(req.Params["expected_head"])
	if err != nil {
		return nil, err
	}
	input, err := dataInput(req.Params["input"])
	if err != nil {
		return nil, err
	}
	if len(input) > durabledata.MaxDecodedImportBytes {
		return nil, NewApplicationError("MESSAGE_BUDGET_EXCEEDED", false, map[string]any{
			"boundary": "decoded_import", "method": req.Method, "limit_bytes": durabledata.MaxDecodedImportBytes,
			"observed_bytes": len(input), "receipt_created": false,
		})
	}
	command := durabledata.SourceCommand{
		Operation: operation, SourceInvocationID: id, Actor: req.ActorTokenID, BundleHash: bundleHash,
		Declaration: declaration, ExpectedHead: expected, InputFormat: "jsonl", Input: input,
	}
	result, err := store.ExecuteDataSourceOperation(ctx, command)
	if err != nil {
		return nil, dataApplicationError(err)
	}
	if err := result.ValidateForCommand(command); err != nil {
		return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"operation": operation, "reason": err.Error()})
	}
	return result, nil
}

func executeDataPrune(ctx context.Context, req Request, store DurableDataStore) (any, error) {
	if err := exactDataParams(req.Params, "prune_invocation_id", "declaration", "version_id", "expected_head"); err != nil {
		return nil, err
	}
	id, err := canonicalDataUUID(req.Params["prune_invocation_id"], "prune_invocation_id")
	if err != nil {
		return nil, err
	}
	declaration, err := dataDeclarationRef(req.Params["declaration"])
	if err != nil {
		return nil, err
	}
	version, err := dataVersionID(req.Params["version_id"], "version_id")
	if err != nil {
		return nil, err
	}
	expected, err := dataExpectedHead(req.Params["expected_head"])
	if err != nil {
		return nil, err
	}
	command := durabledata.PruneCommand{
		PruneInvocationID: id, Actor: req.ActorTokenID, Declaration: declaration, VersionID: version, ExpectedHead: expected,
	}
	result, err := store.PruneDataResource(ctx, command)
	if err != nil {
		return nil, dataApplicationError(err)
	}
	if err := result.ValidateRequest(command); err != nil {
		return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"operation": "prune", "reason": err.Error()})
	}
	return result, nil
}

func executeDataShow(ctx context.Context, req Request, store DurableDataStore) (any, error) {
	view, err := dataString(req.Params["view"], "view")
	if err != nil {
		return nil, err
	}
	switch view {
	case "declarations":
		if err := exactDataParams(req.Params, "view", "bundle_hash", "page"); err != nil {
			return nil, err
		}
		bundleHash, err := dataString(req.Params["bundle_hash"], "bundle_hash")
		if err != nil {
			return nil, err
		}
		page, err := dataPage(req.Params["page"])
		if err != nil {
			return nil, err
		}
		items, err := store.ListDataDeclarationSummaries(ctx, bundleHash)
		if err != nil {
			return nil, dataApplicationError(err)
		}
		return pageDataItems(items, page, dataCursorFingerprint(view, bundleHash))
	case "versions", "version", "rows", "row", "export_chunk", "provenance", "pins", "head_history":
		return executeDataShowResource(ctx, req.Params, store, view)
	case "operation":
		return executeDataShowOperation(ctx, req.Params, store)
	default:
		return nil, NewInvalidParamsError(map[string]any{"field": "view", "reason": "unsupported data.show view"})
	}
}

func executeDataShowResource(ctx context.Context, params map[string]any, store DurableDataStore, view string) (any, error) {
	allowed := []string{"view", "declaration"}
	if view != "versions" && view != "head_history" {
		allowed = append(allowed, "selector")
	}
	if view == "row" {
		allowed = append(allowed, "row_selector")
	}
	if view != "version" && view != "row" {
		allowed = append(allowed, "page")
	}
	if err := exactDataParams(params, allowed...); err != nil {
		return nil, err
	}
	ref, err := dataDeclarationRef(params["declaration"])
	if err != nil {
		return nil, err
	}
	if view == "head_history" {
		page, err := dataPage(params["page"])
		if err != nil {
			return nil, err
		}
		fingerprint := dataCursorFingerprint(view, ref.Key())
		after, err := dataSequenceCursorValue(page.Cursor, fingerprint, "swarm.data.head-history.cursor.v1")
		if err != nil {
			return nil, err
		}
		history, err := store.ListDataHeadHistory(ctx, ref, after, page.Limit+1)
		if err != nil {
			return nil, dataApplicationError(err)
		}
		items := make([]durabledata.HeadHistoryDTO, len(history))
		for index, item := range history {
			items[index] = durabledata.HeadHistoryDTO{
				Revision: item.Revision, Before: item.Before, After: item.After,
				OperationRef: durabledata.SourceOperationRef{Kind: "source", SourceInvocationID: item.OperationID},
				CommittedAt:  item.CommittedAt,
			}
		}
		return pageDataItemsFrom(items, page, 0, func(_ int, last durabledata.HeadHistoryDTO) string {
			return dataSequenceCursor(last.Revision, fingerprint, "swarm.data.head-history.cursor.v1")
		})
	}
	if view == "versions" {
		page, err := dataPage(params["page"])
		if err != nil {
			return nil, err
		}
		fingerprint := dataCursorFingerprint(view, ref.Key())
		after, err := dataSequenceCursorValue(page.Cursor, fingerprint, "swarm.data.version.cursor.v1")
		if err != nil {
			return nil, err
		}
		items, err := store.ListDataVersionSummaries(ctx, ref, after, page.Limit+1)
		if err != nil {
			return nil, dataApplicationError(err)
		}
		last := after
		for _, item := range items {
			sequence, parseErr := durabledata.ParseVersionAlias(item.Alias)
			if err := item.Validate(); err != nil || parseErr != nil || item.Declaration != ref || sequence <= last {
				return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "version inventory is contradictory"})
			}
			last = sequence
		}
		return pageDataItemsFrom(items, page, 0, func(_ int, last durabledata.VersionSummary) string {
			sequence, _ := durabledata.ParseVersionAlias(last.Alias)
			return dataSequenceCursor(sequence, fingerprint, "swarm.data.version.cursor.v1")
		})
	}
	selector, err := dataVersionSelector(params["selector"])
	if err != nil {
		return nil, err
	}
	summary, err := store.ResolveDataVersionSummary(ctx, ref, selector)
	if err != nil {
		return nil, dataApplicationError(err)
	}
	if err := summary.Validate(); err != nil || summary.Declaration != ref ||
		(selector.Kind == "version" && summary.VersionID != selector.VersionID) {
		return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "resolved version summary is contradictory"})
	}
	if selector.Kind == "alias" {
		sequence, parseErr := durabledata.ParseVersionAlias(summary.Alias)
		if parseErr != nil || sequence != selector.SequenceAlias {
			return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "resolved version alias is contradictory"})
		}
	}
	if view == "version" {
		return summary, nil
	}
	if view == "provenance" {
		page, err := dataPage(params["page"])
		if err != nil {
			return nil, err
		}
		fingerprint := dataCursorFingerprint(view, string(summary.VersionID))
		after, err := dataSequenceCursorValue(page.Cursor, fingerprint, "swarm.data.provenance.cursor.v1")
		if err != nil {
			return nil, err
		}
		items, err := store.ListDataVersionProvenance(ctx, summary.VersionID, after, page.Limit+1)
		if err != nil {
			return nil, dataApplicationError(err)
		}
		for _, item := range items {
			if err := item.Validate(); err != nil || item.VersionID != summary.VersionID {
				return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "provenance contradicts selected version"})
			}
		}
		return pageDataItemsFrom(items, page, 0, func(_ int, last durabledata.Provenance) string {
			return dataSequenceCursor(last.Sequence, fingerprint, "swarm.data.provenance.cursor.v1")
		})
	}
	if view == "pins" {
		page, err := dataPage(params["page"])
		if err != nil {
			return nil, err
		}
		fingerprint := dataCursorFingerprint(view, string(summary.VersionID))
		after, err := dataPinCursorRunID(page.Cursor, fingerprint)
		if err != nil {
			return nil, err
		}
		pins, err := store.ListDataPins(ctx, summary.VersionID, after, page.Limit+1)
		if err != nil {
			return nil, dataApplicationError(err)
		}
		for _, pin := range pins {
			if err := pin.Validate(); err != nil || pin.Declaration != summary.Declaration || pin.SchemaDigest != summary.Manifest.SchemaDigest || pin.VersionID != summary.VersionID {
				return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "pin contradicts selected version"})
			}
		}
		return pageDataItemsFrom(pins, page, 0, func(_ int, last durabledata.Pin) string {
			return dataPinCursorForRun(last.RunID, fingerprint)
		})
	}
	version, err := store.LoadDataVersionPayload(ctx, ref, summary.VersionID)
	if err != nil {
		return nil, dataApplicationError(err)
	}
	actualSummary := dataVersionSummary(ref, version)
	wantBytes, wantErr := canonicaljson.Bytes(summary)
	actualBytes, actualErr := canonicaljson.Bytes(actualSummary)
	if wantErr != nil || actualErr != nil || !bytes.Equal(wantBytes, actualBytes) {
		return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"version_id": summary.VersionID, "reason": "version payload contradicts selected metadata"})
	}
	if version.PrunedAt != nil {
		return nil, NewApplicationError(string(durabledata.CodePayloadPruned), false, map[string]any{"version_id": version.VersionID})
	}
	rows, defects := durabledata.CompileJSONL(ref, mustDataSchema(version.CanonicalSchema), version.BusinessKey, version.CanonicalJSONL)
	if len(defects) != 0 || rows.VersionID != version.VersionID {
		return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"version_id": version.VersionID})
	}
	if view == "export_chunk" {
		page, err := dataPage(params["page"])
		if err != nil {
			return nil, err
		}
		return dataExportChunk(version, rows.Rows, page)
	}
	rowDTOs := make([]durabledata.RowDTO, len(rows.Rows))
	for index, row := range rows.Rows {
		var value any
		if err := json.Unmarshal(row.Canonical, &value); err != nil {
			return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"version_id": version.VersionID})
		}
		dto := durabledata.RowDTO{Declaration: ref, VersionID: version.VersionID, Ordinal: row.Ordinal, Value: value}
		if row.BusinessKey != "" {
			key := row.BusinessKey
			dto.Key = &key
		}
		rowDTOs[index] = dto
	}
	if view == "row" {
		selector, ok := params["row_selector"].(map[string]any)
		if !ok {
			return nil, NewInvalidParamsError(map[string]any{"field": "row_selector", "reason": "must be an object"})
		}
		if version.BusinessKey == "" {
			if err := exactDataParams(selector, "position"); err != nil {
				return nil, err
			}
			position, err := dataInteger(selector["position"], "row_selector.position")
			if err != nil || position == 0 {
				return nil, NewInvalidParamsError(map[string]any{"field": "row_selector.position", "reason": "must be a positive integer for a keyless version"})
			}
			for index, row := range rows.Rows {
				if row.Ordinal == uint64(position) {
					return rowDTOs[index], nil
				}
			}
			return nil, NewApplicationError(string(durabledata.CodeVersionMissing), false, map[string]any{"position": position, "version_id": version.VersionID})
		}
		if err := exactDataParams(selector, "key"); err != nil {
			return nil, err
		}
		key, err := durabledata.BusinessKeyFromValue(selector["key"])
		if err != nil {
			return nil, NewInvalidParamsError(map[string]any{"field": "row_selector.key", "reason": err.Error()})
		}
		for index, row := range rows.Rows {
			if row.BusinessKey == key {
				return rowDTOs[index], nil
			}
		}
		return nil, NewApplicationError(string(durabledata.CodeVersionMissing), false, map[string]any{"key": key, "version_id": version.VersionID})
	}
	page, err := dataPage(params["page"])
	if err != nil {
		return nil, err
	}
	return pageDataItems(rowDTOs, page, dataCursorFingerprint(view, string(version.VersionID)))
}

func executeDataShowOperation(ctx context.Context, params map[string]any, store DurableDataStore) (any, error) {
	detail, err := dataString(params["detail"], "detail")
	if err != nil {
		return nil, err
	}
	if detail == "summary" {
		if err := exactDataParams(params, "view", "operation_ref", "detail"); err != nil {
			return nil, err
		}
	} else if err := exactDataParams(params, "view", "operation_ref", "detail", "page"); err != nil {
		return nil, err
	}
	ref, ok := params["operation_ref"].(map[string]any)
	if !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": "operation_ref", "reason": "must be an object"})
	}
	kind, err := dataString(ref["kind"], "operation_ref.kind")
	if err != nil {
		return nil, err
	}
	switch kind {
	case "source":
		if err := exactDataParams(ref, "kind", "source_invocation_id"); err != nil {
			return nil, err
		}
		id, err := canonicalDataUUID(ref["source_invocation_id"], "operation_ref.source_invocation_id")
		if err != nil {
			return nil, err
		}
		record, err := store.LoadDataSourceOperation(ctx, id)
		if err != nil {
			return nil, dataApplicationError(err)
		}
		if err := record.Validate(); err != nil {
			return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"operation": id, "reason": err.Error()})
		}
		if detail == "summary" {
			summary := durabledata.SummarizeSource(record)
			return durabledata.OperationSummary{Kind: "source", Source: &summary}, nil
		}
		page, err := dataPage(params["page"])
		if err != nil {
			return nil, err
		}
		var items any
		switch detail {
		case "defects":
			items = record.Evidence.Defects
		case "delta_added":
			items = record.Evidence.DeltaAdded
		case "delta_removed":
			items = record.Evidence.DeltaRemoved
		case "delta_changed":
			items = record.Evidence.DeltaChanged
		default:
			return nil, NewInvalidParamsError(map[string]any{"field": "detail", "reason": "incompatible source operation detail"})
		}
		return pageDataDynamic(items, page, dataCursorFingerprint(kind, id, detail))
	case "prune":
		if err := exactDataParams(ref, "kind", "prune_invocation_id"); err != nil {
			return nil, err
		}
		id, err := canonicalDataUUID(ref["prune_invocation_id"], "operation_ref.prune_invocation_id")
		if err != nil {
			return nil, err
		}
		result, err := store.LoadDataPruneOperation(ctx, id)
		if err != nil {
			return nil, dataApplicationError(err)
		}
		if err := result.Validate(); err != nil {
			return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"operation": id, "reason": err.Error()})
		}
		if detail == "summary" {
			return durabledata.OperationSummary{Kind: "prune", Prune: &result}, nil
		}
		page, err := dataPage(params["page"])
		if err != nil {
			return nil, err
		}
		switch detail {
		case "defects":
			if result.Defects == nil {
				return nil, NewInvalidParamsError(map[string]any{"field": "detail", "reason": "prune operation has no defects"})
			}
			return pageDataItems(result.Defects.Items, page, dataCursorFingerprint(kind, id, detail))
		case "pins":
			if result.Pins == nil {
				return nil, NewInvalidParamsError(map[string]any{"field": "detail", "reason": "prune operation has no pins"})
			}
			pins, err := store.LoadDataPruneOperationPins(ctx, id)
			if err != nil {
				return nil, dataApplicationError(err)
			}
			if err := result.ValidateWithPins(pins); err != nil {
				return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"operation": id, "reason": err.Error()})
			}
			return pageDataItems(pins, page, dataCursorFingerprint(kind, id, detail))
		default:
			return nil, NewInvalidParamsError(map[string]any{"field": "detail", "reason": "incompatible prune operation detail"})
		}
	case "run_creation":
		if err := exactDataParams(ref, "kind", "run_id"); err != nil {
			return nil, err
		}
		runID, err := canonicalDataUUID(ref["run_id"], "operation_ref.run_id")
		if err != nil {
			return nil, err
		}
		record, err := store.LoadDataRunCreationOperation(ctx, runID)
		if err != nil {
			return nil, dataApplicationError(err)
		}
		if err := record.Validate(); err != nil {
			return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"operation": runID, "reason": err.Error()})
		}
		if detail == "summary" {
			return durabledata.OperationSummary{Kind: "run_creation", RunCreation: &record.Summary}, nil
		}
		page, err := dataPage(params["page"])
		if err != nil {
			return nil, err
		}
		switch detail {
		case "child_evaluations":
			return pageDataItems(record.Evidence.ChildEvaluations, page, dataCursorFingerprint(kind, runID, detail))
		case "child_defects":
			return pageDataItems(record.Evidence.ChildDefects, page, dataCursorFingerprint(kind, runID, detail))
		case "run_binding":
			return pageDataItems(record.Evidence.RunBinding, page, dataCursorFingerprint(kind, runID, detail))
		default:
			return nil, NewInvalidParamsError(map[string]any{"field": "detail", "reason": "incompatible run-creation operation detail"})
		}
	default:
		return nil, NewInvalidParamsError(map[string]any{"field": "operation_ref.kind", "reason": "must be source, prune, or run_creation"})
	}
}

func dataVersionSummary(ref durabledata.DeclarationRef, version durabledata.Version) durabledata.VersionSummary {
	state := "materialized"
	bytes := len(version.CanonicalJSONL)
	if version.PrunedAt != nil {
		state = "pruned"
		bytes = 0
	}
	return durabledata.VersionSummary{Declaration: ref, VersionID: version.VersionID, Alias: fmt.Sprintf("v%d", version.SequenceAlias),
		Manifest: version.Manifest, BusinessKey: version.BusinessKey, PayloadState: state, MaterializedBytes: bytes}
}

func dataVersionSelector(raw any) (durabledata.VersionSelector, error) {
	selector, ok := raw.(map[string]any)
	if !ok {
		return durabledata.VersionSelector{}, NewInvalidParamsError(map[string]any{"field": "selector", "reason": "must be an object"})
	}
	kind, err := dataString(selector["kind"], "selector.kind")
	if err != nil {
		return durabledata.VersionSelector{}, err
	}
	switch kind {
	case "head":
		if err := exactDataParams(selector, "kind"); err != nil {
			return durabledata.VersionSelector{}, err
		}
		return durabledata.VersionSelector{Kind: "head"}, nil
	case "version":
		if err := exactDataParams(selector, "kind", "version_id"); err != nil {
			return durabledata.VersionSelector{}, err
		}
		target, err := dataVersionID(selector["version_id"], "selector.version_id")
		if err != nil {
			return durabledata.VersionSelector{}, err
		}
		return durabledata.VersionSelector{Kind: "version", VersionID: target}, nil
	case "alias":
		if err := exactDataParams(selector, "kind", "alias"); err != nil {
			return durabledata.VersionSelector{}, err
		}
		alias, err := dataString(selector["alias"], "selector.alias")
		if err != nil {
			return durabledata.VersionSelector{}, NewInvalidParamsError(map[string]any{"field": "selector.alias", "reason": "must be vN"})
		}
		sequence, parseErr := durabledata.ParseVersionAlias(alias)
		if parseErr != nil {
			return durabledata.VersionSelector{}, NewInvalidParamsError(map[string]any{"field": "selector.alias", "reason": "must be vN"})
		}
		return durabledata.VersionSelector{Kind: "alias", SequenceAlias: sequence}, nil
	default:
		return durabledata.VersionSelector{}, NewInvalidParamsError(map[string]any{"field": "selector.kind", "reason": "must be head, version, or alias"})
	}
}

func dataExportChunk(version durabledata.Version, rows []durabledata.Row, page durabledata.PageRequest) (any, error) {
	fingerprint := dataCursorFingerprint("export_chunk", string(version.VersionID))
	offset, err := dataCursorOffset(page.Cursor, fingerprint)
	if err != nil {
		return nil, err
	}
	if offset > len(rows) {
		return nil, NewApplicationError("DATA_CURSOR_INVALID", false, map[string]any{"reason": "cursor is past the version"})
	}
	var chunk []byte
	count := 0
	for index := offset; index < len(rows) && count < page.Limit; index++ {
		line := append(append([]byte(nil), rows[index].Canonical...), '\n')
		if len(line) > page.ByteLimit {
			return nil, NewApplicationError("MESSAGE_BUDGET_EXCEEDED", false, map[string]any{"boundary": "public_page", "limit_bytes": page.ByteLimit, "observed_bytes": len(line), "receipt_created": false})
		}
		if count > 0 && len(chunk)+len(line) > page.ByteLimit {
			break
		}
		chunk = append(chunk, line...)
		count++
	}
	continuation := durabledata.EndContinuation()
	if offset+count < len(rows) {
		continuation = durabledata.PageContinuation{State: "more", Cursor: dataCursor(offset+count, fingerprint)}
	}
	hash := sha256.Sum256(chunk)
	first := uint64(0)
	if count > 0 {
		first = rows[offset].Ordinal
	}
	return durabledata.ExportChunk{Declaration: version.Manifest.Declaration, VersionID: version.VersionID,
		ContentDigest: version.Manifest.ContentDigest, TotalRows: len(rows), FirstOrdinal: first,
		RowCount: count, ChunkBase64: base64.StdEncoding.EncodeToString(chunk), ChunkBytes: len(chunk),
		ChunkSHA256: "sha256:" + hex.EncodeToString(hash[:]), Continuation: continuation}, nil
}

func exactDataParams(params map[string]any, names ...string) error {
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		allowed[name] = struct{}{}
		if _, ok := params[name]; !ok {
			return NewInvalidParamsError(map[string]any{"field": name, "reason": "is required"})
		}
	}
	for name := range params {
		if _, ok := allowed[name]; !ok {
			return NewInvalidParamsError(map[string]any{"field": name, "reason": "unknown parameter"})
		}
	}
	return nil
}

func dataString(raw any, field string) (string, error) {
	value, ok := raw.(string)
	if !ok || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "must be a non-empty string without surrounding whitespace"})
	}
	return value, nil
}

func canonicalDataUUID(raw any, field string) (string, error) {
	value, err := dataString(raw, field)
	if err != nil {
		return "", err
	}
	parsed, parseErr := uuid.Parse(value)
	if parseErr != nil || parsed == uuid.Nil || parsed.String() != value {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "must be one canonical non-zero lowercase UUID"})
	}
	return value, nil
}

func dataDeclarationRef(raw any) (durabledata.DeclarationRef, error) {
	object, ok := raw.(map[string]any)
	if !ok {
		return durabledata.DeclarationRef{}, NewInvalidParamsError(map[string]any{"field": "declaration", "reason": "must be an object"})
	}
	if err := exactDataParams(object, "package_key", "event"); err != nil {
		return durabledata.DeclarationRef{}, err
	}
	packageKey, err := dataString(object["package_key"], "declaration.package_key")
	if err != nil {
		return durabledata.DeclarationRef{}, err
	}
	eventName, err := dataString(object["event"], "declaration.event")
	if err != nil {
		return durabledata.DeclarationRef{}, err
	}
	ref, err := durabledata.ParseDeclarationRef(packageKey, eventName)
	if err != nil {
		return durabledata.DeclarationRef{}, NewInvalidParamsError(map[string]any{"field": "declaration", "reason": err.Error()})
	}
	return ref, nil
}

func dataExpectedHead(raw any) (durabledata.ExpectedHead, error) {
	object, ok := raw.(map[string]any)
	if !ok {
		return durabledata.ExpectedHead{}, NewInvalidParamsError(map[string]any{"field": "expected_head", "reason": "must be an object"})
	}
	state, err := dataString(object["state"], "expected_head.state")
	if err != nil {
		return durabledata.ExpectedHead{}, err
	}
	switch state {
	case "absent":
		if err := exactDataParams(object, "state"); err != nil {
			return durabledata.ExpectedHead{}, err
		}
		return durabledata.AbsentHead(), nil
	case "version":
		if err := exactDataParams(object, "state", "version_id"); err != nil {
			return durabledata.ExpectedHead{}, err
		}
		version, err := dataVersionID(object["version_id"], "expected_head.version_id")
		if err != nil {
			return durabledata.ExpectedHead{}, err
		}
		return durabledata.VersionHead(version), nil
	default:
		return durabledata.ExpectedHead{}, NewInvalidParamsError(map[string]any{"field": "expected_head.state", "reason": "must be absent or version"})
	}
}

func dataVersionID(raw any, field string) (durabledata.VersionID, error) {
	value, err := dataString(raw, field)
	if err != nil {
		return "", err
	}
	id := durabledata.VersionID(value)
	if err := id.Validate(); err != nil {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": err.Error()})
	}
	return id, nil
}

func dataInput(raw any) ([]byte, error) {
	object, ok := raw.(map[string]any)
	if !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": "input", "reason": "must be an object"})
	}
	if err := exactDataParams(object, "format", "content_base64"); err != nil {
		return nil, err
	}
	format, err := dataString(object["format"], "input.format")
	if err != nil || format != "jsonl" {
		return nil, NewInvalidParamsError(map[string]any{"field": "input.format", "reason": "must be jsonl"})
	}
	encoded, ok := object["content_base64"].(string)
	if !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": "input.content_base64", "reason": "must be a string"})
	}
	decoded, decodeErr := base64.StdEncoding.DecodeString(encoded)
	if decodeErr != nil || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, NewInvalidParamsError(map[string]any{"field": "input.content_base64", "reason": "must be canonical padded base64"})
	}
	return decoded, nil
}

func dataPage(raw any) (durabledata.PageRequest, error) {
	object, ok := raw.(map[string]any)
	if !ok {
		return durabledata.PageRequest{}, NewInvalidParamsError(map[string]any{"field": "page", "reason": "must be an object"})
	}
	for name := range object {
		if name != "limit" && name != "byte_limit" && name != "cursor" {
			return durabledata.PageRequest{}, NewInvalidParamsError(map[string]any{"field": "page." + name, "reason": "unknown field"})
		}
	}
	page := durabledata.PageRequest{}
	var err error
	if raw, ok := object["limit"]; ok {
		page.Limit, err = dataInteger(raw, "page.limit")
		if err != nil {
			return durabledata.PageRequest{}, err
		}
	}
	if raw, ok := object["byte_limit"]; ok {
		page.ByteLimit, err = dataInteger(raw, "page.byte_limit")
		if err != nil {
			return durabledata.PageRequest{}, err
		}
	}
	if raw, ok := object["cursor"]; ok {
		page.Cursor, err = dataString(raw, "page.cursor")
		if err != nil {
			return durabledata.PageRequest{}, err
		}
	}
	page, err = page.WithDefaults()
	if err != nil {
		return durabledata.PageRequest{}, NewInvalidParamsError(map[string]any{"field": "page", "reason": err.Error()})
	}
	return page, nil
}

func dataInteger(raw any, field string) (int, error) {
	var value int64
	switch typed := raw.(type) {
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, NewInvalidParamsError(map[string]any{"field": field, "reason": "must be an integer"})
		}
		value = parsed
	case float64:
		value = int64(typed)
		if float64(value) != typed {
			return 0, NewInvalidParamsError(map[string]any{"field": field, "reason": "must be an integer"})
		}
	case int:
		value = int64(typed)
	default:
		return 0, NewInvalidParamsError(map[string]any{"field": field, "reason": "must be an integer"})
	}
	if value < 0 || int64(int(value)) != value {
		return 0, NewInvalidParamsError(map[string]any{"field": field, "reason": "must be a non-negative integer"})
	}
	return int(value), nil
}

func pageDataItems[T any](all []T, page durabledata.PageRequest, fingerprint string) (durabledata.PageResult[T], error) {
	offset, err := dataCursorOffset(page.Cursor, fingerprint)
	if err != nil {
		return durabledata.PageResult[T]{}, err
	}
	return pageDataItemsFrom(all, page, offset, func(nextOffset int, _ T) string {
		return dataCursor(nextOffset, fingerprint)
	})
}

func pageDataItemsFrom[T any](all []T, page durabledata.PageRequest, offset int, nextCursor func(int, T) string) (durabledata.PageResult[T], error) {
	if offset > len(all) {
		return durabledata.PageResult[T]{}, NewApplicationError("DATA_CURSOR_INVALID", false, map[string]any{"reason": "cursor is past the result set"})
	}
	items := make([]T, 0, min(page.Limit, len(all)-offset))
	used := 2
	for index := offset; index < len(all) && len(items) < page.Limit; index++ {
		raw, err := canonicaljson.Bytes(all[index])
		if err != nil {
			return durabledata.PageResult[T]{}, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "page item is not canonically encodable"})
		}
		increment := len(raw)
		if len(items) > 0 {
			increment++
		}
		if len(items) == 0 && used+increment > page.ByteLimit {
			return durabledata.PageResult[T]{}, dataPageBudgetError(page.ByteLimit, used+increment)
		}
		if len(items) > 0 && used+increment > page.ByteLimit {
			break
		}
		items = append(items, all[index])
		used += increment
	}
	continuation := durabledata.EndContinuation()
	if offset+len(items) < len(all) {
		continuation = durabledata.PageContinuation{State: "more", Cursor: nextCursor(offset+len(items), items[len(items)-1])}
	}
	return durabledata.PageResult[T]{Items: items, ItemCount: len(items), EncodedItemsBytes: used, Continuation: continuation}, nil
}

type dataPinCursorPayload struct {
	Format      string `json:"format"`
	Fingerprint string `json:"fingerprint"`
	RunID       string `json:"run_id"`
}

type dataPinCursorEnvelope struct {
	Payload  dataPinCursorPayload `json:"payload"`
	Checksum string               `json:"checksum"`
}

type dataSequenceCursorPayload struct {
	Format      string `json:"format"`
	Fingerprint string `json:"fingerprint"`
	Sequence    uint64 `json:"sequence"`
}

type dataSequenceCursorEnvelope struct {
	Payload  dataSequenceCursorPayload `json:"payload"`
	Checksum string                    `json:"checksum"`
}

func pageDataProvenance(items []durabledata.Provenance, page durabledata.PageRequest, fingerprint string, versionID durabledata.VersionID) (durabledata.PageResult[durabledata.Provenance], error) {
	for index, item := range items {
		if err := item.Validate(); err != nil || item.VersionID != versionID {
			return durabledata.PageResult[durabledata.Provenance]{}, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "provenance contradicts selected version"})
		}
		if index > 0 && items[index-1].Sequence >= item.Sequence {
			return durabledata.PageResult[durabledata.Provenance]{}, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "provenance is not strictly sequence ordered"})
		}
	}
	offset := 0
	if page.Cursor != "" {
		key, err := dataProvenanceCursorKey(page.Cursor, fingerprint)
		if err != nil {
			return durabledata.PageResult[durabledata.Provenance]{}, err
		}
		anchor := sort.Search(len(items), func(index int) bool { return items[index].Sequence >= key.Sequence })
		if anchor == len(items) || items[anchor].Sequence != key.Sequence {
			return durabledata.PageResult[durabledata.Provenance]{}, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "provenance cursor anchor is absent"})
		}
		offset = anchor + 1
	}
	return pageDataItemsFrom(items, page, offset, func(_ int, last durabledata.Provenance) string {
		return dataProvenanceCursor(last.Sequence, fingerprint)
	})
}

func dataProvenanceCursor(sequence uint64, fingerprint string) string {
	return dataSequenceCursor(sequence, fingerprint, "swarm.data.provenance.cursor.v1")
}

func dataSequenceCursor(sequence uint64, fingerprint, format string) string {
	payload := dataSequenceCursorPayload{Format: format, Fingerprint: fingerprint, Sequence: sequence}
	payloadBytes, _ := canonicaljson.Bytes(payload)
	hash := sha256.Sum256(append([]byte(format+"\x00"), payloadBytes...))
	raw, _ := canonicaljson.Bytes(dataSequenceCursorEnvelope{Payload: payload, Checksum: hex.EncodeToString(hash[:])})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func dataProvenanceCursorKey(cursor, fingerprint string) (dataSequenceCursorPayload, error) {
	return dataSequenceCursorKey(cursor, fingerprint, "swarm.data.provenance.cursor.v1")
}

func dataSequenceCursorValue(cursor, fingerprint, format string) (uint64, error) {
	if cursor == "" {
		return 0, nil
	}
	key, err := dataSequenceCursorKey(cursor, fingerprint, format)
	return key.Sequence, err
}

func dataSequenceCursorKey(cursor, fingerprint, format string) (dataSequenceCursorPayload, error) {
	invalid := func(reason string) (dataSequenceCursorPayload, error) {
		return dataSequenceCursorPayload{}, NewApplicationError("DATA_CURSOR_INVALID", false, map[string]any{"reason": reason})
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return invalid("cursor encoding is invalid")
	}
	var envelope dataSequenceCursorEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return invalid("cursor payload is invalid")
	}
	canonical, err := canonicaljson.Bytes(envelope)
	if err != nil || !bytes.Equal(canonical, raw) {
		return invalid("cursor payload is not canonical")
	}
	payload := envelope.Payload
	if payload.Format != format || payload.Fingerprint != fingerprint || payload.Sequence == 0 {
		return invalid("cursor does not belong to this query")
	}
	payloadBytes, _ := canonicaljson.Bytes(payload)
	hash := sha256.Sum256(append([]byte(format+"\x00"), payloadBytes...))
	if envelope.Checksum != hex.EncodeToString(hash[:]) {
		return invalid("cursor checksum is invalid")
	}
	return payload, nil
}

func pageDataPins(pins []durabledata.Pin, page durabledata.PageRequest, fingerprint string, version durabledata.Version) (durabledata.PageResult[durabledata.Pin], error) {
	for index, pin := range pins {
		if err := pin.Validate(); err != nil {
			return durabledata.PageResult[durabledata.Pin]{}, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": err.Error()})
		}
		if pin.Declaration != version.Manifest.Declaration || pin.SchemaDigest != version.Manifest.SchemaDigest || pin.VersionID != version.VersionID {
			return durabledata.PageResult[durabledata.Pin]{}, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "pin contradicts selected version"})
		}
		if index > 0 && compareDataPinKey(pins[index-1], pin) >= 0 {
			return durabledata.PageResult[durabledata.Pin]{}, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "pins are not strictly ordered by declaration and run_id"})
		}
	}
	offset := 0
	if page.Cursor != "" {
		key, err := dataPinCursorKey(page.Cursor, fingerprint)
		if err != nil {
			return durabledata.PageResult[durabledata.Pin]{}, err
		}
		offset = sort.Search(len(pins), func(index int) bool {
			return strings.Compare(pins[index].RunID, key.RunID) > 0
		})
	}
	return pageDataItemsFrom(pins, page, offset, func(_ int, last durabledata.Pin) string {
		return dataPinCursor(last, fingerprint)
	})
}

func compareDataPinKey(left, right durabledata.Pin) int {
	if cmp := durabledata.CompareDeclarationRef(left.Declaration, right.Declaration); cmp != 0 {
		return cmp
	}
	return strings.Compare(left.RunID, right.RunID)
}

func dataPinCursor(pin durabledata.Pin, fingerprint string) string {
	return dataPinCursorForRun(pin.RunID, fingerprint)
}

func dataPinCursorForRun(runID, fingerprint string) string {
	payload := dataPinCursorPayload{
		Format: "swarm.data.pin.cursor.v1", Fingerprint: fingerprint, RunID: runID,
	}
	payloadBytes, _ := canonicaljson.Bytes(payload)
	hash := sha256.Sum256(append([]byte("swarm.data.pin.cursor.v1\x00"), payloadBytes...))
	raw, _ := canonicaljson.Bytes(dataPinCursorEnvelope{Payload: payload, Checksum: hex.EncodeToString(hash[:])})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func dataPinCursorRunID(cursor, fingerprint string) (string, error) {
	if cursor == "" {
		return "", nil
	}
	key, err := dataPinCursorKey(cursor, fingerprint)
	return key.RunID, err
}

func dataPinCursorKey(cursor, fingerprint string) (dataPinCursorPayload, error) {
	invalid := func(reason string) (dataPinCursorPayload, error) {
		return dataPinCursorPayload{}, NewApplicationError("DATA_CURSOR_INVALID", false, map[string]any{"reason": reason})
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return invalid("cursor encoding is invalid")
	}
	var envelope dataPinCursorEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return invalid("cursor payload is invalid")
	}
	canonical, err := canonicaljson.Bytes(envelope)
	if err != nil || !bytes.Equal(canonical, raw) {
		return invalid("cursor payload is not canonical")
	}
	payload := envelope.Payload
	if payload.Format != "swarm.data.pin.cursor.v1" || payload.Fingerprint != fingerprint {
		return invalid("cursor does not belong to this query")
	}
	parsed, err := uuid.Parse(payload.RunID)
	if err != nil || parsed == uuid.Nil || parsed.String() != payload.RunID {
		return invalid("cursor run_id is invalid")
	}
	payloadBytes, _ := canonicaljson.Bytes(payload)
	hash := sha256.Sum256(append([]byte("swarm.data.pin.cursor.v1\x00"), payloadBytes...))
	if envelope.Checksum != hex.EncodeToString(hash[:]) {
		return invalid("cursor checksum is invalid")
	}
	return payload, nil
}

func pageDataDynamic(all any, page durabledata.PageRequest, fingerprint string) (any, error) {
	raw, err := json.Marshal(all)
	if err != nil {
		return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "operation evidence is not encodable"})
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, NewApplicationError(string(durabledata.CodeIntegrity), false, map[string]any{"reason": "operation evidence is not an array"})
	}
	result, err := pageDataItems(items, page, fingerprint)
	if err != nil {
		return nil, err
	}
	return durabledata.RawPageResult{
		Items: result.Items, ItemCount: result.ItemCount, EncodedItemsBytes: result.EncodedItemsBytes,
		Continuation: result.Continuation,
	}, nil
}

func dataPageBudgetError(limit, observed int) error {
	return NewApplicationError("MESSAGE_BUDGET_EXCEEDED", false, map[string]any{
		"boundary": "public_page", "method": "data.show", "limit_bytes": limit,
		"observed_bytes": observed, "receipt_created": false,
	})
}

func dataCursorFingerprint(parts ...string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarm.data.cursor.query.v1"))
	for _, part := range parts {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func dataCursor(offset int, fingerprint string) string {
	payload := strconv.Itoa(offset) + ":" + fingerprint
	hash := sha256.Sum256([]byte("swarm.data.cursor.v1\x00" + payload))
	return base64.RawURLEncoding.EncodeToString([]byte(payload + ":" + hex.EncodeToString(hash[:])))
}

func dataCursorOffset(cursor, fingerprint string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, NewApplicationError("DATA_CURSOR_INVALID", false, map[string]any{"reason": "cursor encoding is invalid"})
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 3 || parts[1] != fingerprint {
		return 0, NewApplicationError("DATA_CURSOR_INVALID", false, map[string]any{"reason": "cursor does not belong to this query"})
	}
	payload := parts[0] + ":" + parts[1]
	hash := sha256.Sum256([]byte("swarm.data.cursor.v1\x00" + payload))
	if parts[2] != hex.EncodeToString(hash[:]) {
		return 0, NewApplicationError("DATA_CURSOR_INVALID", false, map[string]any{"reason": "cursor checksum is invalid"})
	}
	offset, err := strconv.Atoi(parts[0])
	if err != nil || offset < 0 {
		return 0, NewApplicationError("DATA_CURSOR_INVALID", false, map[string]any{"reason": "cursor offset is invalid"})
	}
	return offset, nil
}

func dataApplicationError(err error) error {
	var domain *durabledata.DomainError
	if errors.As(err, &domain) {
		details := map[string]any{"reason": domain.Message}
		for key, value := range domain.Details {
			details[key] = value
		}
		return NewApplicationError(string(domain.Code), false, details)
	}
	return err
}

func mustDataSchema(raw []byte) map[string]any {
	var schema map[string]any
	_ = json.Unmarshal(raw, &schema)
	return schema
}
