package tools

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/durabledata"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolresultpolicy"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
)

type flowDataReadInput struct {
	Kind        string                      `json:"kind"`
	StaticID    durabledata.StaticDataID    `json:"static_id"`
	Declaration *durabledata.DeclarationRef `json:"declaration"`
	Position    uint64                      `json:"position"`
	Key         any                         `json:"key"`
	Page        *durabledata.PageRequest    `json:"page"`
	Cursor      string                      `json:"cursor,omitempty"`
}

type flowDataCursor struct {
	Version          string                   `json:"version"`
	RunID            string                   `json:"run_id"`
	ActorFingerprint string                   `json:"actor_fingerprint"`
	StaticID         durabledata.StaticDataID `json:"static_id"`
	ContentDigest    string                   `json:"content_digest"`
	Offset           int                      `json:"offset"`
}

type flowDataCursorEnvelope struct {
	Cursor   flowDataCursor `json:"cursor"`
	Checksum string         `json:"checksum"`
}

type resourceDataCursor struct {
	Version           string `json:"version"`
	RunID             string `json:"run_id"`
	ActorFingerprint  string `json:"actor_fingerprint"`
	TargetFingerprint string `json:"target_fingerprint"`
	Offset            int    `json:"offset"`
}

type resourceDataCursorEnvelope struct {
	Cursor   resourceDataCursor `json:"cursor"`
	Checksum string             `json:"checksum"`
}

func (e *Executor) execReadFlowData(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	var in flowDataReadInput
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	switch in.Kind {
	case "static_file":
		if err := exactFlowDataInput(input, "kind", "static_id", "cursor"); err != nil {
			return nil, err
		}
		return e.execReadStaticFlowData(ctx, actor, in)
	case "resource_row":
		if err := exactFlowDataInput(input, "kind", "declaration", "position", "key"); err != nil {
			return nil, err
		}
		return e.execReadResourceData(ctx, actor, in)
	case "resource_rows":
		if err := exactFlowDataInput(input, "kind", "declaration", "page"); err != nil {
			return nil, err
		}
		return e.execReadResourceData(ctx, actor, in)
	default:
		return nil, fmt.Errorf("read_flow_data kind must be static_file, resource_row, or resource_rows")
	}
}

func (e *Executor) execReadStaticFlowData(ctx context.Context, actor models.AgentConfig, in flowDataReadInput) (any, error) {
	runID := runtimecorrelation.RunIDFromContext(ctx)
	if runID == "" {
		return nil, fmt.Errorf("read_flow_data requires run context")
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	resolved, err := flowdata.Resolve(source, actor, in.StaticID)
	if err != nil {
		return nil, err
	}
	actorFingerprint := ""
	if in.Cursor != "" {
		actorFingerprint, err = actor.Identity.Fingerprint()
		if err != nil {
			return nil, fmt.Errorf("read_flow_data cursor requires canonical actor identity: %w", err)
		}
	}
	offset := 0
	if in.Cursor != "" {
		cursor, err := decodeFlowDataCursor(in.Cursor)
		if err != nil {
			return nil, err
		}
		if cursor.Version != "swarm.flow-data.cursor.v1" || cursor.RunID != runID ||
			cursor.ActorFingerprint != actorFingerprint || cursor.StaticID != resolved.StaticID ||
			cursor.ContentDigest != resolved.ContentDigest || cursor.Offset < 0 || cursor.Offset >= len(resolved.Content) {
			return nil, fmt.Errorf("read_flow_data cursor does not match the selected run, actor, static data, or content")
		}
		offset = cursor.Offset
	}
	buildResult := func(end int) (map[string]any, error) {
		continuation := durabledata.EndContinuation()
		if end < len(resolved.Content) {
			if actorFingerprint == "" {
				actorFingerprint, err = actor.Identity.Fingerprint()
				if err != nil {
					return nil, fmt.Errorf("read_flow_data cursor requires canonical actor identity: %w", err)
				}
			}
			cursor, encodeErr := encodeFlowDataCursor(flowDataCursor{
				Version: "swarm.flow-data.cursor.v1", RunID: runID, ActorFingerprint: actorFingerprint,
				StaticID: resolved.StaticID, ContentDigest: resolved.ContentDigest, Offset: end,
			})
			if encodeErr != nil {
				return nil, encodeErr
			}
			continuation = durabledata.PageContinuation{State: "more", Cursor: cursor}
		}
		chunk := resolved.Content[offset:end]
		return map[string]any{
			"kind":           "static_file",
			"static_id":      resolved.StaticID,
			"flow_path":      resolved.FlowPath,
			"relative_path":  resolved.RelativePath,
			"content_digest": resolved.ContentDigest,
			"content_type":   resolved.ContentType,
			"total_bytes":    len(resolved.Content),
			"content":        string(chunk),
			"chunk_bytes":    len(chunk),
			"continuation":   continuation,
		}, nil
	}
	if complete, buildErr := buildResult(len(resolved.Content)); buildErr != nil {
		return nil, buildErr
	} else if raw, marshalErr := json.Marshal(complete); marshalErr != nil {
		return nil, marshalErr
	} else if len(raw) <= toolresultpolicy.MaxInlineToolResultBytes {
		return complete, nil
	}

	maxEnd := min(len(resolved.Content), offset+toolresultpolicy.MaxInlineToolResultBytes)
	boundaries := []int{offset}
	for end := offset; end < maxEnd; {
		_, size := utf8.DecodeRune(resolved.Content[end:])
		if size == 0 || end+size > maxEnd {
			break
		}
		end += size
		if end < len(resolved.Content) {
			boundaries = append(boundaries, end)
		}
	}
	best := 0
	for low, high := 1, len(boundaries)-1; low <= high; {
		middle := low + (high-low)/2
		candidate, buildErr := buildResult(boundaries[middle])
		if buildErr != nil {
			return nil, buildErr
		}
		raw, marshalErr := json.Marshal(candidate)
		if marshalErr != nil {
			return nil, marshalErr
		}
		if len(raw) <= toolresultpolicy.MaxInlineToolResultBytes {
			best = middle
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	if best == 0 {
		return nil, fmt.Errorf("read_flow_data cannot produce a UTF-8 chunk within the tool result byte budget")
	}
	return buildResult(boundaries[best])
}

func (e *Executor) execReadResourceData(ctx context.Context, actor models.AgentConfig, in flowDataReadInput) (any, error) {
	runID := runtimecorrelation.RunIDFromContext(ctx)
	if runID == "" {
		return nil, fmt.Errorf("read_flow_data requires run context")
	}
	if in.Declaration == nil || in.Declaration.Validate() != nil {
		return nil, fmt.Errorf("read_flow_data resource arm requires one structured declaration")
	}
	e.mu.RLock()
	source := e.workflowSource
	store := e.dataAccessStore
	e.mu.RUnlock()
	allowed := flowdata.AllowedResourceData(source, actor)
	authorized := false
	for _, declaration := range allowed {
		if declaration == *in.Declaration {
			authorized = true
			break
		}
	}
	if !authorized {
		return nil, durabledata.NewDomainError(durabledata.CodeAccessDenied, "resource declaration %s is not admitted for actor %s", in.Declaration.Key(), actor.ID)
	}
	if store == nil {
		return nil, fmt.Errorf("read_flow_data durable selected-store reader is required")
	}
	items, err := store.LoadRunResourceAccess(ctx, runID, []durabledata.DeclarationRef{*in.Declaration})
	if err != nil {
		return nil, err
	}
	if len(items) != 1 || items[0].Declaration != *in.Declaration {
		return nil, durabledata.NewDomainError(durabledata.CodeIntegrity, "resource access projection is incomplete")
	}
	item := items[0]
	var schema map[string]any
	if err := json.Unmarshal(item.Schema, &schema); err != nil {
		return nil, durabledata.NewDomainError(durabledata.CodeIntegrity, "resource %s persisted schema is invalid", item.Declaration.Key())
	}
	compiled, defects := durabledata.CompileJSONL(item.Declaration, schema, item.BusinessKey, item.Content)
	if len(defects) != 0 || compiled.VersionID != item.VersionID {
		return nil, durabledata.NewDomainError(durabledata.CodeIntegrity, "resource %s persisted payload is contradictory", item.Declaration.Key())
	}
	rows := make([]map[string]any, len(compiled.Rows))
	for index, row := range compiled.Rows {
		var value any
		if err := json.Unmarshal(row.Canonical, &value); err != nil {
			return nil, durabledata.NewDomainError(durabledata.CodeIntegrity, "resource %s row %d is invalid", item.Declaration.Key(), row.Ordinal)
		}
		projected := map[string]any{
			"declaration": item.Declaration, "version_id": item.VersionID,
			"ordinal": row.Ordinal, "value": value,
		}
		if row.BusinessKey != "" {
			key, err := row.BusinessKey.Value()
			if err != nil {
				return nil, durabledata.NewDomainError(durabledata.CodeIntegrity, "resource %s row %d has invalid business key", item.Declaration.Key(), row.Ordinal)
			}
			projected["key"] = key
		}
		rows[index] = projected
	}
	if in.Kind == "resource_row" {
		if item.BusinessKey == "" {
			if in.Position == 0 || in.Key != nil {
				return nil, fmt.Errorf("read_flow_data keyless resource_row requires position only")
			}
			for index, row := range compiled.Rows {
				if row.Ordinal == in.Position {
					return inlineResourceRowResult(item, rows[index])
				}
			}
			return nil, durabledata.NewDomainError(durabledata.CodeVersionMissing, "resource row position %d does not exist in version %s", in.Position, item.VersionID)
		}
		if in.Position != 0 || in.Key == nil {
			return nil, fmt.Errorf("read_flow_data keyed resource_row requires key only")
		}
		key, err := durabledata.BusinessKeyFromValue(in.Key)
		if err != nil {
			return nil, fmt.Errorf("read_flow_data resource_row key: %w", err)
		}
		for index, row := range compiled.Rows {
			if row.BusinessKey == key {
				return inlineResourceRowResult(item, rows[index])
			}
		}
		return nil, durabledata.NewDomainError(durabledata.CodeVersionMissing, "resource key %s does not exist in version %s", key, item.VersionID)
	}
	if in.Page == nil {
		return nil, fmt.Errorf("read_flow_data resource_rows requires page")
	}
	page, err := in.Page.WithDefaults()
	if err != nil {
		return nil, err
	}
	if page.ByteLimit > durabledata.MaxToolPageBytes {
		return nil, fmt.Errorf("read_flow_data resource_rows page byte_limit exceeds %d", durabledata.MaxToolPageBytes)
	}
	fingerprint, err := actor.Identity.Fingerprint()
	if err != nil {
		return nil, fmt.Errorf("read_flow_data resource cursor requires canonical actor identity: %w", err)
	}
	targetFingerprint, err := resourceDataTargetFingerprint(item.Declaration, item.VersionID)
	if err != nil {
		return nil, fmt.Errorf("read_flow_data resource cursor requires canonical target identity: %w", err)
	}
	offset := 0
	if page.Cursor != "" {
		cursor, err := decodeResourceDataCursor(page.Cursor)
		if err != nil {
			return nil, err
		}
		if cursor.Version != "swarm.flow-data.resource-cursor.v2" || cursor.RunID != runID || cursor.ActorFingerprint != fingerprint ||
			cursor.TargetFingerprint != targetFingerprint || cursor.Offset < 1 || cursor.Offset >= len(rows) {
			return nil, fmt.Errorf("read_flow_data cursor does not match the selected run, actor, declaration, version, or offset")
		}
		offset = cursor.Offset
	}
	buildResult := func(selected []map[string]any, next int) (map[string]any, error) {
		encodedItems, marshalErr := json.Marshal(selected)
		if marshalErr != nil {
			return nil, marshalErr
		}
		continuation := durabledata.EndContinuation()
		if next < len(rows) {
			cursor, encodeErr := encodeResourceDataCursor(resourceDataCursor{
				Version: "swarm.flow-data.resource-cursor.v2", RunID: runID, ActorFingerprint: fingerprint,
				TargetFingerprint: targetFingerprint, Offset: next,
			})
			if encodeErr != nil {
				return nil, encodeErr
			}
			continuation = durabledata.PageContinuation{State: "more", Cursor: cursor}
		}
		return map[string]any{
			"kind": "resource_rows", "declaration": item.Declaration, "version_id": item.VersionID,
			"rows": durabledata.PageResult[map[string]any]{
				Items: selected, ItemCount: len(selected), EncodedItemsBytes: len(encodedItems), Continuation: continuation,
			},
		}, nil
	}
	selected := make([]map[string]any, 0, page.Limit)
	for index := offset; index < len(rows) && len(selected) < page.Limit; index++ {
		candidate := append(append([]map[string]any(nil), selected...), rows[index])
		encodedItems, marshalErr := json.Marshal(candidate)
		if marshalErr != nil {
			return nil, marshalErr
		}
		if len(encodedItems) > page.ByteLimit {
			if len(selected) == 0 {
				return nil, fmt.Errorf("read_flow_data resource row exceeds tool page byte_limit")
			}
			break
		}
		candidateResult, buildErr := buildResult(candidate, index+1)
		if buildErr != nil {
			return nil, buildErr
		}
		encodedResult, marshalErr := json.Marshal(candidateResult)
		if marshalErr != nil {
			return nil, marshalErr
		}
		if len(encodedResult) > toolresultpolicy.MaxInlineToolResultBytes {
			if len(selected) == 0 {
				return nil, fmt.Errorf("read_flow_data resource row exceeds inline tool result byte limit %d", toolresultpolicy.MaxInlineToolResultBytes)
			}
			break
		}
		selected = candidate
	}
	next := offset + len(selected)
	result, err := buildResult(selected, next)
	if err != nil {
		return nil, err
	}
	encodedItems, err := json.Marshal(selected)
	if err != nil {
		return nil, err
	}
	if len(encodedItems) > page.ByteLimit {
		return nil, fmt.Errorf("read_flow_data resource page exceeds tool page byte_limit")
	}
	encodedResult, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(encodedResult) > toolresultpolicy.MaxInlineToolResultBytes {
		return nil, fmt.Errorf("read_flow_data resource page exceeds inline tool result byte limit %d", toolresultpolicy.MaxInlineToolResultBytes)
	}
	return result, nil
}

func inlineResourceRowResult(item durabledata.ResourceAccessItem, row map[string]any) (any, error) {
	result := map[string]any{"kind": "resource_row", "declaration": item.Declaration, "version_id": item.VersionID, "row": row}
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(raw) > toolresultpolicy.MaxInlineToolResultBytes {
		return nil, fmt.Errorf("read_flow_data resource row exceeds inline tool result byte limit %d", toolresultpolicy.MaxInlineToolResultBytes)
	}
	return result, nil
}

func exactFlowDataInput(input any, allowed ...string) error {
	object, ok := input.(map[string]any)
	if !ok {
		return fmt.Errorf("read_flow_data input must be an object")
	}
	set := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		set[key] = struct{}{}
	}
	for key := range object {
		if _, ok := set[key]; !ok {
			return fmt.Errorf("read_flow_data field %s is not allowed for selected kind", key)
		}
	}
	return nil
}

func encodeResourceDataCursor(cursor resourceDataCursor) (string, error) {
	cursorRaw, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(append([]byte("swarm.flow-data.resource-cursor.proof.v2\x00"), cursorRaw...))
	raw, err := json.Marshal(resourceDataCursorEnvelope{Cursor: cursor, Checksum: hex.EncodeToString(hash[:])})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeResourceDataCursor(encoded string) (resourceDataCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(raw) > durabledata.MaxBusinessKeyBytes {
		return resourceDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	var envelope resourceDataCursorEnvelope
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return resourceDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return resourceDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	cursorRaw, err := json.Marshal(envelope.Cursor)
	if err != nil {
		return resourceDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	hash := sha256.Sum256(append([]byte("swarm.flow-data.resource-cursor.proof.v2\x00"), cursorRaw...))
	if envelope.Checksum != hex.EncodeToString(hash[:]) {
		return resourceDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	return envelope.Cursor, nil
}

func resourceDataTargetFingerprint(declaration durabledata.DeclarationRef, version durabledata.VersionID) (string, error) {
	identity, err := json.Marshal(struct {
		Declaration durabledata.DeclarationRef `json:"declaration"`
		Version     durabledata.VersionID      `json:"version"`
	}{Declaration: declaration, Version: version})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(append([]byte("swarm.flow-data.resource-target.v1\x00"), identity...))
	return "sha256:" + hex.EncodeToString(hash[:]), nil
}

func encodeFlowDataCursor(cursor flowDataCursor) (string, error) {
	cursorRaw, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode read_flow_data cursor: %w", err)
	}
	hash := sha256.Sum256(append([]byte("swarm.flow-data.cursor.proof.v1\x00"), cursorRaw...))
	raw, err := json.Marshal(flowDataCursorEnvelope{Cursor: cursor, Checksum: hex.EncodeToString(hash[:])})
	if err != nil {
		return "", fmt.Errorf("encode read_flow_data cursor envelope: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeFlowDataCursor(encoded string) (flowDataCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return flowDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	if len(raw) > 4<<10 {
		return flowDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	var envelope flowDataCursorEnvelope
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return flowDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return flowDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	cursorRaw, err := json.Marshal(envelope.Cursor)
	if err != nil {
		return flowDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	hash := sha256.Sum256(append([]byte("swarm.flow-data.cursor.proof.v1\x00"), cursorRaw...))
	if envelope.Checksum != hex.EncodeToString(hash[:]) {
		return flowDataCursor{}, fmt.Errorf("read_flow_data cursor is invalid")
	}
	return envelope.Cursor, nil
}
