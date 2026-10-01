package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
)

type resourceReadRecordingStore struct {
	items []durabledata.ResourceAccessItem
	err   error
	runs  []string
	refs  [][]durabledata.DeclarationRef
}

func (s *resourceReadRecordingStore) LoadRunResourceAccess(_ context.Context, run string, refs []durabledata.DeclarationRef) ([]durabledata.ResourceAccessItem, error) {
	s.runs = append(s.runs, run)
	s.refs = append(s.refs, append([]durabledata.DeclarationRef(nil), refs...))
	return s.items, s.err
}

func TestResourceReadCharacterization(t *testing.T) {
	source, _ := loadResourceDataToolSource(t)
	actor := flowDataActorWithIdentity(t, source, "characterization")
	ref := flowdata.AllowedResourceData(source, actor)[0]
	item := flowDataResourceAccessItem(t, ref, []string{"alpha", "beta", "gamma"})
	readFailure := errors.New("selected reader failed")
	for _, tc := range []struct {
		name  string
		input map[string]any
		items []durabledata.ResourceAccessItem
		err   error
		want  string
		calls int
	}{
		{"unknown_kind", map[string]any{"kind": "old"}, nil, nil, "invalid_tool_input", 0},
		{"retired_filename", map[string]any{"kind": "resource_row", "filename": "records"}, nil, nil, "invalid_tool_input", 0},
		{"crossed_arm", map[string]any{"kind": "resource_rows", "key": "row-00"}, nil, nil, "read_flow_data field key is not allowed for selected kind", 0},
		{"missing_declaration", map[string]any{"kind": "resource_row"}, nil, nil, "read_flow_data resource arm requires one structured declaration", 0},
		{"missing_store_record", map[string]any{"kind": "resource_row", "declaration": ref}, nil, nil, "resource access projection is incomplete", 1},
		{"duplicate_store_record", map[string]any{"kind": "resource_row", "declaration": ref}, []durabledata.ResourceAccessItem{item, item}, nil, "resource access projection is incomplete", 1},
		{"selected_read_error", map[string]any{"kind": "resource_row", "declaration": ref}, nil, readFailure, readFailure.Error(), 1},
		{"key_required", map[string]any{"kind": "resource_row", "declaration": ref}, []durabledata.ResourceAccessItem{item}, nil, "read_flow_data keyed resource_row requires key only", 1},
		{"key_and_position", map[string]any{"kind": "resource_row", "declaration": ref, "key": "row-00", "position": 1}, []durabledata.ResourceAccessItem{item}, nil, "read_flow_data keyed resource_row requires key only", 1},
		{"missing_key", map[string]any{"kind": "resource_row", "declaration": ref, "key": "absent"}, []durabledata.ResourceAccessItem{item}, nil, fmt.Sprintf("resource key %q does not exist in version %s", "absent", item.VersionID), 1},
		{"page_required", map[string]any{"kind": "resource_rows", "declaration": ref}, []durabledata.ResourceAccessItem{item}, nil, "read_flow_data resource_rows requires page", 1},
		{"page_limit_before_cursor", map[string]any{"kind": "resource_rows", "declaration": ref, "page": map[string]any{"byte_limit": durabledata.MaxToolPageBytes + 1, "cursor": "broken"}}, []durabledata.ResourceAccessItem{item}, nil, "invalid_tool_input", 0},
		{"malformed_cursor", map[string]any{"kind": "resource_rows", "declaration": ref, "page": map[string]any{"cursor": "!"}}, []durabledata.ResourceAccessItem{item}, nil, "read_flow_data cursor is invalid", 1},
		{"first_row_budget", map[string]any{"kind": "resource_rows", "declaration": ref, "page": map[string]any{"byte_limit": 1}}, []durabledata.ResourceAccessItem{item}, nil, "read_flow_data resource row exceeds tool page byte_limit", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &resourceReadRecordingStore{items: tc.items, err: tc.err}
			exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: store})
			result, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", tc.input)
			if err == nil || result != nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("result=%v error=%v; want %q", result, err, tc.want)
			}
			if len(store.runs) != tc.calls {
				t.Fatalf("selected reads=%v, want %d", store.runs, tc.calls)
			}
			if tc.calls == 1 && (store.runs[0] != "11111111-1111-4111-8111-111111111111" || !reflect.DeepEqual(store.refs[0], []durabledata.DeclarationRef{ref})) {
				t.Fatalf("read authority runs=%v refs=%v", store.runs, store.refs)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("selected-store cause lost: %v", err)
			}
		})
	}
	for _, corruption := range []string{"declaration", "schema", "version", "content", "noncanonical"} {
		t.Run(corruption, func(t *testing.T) {
			bad := item
			switch corruption {
			case "declaration":
				bad.Declaration.EventName = "support/other"
			case "schema":
				bad.Schema = []byte("!")
			case "version":
				bad.VersionID = "resource-v1:sha256:wrong"
			case "content":
				bad.Content = []byte("not json\n")
			case "noncanonical":
				bad.Content = []byte(strings.Replace(string(item.Content), `"alpha"`, `"\u0061lpha"`, 1))
			}
			store := &resourceReadRecordingStore{items: []durabledata.ResourceAccessItem{bad}}
			exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: store})
			result, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"kind": "resource_row", "declaration": ref, "key": "row-00"})
			var domain *durabledata.DomainError
			if result != nil || !errors.As(err, &domain) || domain.Code != durabledata.CodeIntegrity || len(store.runs) != 1 {
				t.Fatalf("corrupt projection=%v error=%v reads=%v", result, err, store.runs)
			}
		})
	}
	store := &resourceReadRecordingStore{items: []durabledata.ResourceAccessItem{item}}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: store})
	input := map[string]any{"kind": "resource_rows", "declaration": ref, "page": map[string]any{"limit": 1}}
	var cursor string
	for index, payload := range []string{"alpha", "beta", "gamma"} {
		result, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", input)
		if err != nil {
			t.Fatal(err)
		}
		page := result.(map[string]any)["rows"].(durabledata.PageResult[map[string]any])
		value := page.Items[0]["value"].(map[string]any)
		encoded, _ := json.Marshal(page.Items)
		if page.ItemCount != 1 || page.EncodedItemsBytes != len(encoded) || page.Items[0]["ordinal"] != uint64(index+1) || value["payload"] != payload || page.Items[0]["key"] != fmt.Sprintf("row-%02d", index) {
			t.Fatalf("page %d=%+v", index, page)
		}
		if index == 2 {
			if page.Continuation.State != "end" || page.Continuation.Cursor != "" {
				t.Fatalf("terminal continuation=%+v", page.Continuation)
			}
			break
		}
		if page.Continuation.State != "more" {
			t.Fatalf("nonterminal continuation=%+v", page.Continuation)
		}
		cursor = page.Continuation.Cursor
		input["page"] = map[string]any{"limit": 1, "cursor": cursor}
	}
	for _, coordinate := range []string{"run", "actor", "target", "version", "zero", "end", "negative"} {
		t.Run("cursor_"+coordinate, func(t *testing.T) {
			decoded, err := decodeResourceDataCursor(cursor)
			if err != nil {
				t.Fatal(err)
			}
			switch coordinate {
			case "run":
				decoded.RunID = "22222222-2222-4222-8222-222222222222"
			case "actor":
				decoded.ActorFingerprint = "foreign"
			case "target":
				decoded.TargetFingerprint = "foreign"
			case "version":
				decoded.Version = "swarm.flow-data.resource-cursor.v1"
			case "zero":
				decoded.Offset = 0
			case "end":
				decoded.Offset = 3
			case "negative":
				decoded.Offset = -1
			}
			foreign, err := encodeResourceDataCursor(decoded)
			if err != nil {
				t.Fatal(err)
			}
			result, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"kind": "resource_rows", "declaration": ref, "page": map[string]any{"cursor": foreign}})
			if result != nil || err == nil || !strings.Contains(err.Error(), "cursor does not match the selected run, actor, declaration, version, or offset") {
				t.Fatalf("foreign cursor result=%v error=%v", result, err)
			}
		})
	}
}
