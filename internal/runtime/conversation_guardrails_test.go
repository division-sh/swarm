package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type panicToolExec struct{}

func (panicToolExec) Execute(context.Context, string, any) (any, error) {
	panic("boom")
}

type largeToolExec struct {
	payload any
}

func (l largeToolExec) Execute(context.Context, string, any) (any, error) {
	return l.payload, nil
}

func TestConversation_ExecuteToolCalls_RecoversPanic(t *testing.T) {
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 4, &fakeRuntime{})
	c.SetToolExecutor(panicToolExec{})

	raw := c.executeToolCalls(context.Background(), []ToolCall{{Name: "sql_execute", Arguments: map[string]any{"query": "select 1"}}})
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		t.Fatalf("unmarshal tool payload: %v", err)
	}
	if len(arr) != 1 || arr[0]["ok"] != false {
		t.Fatalf("expected panic to be captured as error payload, got %#v", arr)
	}
	if !strings.Contains(strings.ToLower(asString(arr[0]["error"])), "tool panic") {
		t.Fatalf("expected tool panic text, got %#v", arr[0]["error"])
	}
}

func TestConversation_ExecuteToolCalls_TruncatesLargeResult(t *testing.T) {
	huge := strings.Repeat("x", maxToolResultBytes+1024)
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 4, &fakeRuntime{})
	c.SetToolExecutor(largeToolExec{payload: map[string]any{"blob": huge}})

	raw := c.executeToolCalls(context.Background(), []ToolCall{{Name: "sql_execute", Arguments: map[string]any{"query": "select 1"}}})
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		t.Fatalf("unmarshal tool payload: %v", err)
	}
	if len(arr) != 1 || arr[0]["ok"] != true {
		t.Fatalf("expected successful tool payload, got %#v", arr)
	}
	resultMap, _ := arr[0]["result"].(map[string]any)
	if resultMap == nil || resultMap["truncated"] != true {
		t.Fatalf("expected truncated result metadata, got %#v", arr[0]["result"])
	}
}
