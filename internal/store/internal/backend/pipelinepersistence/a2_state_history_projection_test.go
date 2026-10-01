package pipelinepersistence

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestA2WorkflowHistoryProjectionPreservesRetainedNumbers(t *testing.T) {
	raw := json.RawMessage(`{"items":[{"integer":9007199254740991,"double":1.0,"exponent":1e0,"fraction":0.25}]}`)
	var retained map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &retained); err != nil {
		t.Fatal(err)
	}
	before := mutationlog.EntityStateProjection{
		CurrentState: "collecting", Fields: retained, Bookkeeping: retained,
		Gates: map[string]any{"ready": true}, Accumulator: retained,
	}
	after, err := workflowEngineStateProjection(pipeline.WorkflowEngineStateRecord{
		CurrentState: "collecting", Fields: raw, Bookkeeping: raw,
		Gates: json.RawMessage(`{"ready":true}`), Accumulator: raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Errorf("history projection changed retained numeric evidence: before=%#v after=%#v", before, after)
	}
	records, err := mutationlog.BuildEntityStateDiffRecords("entity", before, after, mutationlog.Writer{Type: "platform", ID: "workflow_engine"})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("unchanged retained state drafted %d false mutations: %#v", len(records), records)
	}
}

func TestA2WorkflowHistoryProjectionRejectsMalformedEvidence(t *testing.T) {
	for _, raw := range []string{`{"value":1,"value":2}`, `{"value":-0}`, `{"value":9007199254740993}`, `{"value":1} {}`, `[1,2]`} {
		t.Run(raw, func(t *testing.T) {
			for _, bucket := range []string{"fields", "bookkeeping", "gates", "accumulator"} {
				record := pipeline.WorkflowEngineStateRecord{Fields: []byte(`{}`), Bookkeeping: []byte(`{}`), Gates: []byte(`{}`), Accumulator: []byte(`{}`)}
				switch bucket {
				case "fields":
					record.Fields = []byte(raw)
				case "bookkeeping":
					record.Bookkeeping = []byte(raw)
				case "gates":
					record.Gates = []byte(raw)
				case "accumulator":
					record.Accumulator = []byte(raw)
				}
				if _, err := workflowEngineStateProjection(record); err == nil {
					t.Errorf("%s accepted malformed historical evidence: %s", bucket, raw)
				}
			}
		})
	}
}
