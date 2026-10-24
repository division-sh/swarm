package contracts

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

// Required matching payload fields lower once to explicit writes; optional
// fields never copy, and authored writes suppress a candidate before type checks.
func TestDataAccumulationPayloadLoweringPreservesExplicitWrites(t *testing.T) {
	for _, test := range []struct {
		name, source, want string
		targetType         string
		writes             []WorkflowDataWrite
	}{
		{name: "required only"},
		{name: "type mismatch", targetType: "number", want: "type mismatch"},
		{name: "explicit wins", targetType: "number", writes: []WorkflowDataWrite{{TargetField: "account_id", Value: LiteralExpression(42)}}},
		{name: "explicit child wins", targetType: "number", writes: []WorkflowDataWrite{{TargetPathRef: "state.account_id.part", Value: LiteralExpression(42)}}},
		{name: "source_event cannot switch schema", source: "account.bucketed", want: "source_event"},
	} {
		t.Run(test.name, func(t *testing.T) {
			bundle := emitFieldLoweringTestBundle()
			bundle.RootEntities["account"].Fields["computed_tier"] = EntityFieldDecl{Type: "number"}
			if test.targetType != "" {
				bundle.RootEntities["account"].Fields["account_id"] = EntityFieldDecl{Type: test.targetType}
			}
			ctx := EmitFieldLoweringContext{Node: identitytest.RootNode(t, "bucket-node"), TriggerEventType: "account.scored", Site: "handler.data_accumulation"}
			spec := WorkflowDataAccumulation{From: "payload", SourceEvent: test.source, Writes: test.writes}
			lowered, err := bundle.lowerDataAccumulation(ctx, spec)
			if test.want != "" {
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("lowering error = %v, want %s", err, test.want)
				}
				return
			}
			if err != nil || lowered.From != "" || len(lowered.Writes) != 1 {
				t.Fatalf("lowered writes = %#v, %v", lowered, err)
			}
			if len(test.writes) == 0 && (lowered.Writes[0].SourceField != "account_id" || lowered.Writes[0].TargetField != "account_id") {
				t.Fatalf("required matching field not lowered literally: %#v", lowered.Writes)
			}
			if len(test.writes) != 0 && !reflect.DeepEqual(lowered.Writes[0].Value, test.writes[0].Value) {
				t.Fatalf("authored write changed: %#v", lowered.Writes)
			}
			if spec.From != "payload" || len(spec.Writes) != len(test.writes) {
				t.Fatal("lowering mutated authored input")
			}
		})
	}
	for _, body := range []string{
		"worker:\n  account.scored:\n    data_accumulation: {from: payload}\n",
		"worker:\n  account.scored:\n    rules: [{data_accumulation: {from: payload}}]\n",
	} {
		var nodes map[string]SystemNodeContract
		if err := decodeNodeTestYAML([]byte(body), &nodes); err != nil {
			t.Fatalf("from-only effect rejected before lowering: %v", err)
		}
	}
	for _, value := range []string{"null", "entity", "[payload]"} {
		var nodes map[string]SystemNodeContract
		if err := decodeNodeTestYAML([]byte("worker:\n  account.scored:\n    data_accumulation: {from: "+value+"}\n"), &nodes); err == nil {
			t.Fatalf("invalid from %s admitted", value)
		}
	}
	bundle := emitFieldLoweringTestBundle()
	delete(bundle.RootEntities["account"].Fields, "account_id")
	_, err := bundle.compileHandlerWrites(identitytest.RootNode(t, "bucket-node"), "account.scored", SystemNodeEventHandler{
		Rules: []HandlerRuleEntry{{PolicyRow: PolicySheetRowMetadata{Kind: PolicySheetRowKindDefault}, DataAccumulation: WorkflowDataAccumulation{From: "payload"}}},
	})
	if err == nil || !strings.Contains(err.Error(), "empty after payload lowering") {
		t.Fatalf("zero-match default admitted: %v", err)
	}
	_, err = bundle.lowerDataAccumulation(EmitFieldLoweringContext{Node: identitytest.RootNode(t, "bucket-node")}, WorkflowDataAccumulation{From: "payload"})
	if err == nil || !strings.Contains(err.Error(), "join outcomes have no payload") {
		t.Fatalf("payload-less outcome admitted from: %v", err)
	}
}
