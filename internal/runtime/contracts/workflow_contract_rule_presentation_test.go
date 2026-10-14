package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestHandlerRulePresentationCapturesAuthoredPredicateOnce(t *testing.T) {
	for _, tc := range []struct{ name, row, want string }{
		{"when", "when: payload.score > 4", "when payload.score > 4"},
		{"block predicate", "when: |-\n      payload.score > 4 &&\n      payload.ready", "when payload.score > 4 &&\npayload.ready"},
		{"boolean spelling", "when: TRUE", "when TRUE"},
		{"case", "case: {selector: payload.kind, equals: 'gold'}", "case {selector: payload.kind, equals: 'gold'}"},
		{"tuple case", `case: {selectors: [payload.kind, payload.ready], equals: ["gold", true]}`, `case {selectors: [payload.kind, payload.ready], equals: ["gold", true]}`},
		{"range", "range: {value: payload.score, gte: 2, lt: policy.limit, monotonicity: ['2 <= policy.limit']}", "range {value: payload.score, gte: 2, lt: policy.limit, monotonicity: ['2 <= policy.limit']}"},
		{"trailing default", "emit: done", "default"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var handler SystemNodeEventHandler
			source := "rules:\n  - " + tc.row + "\n"
			if tc.want != "default" {
				source += "  - emit: fallback\n"
			}
			if err := decodeNodeTestYAML([]byte(source), &handler); err != nil {
				t.Fatal(err)
			}
			want := "rules[0]"
			if tc.want != "" {
				want += ": " + tc.want
			}
			if got := handler.Rules[0].DisplayLabel("rules", 0); got != want {
				t.Fatalf("display = %q, want %q", got, want)
			}
			if tc.want != "" {
				handler.Rules[0].Condition = "replaced_lowered_expression"
				if got := handler.Rules[0].DisplayLabel("rules", 0); got != want {
					t.Fatalf("presentation reconstructed from execution CEL: %q", got)
				}
			}
		})
	}
}

func TestHandlerRulePresentationKeepsContextIdentityAndFullTextSeparate(t *testing.T) {
	long := strings.Repeat("payload.ready && ", 80) + "payload.ready"
	var handler SystemNodeEventHandler
	if err := decodeNodeTestYAML([]byte("on_complete:\n  - condition: "+long+"\n    emit: done\n"), &handler); err != nil {
		t.Fatal(err)
	}
	node := identitytest.RootNode(t, "worker")
	qualified, err := QualifySystemNodeHandlerRuleRefsForEvent(node, "request", handler)
	if err != nil {
		t.Fatal(err)
	}
	row := qualified.OnComplete[0]
	ref, ok := row.DeclarationIdentity()
	if !ok || ref.SemanticPath() != `nodes["worker"].handlers["request"].on_complete[0]` {
		t.Fatalf("positional identity = %#v, valid=%t", ref, ok)
	}
	if got := row.DisplayLabel("on_complete", 0); got != "on_complete[0]: condition "+long {
		t.Fatalf("full completion text lost: %q", got)
	}
	row.Activity.ID = "separate-activity-owner"
	if got := row.DisplayLabel("on_complete", 0); got != "on_complete[0]: condition "+long {
		t.Fatalf("activity identity changed derived predicate presentation: %q", got)
	}
	if got, valid := row.DeclarationIdentity(); !valid || got != ref {
		t.Fatal("presentation altered declaration identity")
	}
	for _, kind := range []PolicySheetRowKind{PolicySheetRowKindLookup, PolicySheetRowKindValidate, PolicySheetRowKindModule} {
		row := HandlerRuleEntry{PolicyRow: PolicySheetRowMetadata{Kind: kind}}
		if got := row.DisplayLabel("rules", 3); got != "rules[3]: "+string(kind) {
			t.Fatalf("value row invented selection semantics: %q", got)
		}
	}
	if got := (HandlerRuleEntry{}).DisplayLabel("rules", 2); got != "rules[2]" {
		t.Fatalf("predicate-less typed row invented else semantics: %q", got)
	}
}

func TestHandlerRulePresentationPreservesResolvedAliasAndMergeText(t *testing.T) {
	for _, body := range []string{
		"_note: &predicate {selector: payload.kind, equals: 'gold'}\nrules: [{case: *predicate}, {emit: fallback}]\n",
		"_note: &predicate {selector: payload.kind, equals: 'gold'}\nrules: [{case: {<<: *predicate}}, {emit: fallback}]\n",
	} {
		var handler SystemNodeEventHandler
		if err := decodeNodeTestYAML([]byte(body), &handler); err != nil {
			t.Fatal(err)
		}
		if got := handler.Rules[0].DisplayLabel("rules", 0); got != "rules[0]: case {selector: payload.kind, equals: 'gold'}" {
			t.Fatalf("resolved predicate lost its admitted text/style: %q", got)
		}
	}
}
