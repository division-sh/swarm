package contracts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestRuleEmitPresentationKeepsAuthoredSpellingAndExactSite(t *testing.T) {
	for _, tc := range []struct {
		context, predicate, fallback string
		labels                       [2]string
	}{
		{"rules", "when: TRUE", "", [2]string{"rules[0]: when TRUE", "rules[1]: default"}},
		{"on_complete", "condition: TRUE", "", [2]string{"on_complete[0]: condition TRUE", "on_complete[1]"}},
	} {
		t.Run(tc.context, func(t *testing.T) {
			var handler SystemNodeEventHandler
			source := tc.context + ":\n  - " + tc.predicate + "\n    emit: chosen\n  - " + tc.fallback + "emit: fallback\n"
			if err := decodeNodeTestYAML([]byte(source), &handler); err != nil {
				t.Fatal(err)
			}
			node := identitytest.FlowNode(t, "child", "worker")
			qualified, err := QualifySystemNodeHandlerRuleRefsForEvent(node, "request", handler)
			if err != nil {
				t.Fatal(err)
			}
			sites := HandlerDeclarativeEmitSites(qualified, nil)
			if len(sites) != 2 {
				t.Fatalf("emit sites = %#v, want both original outcomes", sites)
			}
			for i, site := range sites {
				wantKey := fmt.Sprintf("handler.%s[%d].emit", tc.context, i)
				wantPath := fmt.Sprintf(`nodes["worker"].handlers["request"].%s[%d]`, tc.context, i)
				if site.RuleDisplay != tc.labels[i] || site.Source != "handler."+tc.context+".emit" || site.SiteKey != wantKey || site.RuleIndex != i || site.RuleRef.Flow().String() != "child" || site.RuleRef.SemanticPath() != wantPath {
					t.Fatalf("emit[%d] lost authored display or exact source coordinates: %#v", i, site)
				}
				wantEvent := []string{"chosen", "fallback"}[i]
				if site.Spec.EventType() != wantEvent {
					t.Fatalf("display changed outcome[%d]: %#v", i, site.Spec)
				}
			}
		})
	}
}

func TestRuleEmitTemplatePresentationKeepsMergedFieldsAndPosition(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := decodeNodeTestYAML([]byte(`
emit:
  event: chosen
  fields: {request: payload.request}
rules:
  - when: TRUE
    emit: {fields: {score: payload.score}}
  - emit: {fields: {score: 0}}
`), &handler); err != nil {
		t.Fatal(err)
	}
	qualified, err := QualifySystemNodeHandlerRuleRefsForEvent(identitytest.RootNode(t, "worker"), "request", handler)
	if err != nil {
		t.Fatal(err)
	}
	sites := HandlerDeclarativeEmitSites(qualified, nil)
	if len(sites) != 2 {
		t.Fatalf("template sites = %#v, want both effective branches", sites)
	}
	for i, wantDisplay := range []string{"rules[0]: when TRUE", "rules[1]: default"} {
		ref, _ := qualified.Rules[i].DeclarationIdentity()
		wantFields := map[string]ExpressionValue{"request": handler.Emit.Fields["request"], "score": handler.Rules[i].Emit.Fields["score"]}
		if site := sites[i]; site.RuleDisplay != wantDisplay || site.Source != "handler.rules.emit_template" || site.SiteKey != fmt.Sprintf("handler.rules[%d].emit_template", i) || site.RuleIndex != i || site.RuleRef != ref || site.Spec.EventType() != "chosen" || !reflect.DeepEqual(site.Spec.Fields, wantFields) {
			t.Fatalf("template branch[%d] changed display, identity or effective fields: %#v", i, site)
		}
	}
}

func TestRuleFanOutPresentationKeepsCompiledSiteAndOperands(t *testing.T) {
	for _, tc := range []struct {
		context string
		kind    FanOutSiteKind
		label   string
	}{
		{"rules", FanOutSiteRule, "rules[0]: when true"},
		{"on_complete", FanOutSiteOnComplete, "on_complete[0]: condition true"},
	} {
		t.Run(tc.context, func(t *testing.T) {
			predicate := "when"
			if tc.context == "on_complete" {
				predicate = "condition"
			}
			var handler SystemNodeEventHandler
			source := fmt.Sprintf(`%s:
  - %s: true
    fan_out:
      items_from: payload.items
      as: row_item
      identity: row_item.id
      emit: {event: item.requested, fields: {id: row_item.id}}
`, tc.context, predicate)
			if err := decodeNodeTestYAML([]byte(source), &handler); err != nil {
				t.Fatal(err)
			}
			var row HandlerRuleEntry
			if tc.context == "rules" {
				row = handler.Rules[0]
			} else {
				row = handler.OnComplete[0]
			}
			wantSpec, err := json.Marshal(row.FanOut)
			if err != nil {
				t.Fatal(err)
			}
			node := identitytest.RootNode(t, "dispatcher")
			qualified, err := QualifySystemNodeHandlerRuleRefsForEvent(node, "batch.ready", handler)
			if err != nil {
				t.Fatal(err)
			}
			bundle := fanOutPlanRegistryTestBundle(t, qualified)
			if failures := bundle.PrepareFanOutPlans(); len(failures) != 0 {
				t.Fatalf("compile original fan-out: %#v", failures)
			}
			plans := bundle.FanOutPlansForHandler(node, "batch.ready")
			fanOutSites := HandlerFanOutSites(qualified)
			if len(plans) != 1 || len(fanOutSites) != 1 {
				t.Fatalf("fan-out census changed: plans=%#v sites=%#v", plans, fanOutSites)
			}
			gotSpec, err := json.Marshal(fanOutSites[0].Spec)
			if err != nil {
				t.Fatal(err)
			}
			if site := fanOutSites[0]; site.Source != "handler."+tc.label+".fan_out" || site.Kind != tc.kind || site.Index != 0 || !bytes.Equal(gotSpec, wantSpec) {
				t.Fatalf("fan-out diagnostic changed execution inputs: %#v", site)
			}
			fanOutRef, valid := fanOutSites[0].Spec.DeclarationIdentity()
			compiledRef, err := plans[0].Ref.ElementRef.DeclarationIdentity()
			if err != nil || !valid || fanOutRef != compiledRef {
				t.Fatalf("fan-out qualification/compilation lost its exact identity: site=%#v plan=%#v err=%v", fanOutRef, compiledRef, err)
			}
			emits := HandlerDeclarativeEmitSites(qualified, plans)
			if len(emits) != 1 {
				t.Fatalf("fan-out emit census changed: %#v", emits)
			}
			if site := emits[0]; site.RuleDisplay != tc.label || site.Source != "handler."+tc.context+".fan_out.emit" || site.SiteKey != "handler."+tc.context+"[0].fan_out.emit" || site.RuleIndex != 0 || !site.RuleRef.Valid() || site.RuleRef.SemanticPath() != `nodes["dispatcher"].handlers["batch.ready"].`+tc.context+"[0]" || site.ItemAlias != "row_item" || !reflect.DeepEqual(site.Spec, plans[0].Emit) {
				t.Fatalf("fan-out display changed exact emitted site/plan: %#v", site)
			}
		})
	}
}
