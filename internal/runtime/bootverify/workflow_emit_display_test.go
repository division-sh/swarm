package bootverify

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestEmitCompletenessConsumesSharedRuleDisplayAndKeepsSingularJoins(t *testing.T) {
	node := identitytest.RootNode(t, "worker")
	for _, tc := range []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
		labels  []string
	}{
		{
			name: "rules",
			handler: runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{
				{Condition: "payload.ready", Emit: runtimecontracts.EmitSpec{Event: "chosen"}},
				{Condition: "else", Emit: runtimecontracts.EmitSpec{Event: "fallback"}},
			}},
			labels: []string{"rules[0]: when payload.ready.emit", "rules[1]: else.emit"},
		},
		{
			name: "completion",
			handler: runtimecontracts.SystemNodeEventHandler{OnComplete: []runtimecontracts.HandlerRuleEntry{
				{Condition: "payload.ready", Emit: runtimecontracts.EmitSpec{Event: "chosen"}},
				{Emit: runtimecontracts.EmitSpec{Event: "fallback"}},
			}},
			labels: []string{"on_complete[0]: condition payload.ready.emit", "on_complete[1].emit"},
		},
		{
			name: "join outcomes",
			handler: runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{
				ID:         "membership-owner",
				OnComplete: runtimecontracts.HandlerRuleEntry{Emit: runtimecontracts.EmitSpec{Event: "chosen"}},
				OnDeadline: runtimecontracts.HandlerRuleEntry{Emit: runtimecontracts.EmitSpec{Event: "fallback"}},
			}},
			labels: []string{"join.on_complete.emit", "join.on_deadline.emit"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, err := runtimecontracts.QualifySystemNodeHandlerRuleRefsForEvent(node, "request", tc.handler)
			if err != nil {
				t.Fatal(err)
			}
			sites := runtimecontracts.HandlerDeclarativeEmitSites(handler, nil)
			if len(sites) != len(tc.labels) {
				t.Fatalf("site census changed: %#v", sites)
			}
			for i, site := range sites {
				if got := payloadCompletenessDeclarativeSiteLabel(site); got != tc.labels[i] || !site.RuleRef.Valid() || site.Spec.EventType() != []string{"chosen", "fallback"}[i] {
					t.Fatalf("site[%d] lost display, exact identity or original output: %#v display=%q", i, site, got)
				}
			}
			if handler.Join != nil && handler.Join.EffectiveID() != "membership-owner" {
				t.Fatalf("outcome presentation changed join membership identity: %#v", handler.Join)
			}
		})
	}
}
