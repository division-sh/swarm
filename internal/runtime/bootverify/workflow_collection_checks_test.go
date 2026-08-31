package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCollectionItemSemanticsAdmitsEverySupportedSourceForm(t *testing.T) {
	tests := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
	}{
		{
			name: "direct and implicit intermediates",
			handler: runtimecontracts.SystemNodeEventHandler{
				Query:  &runtimecontracts.QuerySpec{Source: "payload.items", Select: []string{"id", "note"}},
				Filter: &runtimecontracts.FilterSpec{ItemsFrom: "computed.query", Condition: `has(item.note)`, StoreAs: ""},
				Count:  &runtimecontracts.CountSpec{ItemsFrom: "computed.filter"},
			},
		},
		{
			name:    "entity table",
			handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Entities: "items", Select: []string{"id", "status"}}},
		},
		{
			name:    "standalone grouping",
			handler: runtimecontracts.SystemNodeEventHandler{GroupBy: &runtimecontracts.GroupBySpec{ItemsFrom: "payload.items", Key: "status"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := Run(context.Background(), collectionItemSemanticsSource(tc.handler), Options{})
			for _, finding := range report.Errors() {
				if finding.CheckID == collectionItemSemanticsCheckID {
					t.Fatalf("collection finding = %#v", finding)
				}
			}
		})
	}
}

func TestCollectionItemSemanticsRejectsInvalidSourcesAndSelectors(t *testing.T) {
	tests := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
		want    string
	}{
		{name: "dual query source", handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Source: "payload.items", Entities: "items"}}, want: "exactly one collection source"},
		{name: "unknown query group", handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Source: "payload.items", GroupBy: "missing"}}, want: "undeclared item field missing"},
		{name: "optional query group", handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Source: "payload.items", GroupBy: "note"}}, want: "without a presence decision"},
		{name: "wrong-kind query group", handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Source: "payload.items", GroupBy: "tags"}}, want: "must be scalar"},
		{name: "unknown query selection", handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Source: "payload.items", Select: []string{"missing"}}}, want: "undeclared item field missing"},
		{name: "unknown standalone group", handler: runtimecontracts.SystemNodeEventHandler{GroupBy: &runtimecontracts.GroupBySpec{ItemsFrom: "payload.items", Key: "missing"}}, want: "undeclared item field missing"},
		{name: "optional standalone group", handler: runtimecontracts.SystemNodeEventHandler{GroupBy: &runtimecontracts.GroupBySpec{ItemsFrom: "payload.items", Key: "note"}}, want: "without a presence decision"},
		{name: "wrong-kind standalone group", handler: runtimecontracts.SystemNodeEventHandler{GroupBy: &runtimecontracts.GroupBySpec{ItemsFrom: "payload.items", Key: "tags"}}, want: "must be scalar"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := Run(context.Background(), collectionItemSemanticsSource(tc.handler), Options{})
			for _, finding := range report.Errors() {
				if finding.CheckID == collectionItemSemanticsCheckID && strings.Contains(finding.Message, tc.want) {
					return
				}
			}
			t.Fatalf("collection findings = %#v, want %q", report.Errors(), tc.want)
		})
	}
}

func collectionItemSemanticsSource(handler runtimecontracts.SystemNodeEventHandler) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"WorkItem": {Fields: map[string]runtimecontracts.TypeFieldSpec{
				"id": {Type: "text"}, "status": {Type: "text"},
				"note": {Type: "text", IsOptional: true}, "tags": {Type: "[text]"},
			}},
		}},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"items": {Fields: map[string]runtimecontracts.EntityFieldDecl{"id": {Type: "text"}, "status": {Type: "text"}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"work.received": {Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{"items": {Type: "[WorkItem]"}}, Required: []string{"items"},
			}},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {ID: "worker", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.received": handler}},
		},
	})
}
