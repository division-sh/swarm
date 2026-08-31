package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const collectionItemSemanticsCheckID = "collection_item_semantics"

// checkCollectionItemSemantics admits every executable collection source and
// non-CEL selector even when no condition happens to read item.
func checkCollectionItemSemantics(c *checkerContext) []Finding {
	bundle, ok := semanticview.Bundle(c.source)
	if !ok || bundle == nil {
		return []Finding{NewHardInvalidityFinding(collectionItemSemanticsCheckID, "global", "collection semantics require a loaded contract bundle", "load the exact workflow contract bundle")}
	}
	var findings []Finding
	for _, record := range wave1ScopedNodeRecords(c.source) {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		for eventType, handler := range record.Entry.EventHandlers {
			location := node.Key()
			add := func(kind string, err error) {
				if err == nil {
					return
				}
				findings = append(findings, NewHardInvalidityFinding(
					collectionItemSemanticsCheckID,
					location,
					fmt.Sprintf("node %s handler %s %s: %v", node.Key(), strings.TrimSpace(eventType), kind, err),
					"declare one exact collection source and schema-valid item fields",
				))
			}
			if handler.Query != nil {
				_, err := bundle.ResolveHandlerQueryCollectionPlan(node, eventType, handler)
				add("query", err)
			}
			if handler.GroupBy != nil {
				_, err := bundle.ResolveHandlerGroupByCollectionPlan(node, eventType, handler)
				add("group_by", err)
			}
			if handler.Filter != nil {
				_, err := bundle.ResolveHandlerCollectionItemType(node, eventType, handler, collectionSource(handler.Filter))
				add("filter", err)
			}
			if handler.Reduce != nil {
				_, err := bundle.ResolveHandlerCollectionItemType(node, eventType, handler, collectionSource(handler.Reduce))
				add("reduce", err)
			}
			if handler.Count != nil {
				_, err := bundle.ResolveHandlerCollectionItemType(node, eventType, handler, collectionSource(handler.Count))
				add("count", err)
			}
		}
	}
	return findings
}

func collectionSource(spec any) string {
	switch value := spec.(type) {
	case *runtimecontracts.FilterSpec:
		if value != nil {
			return firstCollectionSource(value.ItemsFrom, value.Source)
		}
	case *runtimecontracts.ReduceSpec:
		if value != nil {
			return firstCollectionSource(value.ItemsFrom, value.Source)
		}
	case *runtimecontracts.CountSpec:
		if value != nil {
			return firstCollectionSource(value.ItemsFrom, value.Source)
		}
	}
	return ""
}

func firstCollectionSource(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
