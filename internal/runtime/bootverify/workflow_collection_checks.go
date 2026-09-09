package bootverify

import (
	"fmt"
	"strings"

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
			if _, err := bundle.ResolveHandlerCollectionPlan(node, eventType, handler); err != nil {
				findings = append(findings, NewHardInvalidityFinding(
					collectionItemSemanticsCheckID,
					location,
					fmt.Sprintf("node %s handler %s collection dataflow: %v", node.Key(), strings.TrimSpace(eventType), err),
					"declare phase-ordered collection sources and non-overlapping output targets",
				))
			}
		}
	}
	return findings
}
