package contracts

import (
	"fmt"
	"sort"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func multipleEventOwnersError(bundle *WorkflowContractBundle, event string, owners []string) error {
	display := make([]string, 0, len(owners))
	for _, owner := range normalizeStrings(owners) {
		node, err := runtimeidentity.ParseExecutableNodeKey(owner)
		if err != nil {
			display = append(display, fmt.Sprintf("node %q: location unavailable", owner))
			continue
		}
		locations := eventOwnerDeclarationLocations(bundle, node, event)
		if len(locations) == 0 {
			locations = []string{"location unavailable"}
		}
		display = append(display, fmt.Sprintf("node %q (scope %q): %s", node.NodeID(), node.FlowPath(), strings.Join(locations, "; ")))
	}
	sort.Strings(display)
	return fmt.Errorf("%w: event %s has multiple authoritative system node owners: %s; an event must have one owning system node; use one router node with an ordered rules: list whose selected branch emits distinct events to the other nodes", ErrMultipleAuthoritativeOwners, strings.TrimSpace(event), strings.Join(display, ", "))
}

func eventOwnerDeclarationLocations(bundle *WorkflowContractBundle, node runtimeidentity.ExecutableNode, event string) []string {
	record, found := bundle.ExecutableNode(node)
	if !found {
		return nil
	}
	var locations []string
	add := func(handler, slot, label string) {
		path := NodeProvenanceMapPath("event_handlers", handler) + slot
		proof := record.Entry.admissionProvenance[path]
		location := "location unavailable"
		if proof.SourceFile != "" && proof.SourceLine > 0 && proof.SourceColumn > 0 {
			location = fmt.Sprintf("%s:%d:%d", proof.SourceFile, proof.SourceLine, proof.SourceColumn)
		}
		locations = append(locations, fmt.Sprintf("%s %q at %s", label, handler, location))
	}
	// These are the coordinates already selected by the semantics owner, not a
	// second resolution/expansion of authored subscription patterns.
	for _, transition := range bundle.Semantics.HandlerTransitions {
		if transition.Node.Equal(node) && transition.ownerEventType == event {
			add(transition.EventType, "", "handler")
		}
	}
	for _, join := range bundle.Semantics.Joins {
		if join.Node.Equal(node) && join.UntilEvent == event {
			add(join.HandlerEvent, ".join.until", "join.until in handler")
		}
	}
	sort.Strings(locations)
	return locations
}
