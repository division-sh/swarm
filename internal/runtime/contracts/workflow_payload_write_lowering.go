package contracts

import (
	"fmt"
	"reflect"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

func (b *WorkflowContractBundle) lowerPayloadWriteSources() error {
	for _, record := range b.ScopedNodeRecords() {
		for _, event := range sortedContractKeys(record.Entry.EventHandlers) {
			handler := record.Entry.EventHandlers[event]
			lower := func(writes *WorkflowDataAccumulation) error {
				if writes.From == "" {
					return nil
				}
				node, err := record.Identity()
				if err != nil {
					return err
				}
				return b.lowerPayloadWrites(node, event, writes)
			}
			if err := lower(&handler.DataAccumulation); err != nil {
				return err
			}
			for _, rules := range [][]HandlerRuleEntry{handler.Rules, handler.OnComplete} {
				for i := range rules {
					if err := lower(&rules[i].DataAccumulation); err != nil {
						return err
					}
				}
			}
			if handler.Join != nil {
				if err := lower(&handler.Join.OnComplete.DataAccumulation); err != nil {
					return err
				}
				if err := lower(&handler.Join.OnDeadline.DataAccumulation); err != nil {
					return err
				}
			}
			record.Entry.EventHandlers[event] = handler
		}
	}
	return nil
}

func (b *WorkflowContractBundle) lowerPayloadWrites(node runtimeidentity.ExecutableNode, event string, writes *WorkflowDataAccumulation) error {
	if writes.From == "" {
		return nil
	}
	fail := func(message string) error {
		return fmt.Errorf("data_accumulation.from at %s node %s handler %s: %s", writes.fromLocation, node.Key(), event, message)
	}
	if writes.From != "payload" {
		return fail("from names exactly one source namespace: payload")
	}
	compiled, found, err := b.ResolveEffectiveCompiledFlowEventSchema(node.FlowPath(), event)
	if err != nil {
		return err
	}
	if !found {
		return fail("from: payload requires the handler's declared event schema")
	}
	payload, found := compiled.StructuralType()
	if !found {
		return fail("from: payload requires the handler's typed payload schema")
	}
	_, primary, hasState := b.FlowPrimaryEntityContract(node.FlowPath())
	if !hasState {
		writes.From, writes.fromLocation = "", ""
		return nil
	}
	explicit := make(map[string]bool, len(writes.Writes))
	for _, write := range writes.Writes {
		target := paths.Parse(write.Target())
		if (target.Root == paths.RootUnknown || target.Root == paths.RootEntity) && len(target.Segments) > 0 {
			explicit[target.Segments[0]] = true
		}
	}
	catalog := b.ResolvedTypeCatalogForFlow(node.FlowPath())
	for _, field := range payload.Fields {
		target, matches := primary.Fields[field.Name]
		if field.IsOptional || !matches || explicit[field.Name] {
			continue
		}
		sourceRef, found := ResolveExecutableNodeEventFieldType(b, node, event, field.Name)
		if !found {
			return fail(fmt.Sprintf("payload.%s has no declared type; write it explicitly", field.Name))
		}
		sourceType, err := sourceRef.Resolve()
		if err != nil {
			return err
		}
		targetType, err := (CatalogTypeReference{Type: target.Type, Catalog: catalog}).Resolve()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(sourceType, targetType) {
			return fail(fmt.Sprintf("payload.%s is %s, state.%s is %s; declare the same type or write it explicitly", field.Name, field.TypeRef, field.Name, target.Type))
		}
		write := WorkflowDataWrite{SourceField: field.Name, TargetField: field.Name}
		if err := hydrateWorkflowDataWrite(&write); err != nil {
			return err
		}
		writes.Writes = append(writes.Writes, write)
	}
	writes.From, writes.fromLocation = "", ""
	if len(writes.Writes) > 0 && strings.TrimSpace(writes.SourceEvent) == "" {
		writes.SourceEvent = event
	}
	return nil
}
