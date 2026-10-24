package contracts

import (
	"fmt"
	"sort"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

// Compile copy-all authoring to ordinary writes, so execution has one write path.
func (b *WorkflowContractBundle) lowerDataAccumulation(ctx EmitFieldLoweringContext, spec WorkflowDataAccumulation) (WorkflowDataAccumulation, error) {
	if spec.From == "" {
		return spec, nil
	}
	fail := func(message string) (WorkflowDataAccumulation, error) {
		return WorkflowDataAccumulation{}, fmt.Errorf("INVALID-WRITES: %s: %s", ctx.Site, message)
	}
	if spec.From != "payload" || !ctx.Node.Valid() || strings.TrimSpace(ctx.TriggerEventType) == "" {
		return fail("data_accumulation.from requires payload from an exact trigger event; join outcomes have no payload")
	}
	if spec.SourceEvent != "" && b.ResolveExecutableNodeEventPattern(ctx.Node, spec.SourceEvent) != b.ResolveExecutableNodeEventPattern(ctx.Node, ctx.TriggerEventType) {
		return fail("data_accumulation.source_event cannot select a different payload schema")
	}
	source, ok := b.resolveEmitStructuralType(ctx, ctx.TriggerEventType)
	if !ok {
		return fail("data_accumulation.from payload requires the trigger's structural payload schema")
	}
	primary, err := b.ResolveFlowPrimaryEntity(ctx.Node.FlowPath())
	if err != nil {
		return fail(err.Error())
	}
	target, err := primary.StructuralType()
	if err != nil {
		return fail(err.Error())
	}
	targetFields := make(map[string]ResolvedCatalogType, len(target.Fields))
	for _, field := range target.Fields {
		targetFields[field.Name] = field.Type
	}
	explicit := make(map[string]bool, len(spec.Writes))
	for _, write := range spec.Writes {
		path := paths.Parse(strings.TrimPrefix(strings.TrimSpace(write.Target()), "state."))
		if len(path.Segments) > 0 {
			explicit[path.Segments[0]] = true
		}
	}
	spec.Writes = append([]WorkflowDataWrite(nil), spec.Writes...)
	fields := append([]ResolvedCatalogField(nil), source.Fields...)
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	for _, field := range fields {
		targetType, matches := targetFields[field.Name]
		if field.IsOptional || !matches || explicit[field.Name] {
			continue
		}
		if !StructuralCatalogTypesEqual(field.Type, targetType) {
			return fail(fmt.Sprintf("data_accumulation.from payload field %s type mismatch: %s cannot write %s", field.Name, StructuralCatalogTypeSyntax(field.Type), StructuralCatalogTypeSyntax(targetType)))
		}
		write := WorkflowDataWrite{SourceField: field.Name, TargetField: field.Name}
		if err := hydrateWorkflowDataWrite(&write); err != nil {
			return fail(err.Error())
		}
		spec.Writes = append(spec.Writes, write)
	}
	spec.From = ""
	return spec, nil
}

func (b *WorkflowContractBundle) compileHandlerWrites(node runtimeidentity.ExecutableNode, eventType string, handler SystemNodeEventHandler) (SystemNodeEventHandler, error) {
	handler, err := QualifySystemNodeHandlerRuleRefsForEvent(node, eventType, handler)
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	ctx := EmitFieldLoweringContext{Node: node, TriggerEventType: eventType, Site: node.Key() + ":" + eventType}
	handler.DataAccumulation, err = b.lowerDataAccumulation(ctx, handler.DataAccumulation)
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	lowerRows := func(rows []HandlerRuleEntry) ([]HandlerRuleEntry, error) {
		out := append([]HandlerRuleEntry(nil), rows...)
		for index := range out {
			var err error
			out[index].DataAccumulation, err = b.lowerDataAccumulation(ctx, out[index].DataAccumulation)
			if err != nil {
				return nil, err
			}
			if out[index].PolicyRow.Kind == PolicySheetRowKindDefault && !ruleHasAuthoredEffect(out[index]) {
				return nil, fmt.Errorf("DIALECT-RULES-DEFAULT: %s rules[%d] is empty after payload lowering; a default row must contain an authored effect", ctx.Site, index)
			}
		}
		return out, nil
	}
	handler.Rules, err = lowerRows(handler.Rules)
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	handler.OnComplete, err = lowerRows(handler.OnComplete)
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	if handler.Join != nil {
		join := *handler.Join
		ctx.TriggerEventType = ""
		join.OnComplete.DataAccumulation, err = b.lowerDataAccumulation(ctx, join.OnComplete.DataAccumulation)
		if err != nil {
			return SystemNodeEventHandler{}, err
		}
		join.OnDeadline.DataAccumulation, err = b.lowerDataAccumulation(ctx, join.OnDeadline.DataAccumulation)
		if err != nil {
			return SystemNodeEventHandler{}, err
		}
		handler.Join = &join
	}
	return handler, nil
}
