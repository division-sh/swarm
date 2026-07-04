package contracts

import (
	"fmt"
	"sort"
	"strings"
)

const (
	EmitFromEntity  = "entity"
	EmitFromPayload = "payload"
)

type EmitFieldLoweringContext struct {
	NodeID           string
	FlowID           string
	TriggerEventType string
	Site             string
}

func (b *WorkflowContractBundle) LowerEmitSpecFields(ctx EmitFieldLoweringContext, spec EmitSpec) (EmitSpec, error) {
	spec = cloneEmitSpec(spec)
	ctx = b.normalizeEmitFieldLoweringContext(ctx)
	if spec.Empty() {
		return spec, nil
	}
	if err := validateEmitFromSource(spec.From); err != nil {
		return EmitSpec{}, emitLoweringError(ctx, err.Error())
	}
	if !emitSpecNeedsFieldLowering(spec) {
		return spec, nil
	}
	eventType := spec.EventType()
	if eventType == "" {
		return EmitSpec{}, emitLoweringError(ctx, "emit.from and bare namespace emit.fields values require emit.event")
	}
	targetFields, required, err := b.emitPayloadTargetFields(ctx, eventType)
	if err != nil {
		return EmitSpec{}, emitLoweringError(ctx, err.Error())
	}
	fields := cloneExpressionValueMap(spec.Fields)
	if len(fields) == 0 {
		fields = map[string]ExpressionValue{}
	}
	for target, value := range fields {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		if !emitPayloadTargetDeclared(targetFields, target) {
			return EmitSpec{}, emitLoweringError(ctx, fmt.Sprintf("emit.fields.%s is not declared by emitted event %s payload schema", target, eventType))
		}
		source := bareEmitNamespaceValue(value)
		if source == "" {
			continue
		}
		if err := validateEmitFromSource(source); err != nil {
			return EmitSpec{}, emitLoweringError(ctx, err.Error())
		}
		if !emitSimplePayloadField(target) {
			return EmitSpec{}, emitLoweringError(ctx, fmt.Sprintf("emit.fields.%s bare namespace value %q requires a top-level payload field target", target, source))
		}
		sourceFields, err := b.emitFieldSourceFields(ctx, source)
		if err != nil {
			return EmitSpec{}, emitLoweringError(ctx, err.Error())
		}
		if !emitPayloadTargetDeclared(sourceFields, target) {
			return EmitSpec{}, emitLoweringError(ctx, fmt.Sprintf("emit.fields.%s source %s does not declare same-named field %s", target, source, target))
		}
		fields[target] = CELExpression(source + "." + target)
	}
	if source := strings.TrimSpace(spec.From); source != "" {
		sourceFields, err := b.emitFieldSourceFields(ctx, source)
		if err != nil {
			return EmitSpec{}, emitLoweringError(ctx, err.Error())
		}
		for _, field := range required {
			if field == "" {
				continue
			}
			if _, explicit := fields[field]; explicit {
				continue
			}
			if !emitPayloadTargetDeclared(sourceFields, field) {
				return EmitSpec{}, emitLoweringError(ctx, fmt.Sprintf("emit.from %s cannot fill required emitted payload field %s because source %s does not declare it", source, field, source))
			}
			fields[field] = CELExpression(source + "." + field)
		}
	}
	spec.From = ""
	spec.Fields = fields
	return spec, nil
}

func validateEmitFromSource(source string) error {
	source = strings.TrimSpace(source)
	switch source {
	case "", EmitFromEntity, EmitFromPayload:
		return nil
	default:
		return fmt.Errorf("emit.from source %q is unsupported; use entity or payload", source)
	}
}

func emitSpecNeedsFieldLowering(spec EmitSpec) bool {
	return EmitSpecNeedsFieldLowering(spec)
}

func EmitSpecNeedsFieldLowering(spec EmitSpec) bool {
	if strings.TrimSpace(spec.From) != "" {
		return true
	}
	for _, value := range spec.Fields {
		if bareEmitNamespaceValue(value) != "" {
			return true
		}
	}
	return false
}

func bareEmitNamespaceValue(value ExpressionValue) string {
	value.hydrate()
	if value.Kind != ExpressionKindCEL {
		return ""
	}
	switch strings.TrimSpace(value.CEL) {
	case EmitFromEntity:
		return EmitFromEntity
	case EmitFromPayload:
		return EmitFromPayload
	default:
		return ""
	}
}

func (b *WorkflowContractBundle) normalizeEmitFieldLoweringContext(ctx EmitFieldLoweringContext) EmitFieldLoweringContext {
	ctx.NodeID = strings.TrimSpace(ctx.NodeID)
	ctx.FlowID = strings.TrimSpace(ctx.FlowID)
	ctx.TriggerEventType = strings.TrimSpace(ctx.TriggerEventType)
	ctx.Site = strings.TrimSpace(ctx.Site)
	if ctx.Site == "" {
		ctx.Site = "emit"
	}
	if b != nil && ctx.FlowID == "" && ctx.NodeID != "" {
		if source, ok := b.NodeContractSource(ctx.NodeID); ok {
			ctx.FlowID = strings.TrimSpace(source.FlowID)
		}
	}
	return ctx
}

func (b *WorkflowContractBundle) emitPayloadTargetFields(ctx EmitFieldLoweringContext, eventType string) (map[string]struct{}, []string, error) {
	if b == nil {
		return nil, nil, fmt.Errorf("emit field lowering requires a workflow contract bundle")
	}
	entry, resolved, ok := b.ResolveFlowEventCatalogEntry(ctx.FlowID, eventType)
	if !ok {
		if platformEntry, platformKey, platformOK := PlatformEventCatalogEntry(b.Platform, eventType); platformOK {
			entry = platformEntry
			resolved = platformKey
			ok = true
		}
	}
	if !ok {
		return nil, nil, fmt.Errorf("emit field lowering requires emitted event %s payload schema", strings.TrimSpace(eventType))
	}
	fields := eventPayloadDeclaredFields(entry)
	required := eventPayloadRequiredFields(entry)
	if len(fields) == 0 && len(required) == 0 {
		return nil, nil, fmt.Errorf("emit field lowering requires emitted event %s payload schema", strings.TrimSpace(resolved))
	}
	for _, field := range required {
		fields[field] = struct{}{}
	}
	return fields, required, nil
}

func (b *WorkflowContractBundle) emitFieldSourceFields(ctx EmitFieldLoweringContext, source string) (map[string]struct{}, error) {
	switch strings.TrimSpace(source) {
	case EmitFromEntity:
		primary, err := b.ResolveFlowPrimaryEntity(ctx.FlowID)
		if err != nil {
			return nil, fmt.Errorf("emit.from entity requires a primary entity contract: %w", err)
		}
		fields := make(map[string]struct{}, len(primary.Contract.Fields))
		for field := range primary.Contract.Fields {
			field = strings.TrimSpace(field)
			if field != "" {
				fields[field] = struct{}{}
			}
		}
		return fields, nil
	case EmitFromPayload:
		entry, resolved, ok := b.ResolveFlowEventCatalogEntry(ctx.FlowID, ctx.TriggerEventType)
		if !ok {
			if platformEntry, platformKey, platformOK := PlatformEventCatalogEntry(b.Platform, ctx.TriggerEventType); platformOK {
				entry = platformEntry
				resolved = platformKey
				ok = true
			}
		}
		if !ok {
			return nil, fmt.Errorf("emit.from payload requires trigger event %s payload schema", strings.TrimSpace(ctx.TriggerEventType))
		}
		fields := eventPayloadDeclaredFields(entry)
		for _, field := range eventPayloadRequiredFields(entry) {
			fields[field] = struct{}{}
		}
		if len(fields) == 0 {
			return nil, fmt.Errorf("emit.from payload requires trigger event %s payload schema", strings.TrimSpace(resolved))
		}
		return fields, nil
	default:
		return nil, fmt.Errorf("emit.from source %q is unsupported; use entity or payload", source)
	}
}

func eventPayloadDeclaredFields(entry EventCatalogEntry) map[string]struct{} {
	fields := map[string]struct{}{}
	for field := range entry.Payload.Properties {
		field = strings.TrimSpace(field)
		if field != "" {
			fields[field] = struct{}{}
		}
	}
	return fields
}

func eventPayloadRequiredFields(entry EventCatalogEntry) []string {
	return uniqueEmitFieldNames(entry.Required, entry.Payload.Required)
}

func uniqueEmitFieldNames(groups ...[]string) []string {
	seen := map[string]struct{}{}
	for _, group := range groups {
		for _, field := range group {
			field = strings.TrimSpace(field)
			if field != "" {
				seen[field] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for field := range seen {
		out = append(out, field)
	}
	sort.Strings(out)
	return out
}

func emitPayloadTargetDeclared(fields map[string]struct{}, target string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	_, ok := fields[target]
	return ok
}

func emitSimplePayloadField(target string) bool {
	target = strings.TrimSpace(target)
	return target != "" && !strings.Contains(target, ".")
}

func emitLoweringError(ctx EmitFieldLoweringContext, message string) error {
	prefix := strings.TrimSpace(ctx.Site)
	if prefix == "" {
		prefix = "emit"
	}
	return fmt.Errorf("INVALID-EMIT: %s: %s", prefix, strings.TrimSpace(message))
}
