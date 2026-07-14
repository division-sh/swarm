package gateruntime

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
)

type FrozenExpression struct {
	Kind    runtimecontracts.ExpressionKind
	Literal semanticvalue.Value
	Ref     string
	CEL     string
}

func (e FrozenExpression) HasLiteralValue() bool {
	return e.Kind == runtimecontracts.ExpressionKindLiteral
}

type FrozenEmit struct {
	Event  string
	Fields map[string]FrozenExpression
}

func (e FrozenEmit) Empty() bool {
	return strings.TrimSpace(e.Event) == "" && len(e.Fields) == 0
}

type Route struct {
	AdvancesTo string
	Emit       FrozenEmit
	EmitSchema semanticvalue.Value
}

func FreezeRoutes(outcomes map[string]runtimecontracts.WorkflowGateOutcomePlan) (string, error) {
	routes := make(map[string]semanticvalue.Value, len(outcomes))
	for verdict, outcome := range outcomes {
		if verdict == "" || verdict != strings.TrimSpace(verdict) {
			return "", fmt.Errorf("gate continuation verdict %q is not canonical", verdict)
		}
		route := Route{
			AdvancesTo: strings.TrimSpace(outcome.AdvancesTo),
			Emit:       FrozenEmit{Event: strings.TrimSpace(outcome.Emit.Event), Fields: make(map[string]FrozenExpression, len(outcome.Emit.Fields))},
			EmitSchema: semanticvalue.EmptyObject(),
		}
		for field, expression := range outcome.Emit.Fields {
			frozen := FrozenExpression{Kind: expression.Kind, Ref: strings.TrimSpace(expression.Ref), CEL: strings.TrimSpace(expression.CEL)}
			if expression.HasLiteralValue() {
				literal, err := canonicaljson.FromGo(expression.Literal)
				if err != nil {
					return "", fmt.Errorf("admit gate route %s literal %s: %w", verdict, field, err)
				}
				frozen.Literal = literal
			}
			route.Emit.Fields[field] = frozen
		}
		if len(outcome.EmitSchema) > 0 {
			schema, err := canonicaljson.FromGo(outcome.EmitSchema)
			if err != nil {
				return "", fmt.Errorf("admit gate route %s schema: %w", verdict, err)
			}
			if schema.Kind() != semanticvalue.KindObject {
				return "", fmt.Errorf("gate route %s schema must be an object", verdict)
			}
			route.EmitSchema = schema
		}
		if err := validateRoute(verdict, route, outcome.Input); err != nil {
			return "", err
		}
		encoded, err := routeSemanticValue(route)
		if err != nil {
			return "", err
		}
		routes[verdict] = encoded
	}
	value, err := semanticvalue.ObjectFromMap(routes)
	if err != nil {
		return "", err
	}
	raw, err := canonicaljson.Encode(value)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func RouteFor(routesJSON, verdict string) (Route, error) {
	value, err := canonicaljson.Decode([]byte(strings.TrimSpace(routesJSON)))
	if err != nil {
		return Route{}, fmt.Errorf("decode gate continuation routes: %w", err)
	}
	routes, ok := value.ObjectMap()
	if !ok {
		return Route{}, fmt.Errorf("gate continuation routes must be an object")
	}
	encoded, ok := routes[strings.TrimSpace(verdict)]
	if !ok {
		return Route{}, fmt.Errorf("gate verdict %s has no durable route", verdict)
	}
	route, err := routeFromSemanticValue(encoded)
	if err != nil {
		return Route{}, fmt.Errorf("decode gate route %s: %w", verdict, err)
	}
	return route, nil
}

func ValidateRoutes(routesJSON string) error {
	value, err := canonicaljson.Decode([]byte(strings.TrimSpace(routesJSON)))
	if err != nil {
		return fmt.Errorf("decode gate continuation routes: %w", err)
	}
	routes, ok := value.ObjectMap()
	if !ok || len(routes) == 0 {
		return fmt.Errorf("gate continuation routes must be a non-empty object")
	}
	for verdict, encoded := range routes {
		if verdict == "" || verdict != strings.TrimSpace(verdict) {
			return fmt.Errorf("gate continuation verdict %q is not canonical", verdict)
		}
		if _, err := routeFromSemanticValue(encoded); err != nil {
			return fmt.Errorf("decode gate route %s: %w", verdict, err)
		}
	}
	return nil
}

func BuildRoutePayload(route Route, fields semanticvalue.Value) (semanticvalue.Value, error) {
	fieldValues, ok := fields.ObjectMap()
	if !ok {
		return semanticvalue.Value{}, fmt.Errorf("decision fields must be an object")
	}
	payload := make(map[string]semanticvalue.Value, len(route.Emit.Fields))
	for field, expression := range route.Emit.Fields {
		if expression.HasLiteralValue() {
			payload[field] = expression.Literal
			continue
		}
		inputName, err := decisionField(expression)
		if err != nil {
			return semanticvalue.Value{}, fmt.Errorf("gate route field %s: %w", field, err)
		}
		value, ok := fieldValues[inputName]
		if !ok || value.Kind() == semanticvalue.KindNull {
			return semanticvalue.Value{}, fmt.Errorf("gate route field %s: decision field %s is absent", field, inputName)
		}
		payload[field] = value
	}
	value, err := semanticvalue.ObjectFromMap(payload)
	if err != nil {
		return semanticvalue.Value{}, err
	}
	if !route.Emit.Empty() {
		schema, _ := route.EmitSchema.ObjectMap()
		payloadMap, _ := value.ObjectMap()
		projectedSchema := make(map[string]any, len(schema))
		for key, item := range schema {
			projectedSchema[key] = item.Interface()
		}
		projectedPayload := make(map[string]any, len(payloadMap))
		for key, item := range payloadMap {
			projectedPayload[key] = item.Interface()
		}
		if err := runtimeeventschema.ValidatePayloadAgainstSchema(projectedSchema, projectedPayload); err != nil {
			return semanticvalue.Value{}, fmt.Errorf("emitted payload does not satisfy the frozen event schema: %w", err)
		}
	}
	return value, nil
}

func validateRoute(verdict string, route Route, inputs map[string]runtimecontracts.WorkflowGateInputField) error {
	if route.AdvancesTo == "" {
		return fmt.Errorf("gate route %s advances_to is required", verdict)
	}
	if route.Emit.Empty() {
		if route.EmitSchema.Kind() != semanticvalue.KindObject || route.EmitSchema.Len() != 0 {
			return fmt.Errorf("gate route %s carries an event schema without an emit", verdict)
		}
		return nil
	}
	if route.EmitSchema.Kind() != semanticvalue.KindObject || route.EmitSchema.Len() == 0 {
		return fmt.Errorf("gate route %s emit is missing its frozen resolved event schema", verdict)
	}
	for field := range route.Emit.Fields {
		if field == "" || field != strings.TrimSpace(field) {
			return fmt.Errorf("gate route %s emit field %q is not canonical", verdict, field)
		}
	}
	schemaMembers, _ := route.EmitSchema.ObjectMap()
	schema := make(map[string]any, len(schemaMembers))
	for key, value := range schemaMembers {
		schema[key] = value.Interface()
	}
	properties := runtimesharedjson.SchemaProperties(schema["properties"])
	literalPayload := make(map[string]any, len(route.Emit.Fields))
	allLiteral := true
	for field, expression := range route.Emit.Fields {
		fieldSchema, ok := properties[field]
		if !ok {
			return fmt.Errorf("gate route %s emit field %s is absent from its frozen event schema", verdict, field)
		}
		if expression.HasLiteralValue() {
			literalPayload[field] = expression.Literal.Interface()
			if err := runtimeeventschema.ValidateValueAgainstSchema(fieldSchema, expression.Literal.Interface()); err != nil {
				return fmt.Errorf("gate route %s literal emit field %s: %w", verdict, field, err)
			}
			continue
		}
		allLiteral = false
		inputName, err := decisionField(expression)
		if err != nil {
			return fmt.Errorf("gate route %s emit field %s: %w", verdict, field, err)
		}
		input, ok := inputs[inputName]
		if !ok || !input.Required {
			return fmt.Errorf("gate route %s emit field %s must read a required declared decision.%s", verdict, field, inputName)
		}
		if !runtimecontracts.WorkflowGateInputTypeCompatibleWithResolvedSchema(input.Type, fieldSchema) {
			return fmt.Errorf("gate route %s decision.%s type %s is incompatible with emit field %s frozen schema", verdict, inputName, input.Type, field)
		}
	}
	for _, required := range runtimesharedjson.RequiredList(schema["required"]) {
		if _, ok := route.Emit.Fields[required]; !ok {
			return fmt.Errorf("gate route %s emit is missing required field %s from its frozen event schema", verdict, required)
		}
	}
	if allLiteral {
		if err := runtimeeventschema.ValidatePayloadAgainstSchema(schema, literalPayload); err != nil {
			return fmt.Errorf("gate route %s assembled literal payload: %w", verdict, err)
		}
	}
	return nil
}

func decisionField(expression FrozenExpression) (string, error) {
	raw := strings.TrimSpace(expression.Ref)
	if raw == "" {
		raw = strings.TrimSpace(expression.CEL)
	}
	if !strings.HasPrefix(raw, "decision.") || strings.Count(raw, ".") != 1 {
		return "", fmt.Errorf("only exact decision.<field> references are supported")
	}
	field := strings.TrimPrefix(raw, "decision.")
	if field == "" || field != strings.TrimSpace(field) {
		return "", fmt.Errorf("decision field reference %q is not canonical", raw)
	}
	return field, nil
}

func routeSemanticValue(route Route) (semanticvalue.Value, error) {
	fields := make(map[string]any, len(route.Emit.Fields))
	for name, expression := range route.Emit.Fields {
		var literal any
		if expression.HasLiteralValue() {
			literal = expression.Literal.Interface()
		}
		fields[name] = map[string]any{"kind": string(expression.Kind), "ref": expression.Ref, "cel": expression.CEL, "literal": literal}
	}
	return canonicaljson.FromGo(map[string]any{
		"advances_to": route.AdvancesTo,
		"emit":        map[string]any{"event": route.Emit.Event, "fields": fields},
		"emit_schema": route.EmitSchema.Interface(),
	})
}

func routeFromSemanticValue(value semanticvalue.Value) (Route, error) {
	root, ok := value.ObjectMap()
	if !ok {
		return Route{}, fmt.Errorf("route must be an object")
	}
	if err := exactFields(root, "route", "advances_to", "emit", "emit_schema"); err != nil {
		return Route{}, err
	}
	advancesTo, ok := root["advances_to"].String()
	if !ok || strings.TrimSpace(advancesTo) == "" {
		return Route{}, fmt.Errorf("route advances_to must be a non-empty string")
	}
	emitRoot, ok := root["emit"].ObjectMap()
	if !ok {
		return Route{}, fmt.Errorf("route emit must be an object")
	}
	if err := exactFields(emitRoot, "route emit", "event", "fields"); err != nil {
		return Route{}, err
	}
	event, ok := emitRoot["event"].String()
	if !ok {
		return Route{}, fmt.Errorf("route emit event must be a string")
	}
	fieldMembers, ok := emitRoot["fields"].ObjectMap()
	if !ok {
		return Route{}, fmt.Errorf("route emit fields must be an object")
	}
	fields := make(map[string]FrozenExpression, len(fieldMembers))
	for name, encoded := range fieldMembers {
		members, ok := encoded.ObjectMap()
		if !ok {
			return Route{}, fmt.Errorf("route emit field %s must be an object", name)
		}
		if err := exactFields(members, "route emit field "+name, "kind", "ref", "cel", "literal"); err != nil {
			return Route{}, err
		}
		kind, kindOK := members["kind"].String()
		ref, refOK := members["ref"].String()
		cel, celOK := members["cel"].String()
		if !kindOK || !refOK || !celOK {
			return Route{}, fmt.Errorf("route emit field %s identity fields must be strings", name)
		}
		expression := FrozenExpression{Kind: runtimecontracts.ExpressionKind(kind), Ref: ref, CEL: cel}
		if expression.HasLiteralValue() {
			expression.Literal = members["literal"]
		} else if members["literal"].Kind() != semanticvalue.KindNull {
			return Route{}, fmt.Errorf("route emit field %s non-literal expression carries a literal", name)
		}
		fields[name] = expression
	}
	schema := root["emit_schema"]
	if schema.Kind() != semanticvalue.KindObject {
		return Route{}, fmt.Errorf("route emit_schema must be an object")
	}
	return Route{AdvancesTo: advancesTo, Emit: FrozenEmit{Event: event, Fields: fields}, EmitSchema: schema}, nil
}

func exactFields(values map[string]semanticvalue.Value, label string, expected ...string) error {
	want := make(map[string]struct{}, len(expected))
	for _, name := range expected {
		want[name] = struct{}{}
		if _, ok := values[name]; !ok {
			return fmt.Errorf("%s is missing field %s", label, name)
		}
	}
	for name := range values {
		if _, ok := want[name]; !ok {
			return fmt.Errorf("%s has unexpected field %s", label, name)
		}
	}
	return nil
}
