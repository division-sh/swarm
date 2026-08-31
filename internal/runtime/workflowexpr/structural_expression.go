package workflowexpr

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	celtypes "github.com/google/cel-go/common/types"
)

const workflowStructuralTypePrefix = "swarm.workflow.structural."

type workflowStructuralField struct {
	name       string
	typeValue  runtimecontracts.ResolvedCatalogType
	celType    *cel.Type
	isOptional bool
}

type workflowStructuralNode struct {
	name      string
	typeValue runtimecontracts.ResolvedCatalogType
	fields    map[string]workflowStructuralField
}

type workflowStructuralTypeProvider struct {
	celtypes.Provider
	nodes           map[string]*workflowStructuralNode
	rootTypes       map[string]*cel.Type
	rootIdentifiers map[string]struct{}
	aliases         map[string]string
}

func newWorkflowStructuralTypeProvider(base celtypes.Provider, opts ValueExpressionOptions) (*workflowStructuralTypeProvider, error) {
	provider := &workflowStructuralTypeProvider{
		Provider:        base,
		nodes:           map[string]*workflowStructuralNode{},
		rootTypes:       map[string]*cel.Type{},
		rootIdentifiers: map[string]struct{}{},
		aliases:         map[string]string{},
	}
	for _, root := range []struct {
		name  string
		type_ *runtimecontracts.ResolvedCatalogType
	}{
		{name: "payload", type_: opts.PayloadType},
		{name: "item", type_: opts.ItemType},
	} {
		if root.type_ == nil || root.type_.Kind == "" {
			continue
		}
		resolved, err := provider.register(root.name, root.name, root.type_.Clone())
		if err != nil {
			return nil, fmt.Errorf("workflow %s structural type: %w", root.name, err)
		}
		provider.rootTypes[root.name] = resolved
		identifier := root.name
		if root.name == "item" && strings.TrimSpace(opts.ItemAlias) != "" {
			identifier = strings.TrimSpace(opts.ItemAlias)
		}
		provider.rootIdentifiers[identifier] = struct{}{}
	}
	return provider, nil
}

func (p *workflowStructuralTypeProvider) register(root, path string, resolved runtimecontracts.ResolvedCatalogType) (*cel.Type, error) {
	switch resolved.Kind {
	case runtimecontracts.CatalogTypeDynamic:
		return cel.DynType, nil
	case runtimecontracts.CatalogTypeText:
		return cel.StringType, nil
	case runtimecontracts.CatalogTypeInteger:
		return cel.IntType, nil
	case runtimecontracts.CatalogTypeNumber:
		return cel.DoubleType, nil
	case runtimecontracts.CatalogTypeBoolean:
		return cel.BoolType, nil
	case runtimecontracts.CatalogTypeList:
		if resolved.Element == nil {
			return nil, fmt.Errorf("list type at %s has no element", path)
		}
		element, err := p.register(root, path+"_item", *resolved.Element)
		if err != nil {
			return nil, err
		}
		return cel.ListType(element), nil
	case runtimecontracts.CatalogTypeMap:
		if resolved.Key == nil || resolved.Value == nil {
			return nil, fmt.Errorf("map type at %s is incomplete", path)
		}
		key, err := p.register(root, path+"_key", *resolved.Key)
		if err != nil {
			return nil, err
		}
		value, err := p.register(root, path+"_value", *resolved.Value)
		if err != nil {
			return nil, err
		}
		return cel.MapType(key, value), nil
	case runtimecontracts.CatalogTypeObject:
		alias := strings.TrimSpace(resolved.Name)
		if alias != "" {
			if typeName, ok := p.aliases[alias]; ok {
				return cel.ObjectType(typeName), nil
			}
		}
		typeIdentity := root + "_" + path
		if alias != "" {
			typeIdentity = alias
		}
		typeName := workflowStructuralTypePrefix + sanitizeStructuralTypeName(typeIdentity)
		if alias != "" {
			p.aliases[alias] = typeName
		}
		node := &workflowStructuralNode{name: typeName, typeValue: resolved.Clone(), fields: map[string]workflowStructuralField{}}
		p.nodes[typeName] = node
		for _, field := range resolved.Fields {
			fieldType, err := p.register(root, path+"_"+field.Name, field.Type)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", field.Name, err)
			}
			node.fields[field.Name] = workflowStructuralField{
				name: field.Name, typeValue: field.Type.Clone(), celType: fieldType, isOptional: field.IsOptional,
			}
		}
		return cel.ObjectType(typeName), nil
	default:
		return nil, fmt.Errorf("unsupported structural kind %q at %s", resolved.Kind, path)
	}
}

func sanitizeStructuralTypeName(value string) string {
	var out strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			out.WriteRune(r)
		} else {
			out.WriteByte('_')
		}
	}
	return out.String()
}

func (p *workflowStructuralTypeProvider) rootType(name string) (*cel.Type, bool) {
	value, ok := p.rootTypes[strings.TrimSpace(name)]
	return value, ok
}

func (p *workflowStructuralTypeProvider) resolvedType(value *cel.Type) (runtimecontracts.ResolvedCatalogType, bool) {
	if value == nil {
		return runtimecontracts.ResolvedCatalogType{}, false
	}
	if node, ok := p.nodes[value.TypeName()]; ok {
		return node.typeValue.Clone(), true
	}
	switch value.TypeName() {
	case "string":
		return runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}, true
	case "int":
		return runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeInteger}, true
	case "double":
		return runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeNumber}, true
	case "bool":
		return runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeBoolean}, true
	case "list":
		if len(value.Parameters()) != 1 {
			return runtimecontracts.ResolvedCatalogType{}, false
		}
		element, ok := p.resolvedType(value.Parameters()[0])
		if !ok {
			return runtimecontracts.ResolvedCatalogType{}, false
		}
		return runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeList, Element: &element}, true
	case "map":
		if len(value.Parameters()) != 2 {
			return runtimecontracts.ResolvedCatalogType{}, false
		}
		key, keyOK := p.resolvedType(value.Parameters()[0])
		item, itemOK := p.resolvedType(value.Parameters()[1])
		if !keyOK || !itemOK {
			return runtimecontracts.ResolvedCatalogType{}, false
		}
		return runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeMap, Key: &key, Value: &item}, true
	default:
		return runtimecontracts.ResolvedCatalogType{}, false
	}
}

func (p *workflowStructuralTypeProvider) FindStructType(typeName string) (*celtypes.Type, bool) {
	if _, ok := p.nodes[typeName]; ok {
		return celtypes.NewTypeTypeWithParam(celtypes.NewObjectType(typeName)), true
	}
	if p.Provider == nil {
		return nil, false
	}
	return p.Provider.FindStructType(typeName)
}

func (p *workflowStructuralTypeProvider) FindStructFieldNames(typeName string) ([]string, bool) {
	if node, ok := p.nodes[typeName]; ok {
		names := make([]string, 0, len(node.fields))
		for name := range node.fields {
			names = append(names, name)
		}
		sort.Strings(names)
		return names, true
	}
	if p.Provider == nil {
		return nil, false
	}
	return p.Provider.FindStructFieldNames(typeName)
}

func (p *workflowStructuralTypeProvider) FindStructFieldType(typeName, fieldName string) (*celtypes.FieldType, bool) {
	if node, ok := p.nodes[typeName]; ok {
		field, found := node.fields[strings.TrimSpace(fieldName)]
		if !found {
			return nil, false
		}
		return &celtypes.FieldType{
			Type: field.celType,
			IsSet: func(target any) bool {
				_, present := workflowStructuralMapField(target, field.name)
				return present
			},
			GetFrom: func(target any) (any, error) {
				value, present := workflowStructuralMapField(target, field.name)
				if !present {
					return nil, fmt.Errorf("field %s is absent", field.name)
				}
				return value, nil
			},
		}, true
	}
	if p.Provider == nil {
		return nil, false
	}
	return p.Provider.FindStructFieldType(typeName, fieldName)
}

func workflowStructuralMapField(target any, field string) (any, bool) {
	switch typed := target.(type) {
	case map[string]any:
		value, ok := typed[field]
		return value, ok
	case interface{ Value() any }:
		return workflowStructuralMapField(typed.Value(), field)
	default:
		return nil, false
	}
}

type workflowPresenceFacts map[string]struct{}

func validateWorkflowOptionalReads(compiled *cel.Ast, provider *workflowStructuralTypeProvider) error {
	if compiled == nil || compiled.NativeRep() == nil || provider == nil || len(provider.nodes) == 0 {
		return nil
	}
	analyzer := workflowOptionalReadAnalyzer{ast: compiled.NativeRep(), provider: provider}
	return analyzer.validate(compiled.NativeRep().Expr(), workflowPresenceFacts{}, cloneWorkflowStructuralBindings(provider.rootIdentifiers))
}

type workflowOptionalReadAnalyzer struct {
	ast      *celast.AST
	provider *workflowStructuralTypeProvider
}

type workflowStructuralBindings map[string]struct{}

func cloneWorkflowStructuralBindings(source map[string]struct{}) workflowStructuralBindings {
	out := make(workflowStructuralBindings, len(source))
	for name := range source {
		out[name] = struct{}{}
	}
	return out
}

func (a workflowOptionalReadAnalyzer) validate(expr celast.Expr, facts workflowPresenceFacts, bindings workflowStructuralBindings) error {
	if expr == nil {
		return nil
	}
	if expr.Kind() == celast.SelectKind {
		selection := expr.AsSelect()
		if err := a.validate(selection.Operand(), facts, bindings); err != nil {
			return err
		}
		if selection.IsTestOnly() {
			return nil
		}
		field, path, optional := a.selectedField(selection.Operand(), selection.FieldName())
		if optional {
			if _, proven := facts[path]; !proven {
				return undecidedOptionalReadError(path, field)
			}
		}
		return nil
	}
	if expr.Kind() == celast.CallKind {
		call := expr.AsCall()
		args := call.Args()
		if call.FunctionName() == "_[_]" && len(args) == 2 && a.structuralExpression(args[0], bindings) {
			return fmt.Errorf("direct map/list lookup is not presence-safe; use the stock optional lookup form <value>[?<key-or-index>] and decide or forward its absence")
		}
		switch call.FunctionName() {
		case "_&&_":
			if len(args) == 2 {
				if err := a.validate(args[0], facts, bindings); err != nil {
					return err
				}
				return a.validate(args[1], mergeWorkflowPresenceFacts(facts, a.factsWhen(args[0], true)), bindings)
			}
		case "_||_":
			if len(args) == 2 {
				if err := a.validate(args[0], facts, bindings); err != nil {
					return err
				}
				return a.validate(args[1], mergeWorkflowPresenceFacts(facts, a.factsWhen(args[0], false)), bindings)
			}
		case "_?_:_":
			if len(args) == 3 {
				if err := a.validate(args[0], facts, bindings); err != nil {
					return err
				}
				if err := a.validate(args[1], mergeWorkflowPresenceFacts(facts, a.factsWhen(args[0], true)), bindings); err != nil {
					return err
				}
				return a.validate(args[2], mergeWorkflowPresenceFacts(facts, a.factsWhen(args[0], false)), bindings)
			}
		case "_?._":
			// Stock optional selection owns the absence decision. Its operand may
			// still contain an independently undecided parent read.
			if len(args) > 0 {
				return a.validate(args[0], facts, bindings)
			}
		}
		if call.IsMemberFunction() {
			if err := a.validate(call.Target(), facts, bindings); err != nil {
				return err
			}
		}
		for _, arg := range args {
			if err := a.validate(arg, facts, bindings); err != nil {
				return err
			}
		}
		return nil
	}
	switch expr.Kind() {
	case celast.ComprehensionKind:
		value := expr.AsComprehension()
		if err := a.validate(value.IterRange(), facts, bindings); err != nil {
			return err
		}
		if err := a.validate(value.AccuInit(), facts, bindings); err != nil {
			return err
		}
		bodyBindings := cloneWorkflowStructuralBindings(bindings)
		if a.structuralExpression(value.IterRange(), bindings) {
			bodyBindings[value.IterVar()] = struct{}{}
			if value.HasIterVar2() {
				bodyBindings[value.IterVar2()] = struct{}{}
			}
		}
		if a.structuralExpression(value.AccuInit(), bindings) {
			bodyBindings[value.AccuVar()] = struct{}{}
		}
		for _, child := range []celast.Expr{value.LoopCondition(), value.LoopStep(), value.Result()} {
			if err := a.validate(child, facts, bodyBindings); err != nil {
				return err
			}
		}
	case celast.ListKind:
		for _, element := range expr.AsList().Elements() {
			if err := a.validate(element, facts, bindings); err != nil {
				return err
			}
		}
	case celast.MapKind:
		for _, entry := range expr.AsMap().Entries() {
			value := entry.AsMapEntry()
			if err := a.validate(value.Key(), facts, bindings); err != nil {
				return err
			}
			if err := a.validate(value.Value(), facts, bindings); err != nil {
				return err
			}
		}
	case celast.StructKind:
		for _, field := range expr.AsStruct().Fields() {
			if err := a.validate(field.AsStructField().Value(), facts, bindings); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a workflowOptionalReadAnalyzer) structuralExpression(expr celast.Expr, bindings workflowStructuralBindings) bool {
	if expr == nil {
		return false
	}
	if typeValue := a.ast.GetType(expr.ID()); typeValue != nil {
		if _, owned := a.provider.nodes[typeValue.TypeName()]; owned {
			return true
		}
	}
	switch expr.Kind() {
	case celast.IdentKind:
		_, ok := bindings[expr.AsIdent()]
		return ok
	case celast.SelectKind:
		return a.structuralExpression(expr.AsSelect().Operand(), bindings)
	case celast.CallKind:
		call := expr.AsCall()
		if call.IsMemberFunction() && a.structuralExpression(call.Target(), bindings) {
			return true
		}
		args := call.Args()
		return len(args) > 0 && a.structuralExpression(args[0], bindings)
	case celast.ComprehensionKind:
		return a.structuralExpression(expr.AsComprehension().IterRange(), bindings)
	default:
		return false
	}
}

func validateWorkflowResultType(output *cel.Type, provider *workflowStructuralTypeProvider, opts ValueExpressionOptions) error {
	if output == nil {
		return fmt.Errorf("workflow expression result type is unavailable")
	}
	actual := output
	isOptional := output.TypeName() == "optional_type" && len(output.Parameters()) == 1
	if isOptional {
		if !opts.ResultOptional {
			return fmt.Errorf("optional expression result requires an optional sink or an explicit presence decision")
		}
		actual = output.Parameters()[0]
	}
	if opts.ResultType == nil {
		return nil
	}
	// Entity and other separately governed roots remain dynamic in this slice;
	// their existing structural validators own sink compatibility. Exact
	// payload/item roots never compile to dyn.
	if actual == cel.DynType {
		return nil
	}
	if structural, ok := provider.resolvedType(actual); ok && runtimecontracts.StructuralCatalogTypesEqual(structural, *opts.ResultType) {
		return nil
	}
	expected, err := provider.register("result", "result", opts.ResultType.Clone())
	if err != nil {
		return fmt.Errorf("workflow expression result type: %w", err)
	}
	if !expected.IsAssignableType(actual) {
		return fmt.Errorf("workflow expression result %s is not assignable to %s", output, expected)
	}
	return nil
}

func (a workflowOptionalReadAnalyzer) selectedField(operand celast.Expr, fieldName string) (workflowStructuralField, string, bool) {
	typeValue := a.ast.GetType(operand.ID())
	if typeValue == nil {
		return workflowStructuralField{}, "", false
	}
	node, ok := a.provider.nodes[typeValue.TypeName()]
	if !ok {
		return workflowStructuralField{}, "", false
	}
	field, ok := node.fields[strings.TrimSpace(fieldName)]
	if !ok {
		return workflowStructuralField{}, "", false
	}
	path, ok := workflowSelectionPath(operand)
	if !ok {
		path = typeValue.TypeName()
	}
	path += "." + field.name
	return field, path, field.isOptional
}

func (a workflowOptionalReadAnalyzer) factsWhen(expr celast.Expr, truth bool) workflowPresenceFacts {
	if expr == nil {
		return nil
	}
	if expr.Kind() == celast.SelectKind && expr.AsSelect().IsTestOnly() {
		path, ok := workflowSelectionPath(expr)
		if ok && truth {
			return workflowPresenceFacts{path: {}}
		}
		return nil
	}
	if expr.Kind() != celast.CallKind {
		return nil
	}
	call := expr.AsCall()
	args := call.Args()
	switch call.FunctionName() {
	case "!_":
		if len(args) == 1 {
			return a.factsWhen(args[0], !truth)
		}
	case "_&&_":
		if len(args) == 2 && truth {
			return mergeWorkflowPresenceFacts(a.factsWhen(args[0], true), a.factsWhen(args[1], true))
		}
	case "_||_":
		if len(args) == 2 && !truth {
			return mergeWorkflowPresenceFacts(a.factsWhen(args[0], false), a.factsWhen(args[1], false))
		}
	}
	return nil
}

func workflowSelectionPath(expr celast.Expr) (string, bool) {
	if expr == nil {
		return "", false
	}
	switch expr.Kind() {
	case celast.IdentKind:
		return expr.AsIdent(), true
	case celast.SelectKind:
		parent, ok := workflowSelectionPath(expr.AsSelect().Operand())
		if !ok {
			return "", false
		}
		return parent + "." + expr.AsSelect().FieldName(), true
	default:
		return "", false
	}
}

func mergeWorkflowPresenceFacts(values ...workflowPresenceFacts) workflowPresenceFacts {
	out := workflowPresenceFacts{}
	for _, value := range values {
		for path := range value {
			out[path] = struct{}{}
		}
	}
	return out
}

func undecidedOptionalReadError(path string, field workflowStructuralField) error {
	return fmt.Errorf(
		"optional field %s is read without a presence decision; absent should mean a default -> %s; absent should mean don't-match -> has(%s) && ...",
		path,
		strings.TrimSuffix(path, "."+field.name)+".?"+field.name+".orValue(<default>)",
		path,
	)
}
