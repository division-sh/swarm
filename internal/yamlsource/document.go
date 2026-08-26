package yamlsource

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Presence is the closed lexical shape vocabulary retained until typed
// admission assigns domain meaning.
type Presence uint8

const (
	PresenceMissing Presence = iota
	PresenceNull
	PresenceEmptyScalar
	PresenceScalar
	PresenceEmptySequence
	PresenceSequence
	PresenceEmptyMapping
	PresenceMapping
)

func (p Presence) String() string {
	switch p {
	case PresenceMissing:
		return "missing"
	case PresenceNull:
		return "null"
	case PresenceEmptyScalar:
		return "empty_scalar"
	case PresenceScalar:
		return "scalar"
	case PresenceEmptySequence:
		return "empty_sequence"
	case PresenceSequence:
		return "sequence"
	case PresenceEmptyMapping:
		return "empty_mapping"
	case PresenceMapping:
		return "mapping"
	default:
		return fmt.Sprintf("presence(%d)", p)
	}
}

type Location struct {
	File   string
	Line   int
	Column int
}

func (l Location) String() string {
	if strings.TrimSpace(l.File) == "" {
		return fmt.Sprintf("line %d, column %d", l.Line, l.Column)
	}
	return fmt.Sprintf("%s:%d:%d", l.File, l.Line, l.Column)
}

// Document is an immutable view over one authoritative YAML source. An absent
// optional source is represented explicitly rather than as an empty mapping.
type Document struct {
	snapshot Snapshot
	file     string
	present  bool
}

func (s Snapshot) Document(file string) Document {
	return Document{snapshot: s, file: strings.TrimSpace(file), present: s.entry != nil}
}

func MissingDocument(file string) Document {
	return Document{file: strings.TrimSpace(file)}
}

func (d Document) Presence() Presence {
	return d.Root().Presence()
}

func (d Document) Root() Value {
	if !d.present || d.snapshot.entry == nil {
		return Value{file: d.file, path: "$", missing: true}
	}
	return Value{node: &d.snapshot.entry.root, file: d.file, path: "$"}
}

// Value exposes syntax and location without exposing the retained yaml.Node.
type Value struct {
	node         *yaml.Node
	file         string
	path         string
	missing      bool
	introduction Location
}

func (v Value) SemanticPath() string { return v.path }

func (v Value) Location() Location {
	node := authoredValueNode(v.node)
	if node == nil {
		return Location{File: v.file}
	}
	return Location{File: v.file, Line: node.Line, Column: node.Column}
}

// IntroductionLocation identifies the lexical occurrence that introduced this
// effective value. For descendants projected through an alias or merge it is
// the outer alias/merge use; Location and ResolvedLocation still expose the
// retained value and target coordinates separately.
func (v Value) IntroductionLocation() Location {
	if validLocation(v.introduction) {
		return v.introduction
	}
	return v.Location()
}

// ResolvedLocation identifies the semantic target of a YAML alias. Location
// remains the authored alias-use coordinate so provenance never loses the
// lexical occurrence that introduced the effective value.
func (v Value) ResolvedLocation() Location {
	node := resolvedValueNode(v.node)
	if node == nil {
		return Location{File: v.file}
	}
	return Location{File: v.file, Line: node.Line, Column: node.Column}
}

func (v Value) Presence() Presence {
	if v.missing || v.node == nil {
		return PresenceMissing
	}
	node := resolvedValueNode(v.node)
	if node == nil {
		return PresenceMissing
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
			return PresenceNull
		}
		if node.Value == "" {
			return PresenceEmptyScalar
		}
		return PresenceScalar
	case yaml.SequenceNode:
		if len(node.Content) == 0 {
			return PresenceEmptySequence
		}
		return PresenceSequence
	case yaml.MappingNode:
		if len(node.Content) == 0 {
			return PresenceEmptyMapping
		}
		return PresenceMapping
	default:
		return PresenceMissing
	}
}

type Scalar struct {
	Value            string
	Tag              string
	Style            Style
	Location         Location
	ResolvedLocation Location
}

func (v Value) Scalar() (Scalar, error) {
	node := resolvedValueNode(v.node)
	if node == nil || node.Kind != yaml.ScalarNode {
		return Scalar{}, fmt.Errorf("%s at %s is %s, want scalar", v.path, v.Location(), v.Presence())
	}
	return Scalar{
		Value:            node.Value,
		Tag:              node.Tag,
		Style:            Style(node.Style),
		Location:         v.Location(),
		ResolvedLocation: v.ResolvedLocation(),
	}, nil
}

func (v Value) Sequence() ([]Value, error) {
	node := resolvedValueNode(v.node)
	if node == nil || node.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("%s at %s is %s, want sequence", v.path, v.Location(), v.Presence())
	}
	introduction := v.descendantIntroduction()
	out := make([]Value, 0, len(node.Content))
	for i, child := range node.Content {
		out = append(out, Value{node: child, file: v.file, path: v.path + "[" + strconv.Itoa(i) + "]", introduction: introduction})
	}
	return out, nil
}

type MappingField struct {
	Name                       string
	KeyLocation                Location
	Value                      Value
	FromMerge                  bool
	MergeLocation              Location
	MergeValueLocation         Location
	MergeResolvedValueLocation Location
}

// IntroductionLocation identifies where this field entered the effective
// mapping while KeyLocation retains the expanded key coordinate.
func (f MappingField) IntroductionLocation() Location {
	if validLocation(f.Value.introduction) {
		return f.Value.introduction
	}
	if f.FromMerge && validLocation(f.MergeValueLocation) {
		return f.MergeValueLocation
	}
	return f.KeyLocation
}

type Occurrence struct {
	KeyLocation                Location
	ValueLocation              Location
	ResolvedValueLocation      Location
	Presence                   Presence
	FromMerge                  bool
	MergeLocation              Location
	MergeValueLocation         Location
	MergeResolvedValueLocation Location
}

type Lookup struct {
	SemanticPath string
	Presence     Presence
	Value        Value
	Occurrences  []Occurrence
}

func (v Value) Mapping() ([]MappingField, error) {
	node := resolvedValueNode(v.node)
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s at %s is %s, want mapping", v.path, v.Location(), v.Presence())
	}
	return collectMappingFields(node, v.file, v.path, v.descendantIntroduction(), false, Location{}, Location{}, Location{}, map[*yaml.Node]bool{})
}

func (v Value) Lookup(name string) (Lookup, error) {
	name = strings.TrimSpace(name)
	path := mappingChildPath(v.path, name)
	fields, err := v.Mapping()
	if err != nil {
		return Lookup{}, err
	}
	lookup := Lookup{SemanticPath: path, Presence: PresenceMissing, Value: Value{file: v.file, path: path, missing: true}}
	for _, field := range fields {
		if field.Name != name {
			continue
		}
		if len(lookup.Occurrences) == 0 {
			lookup.Presence = field.Value.Presence()
			lookup.Value = field.Value
		}
		lookup.Occurrences = append(lookup.Occurrences, Occurrence{
			KeyLocation:                field.KeyLocation,
			ValueLocation:              field.Value.Location(),
			ResolvedValueLocation:      field.Value.ResolvedLocation(),
			Presence:                   field.Value.Presence(),
			FromMerge:                  field.FromMerge,
			MergeLocation:              field.MergeLocation,
			MergeValueLocation:         field.MergeValueLocation,
			MergeResolvedValueLocation: field.MergeResolvedValueLocation,
		})
	}
	return lookup, nil
}

func (v Value) ValidateUniqueMappings() error {
	switch v.Presence() {
	case PresenceMapping, PresenceEmptyMapping:
		fields, err := v.Mapping()
		if err != nil {
			return err
		}
		seen := map[string]Location{}
		for _, field := range fields {
			if previous, ok := seen[field.Name]; ok {
				return fmt.Errorf("duplicate effective YAML key %q at %s and %s for %s", field.Name, previous, field.KeyLocation, field.Value.SemanticPath())
			}
			seen[field.Name] = field.KeyLocation
		}
		for _, field := range fields {
			if err := field.Value.ValidateUniqueMappings(); err != nil {
				return err
			}
		}
	case PresenceSequence, PresenceEmptySequence:
		values, err := v.Sequence()
		if err != nil {
			return err
		}
		for _, value := range values {
			if err := value.ValidateUniqueMappings(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Project decodes one already-inspected value into a caller-owned typed value.
// Admission must classify presence and duplicate occurrences before calling it.
func (v Value) Project(target any) error {
	if v.missing || v.node == nil {
		return fmt.Errorf("cannot project missing YAML value %s", v.path)
	}
	node := cloneNode(resolvedValueNode(v.node))
	if node == nil {
		return fmt.Errorf("cannot project missing YAML value %s", v.path)
	}
	return node.Decode(target)
}

func collectMappingFields(node *yaml.Node, file, path string, introduction Location, fromMerge bool, mergeLocation, mergeValueLocation, mergeResolvedValueLocation Location, stack map[*yaml.Node]bool) ([]MappingField, error) {
	node = resolvedValueNode(node)
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s is not a mapping", path)
	}
	if stack[node] {
		return nil, fmt.Errorf("YAML merge cycle at %s", Location{File: file, Line: node.Line, Column: node.Column})
	}
	stack[node] = true
	defer delete(stack, node)

	out := make([]MappingField, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := resolvedValueNode(node.Content[i])
		valueNode := node.Content[i+1]
		if keyNode == nil || keyNode.Kind != yaml.ScalarNode {
			return nil, fmt.Errorf("mapping key at %s must be scalar", Location{File: file, Line: node.Content[i].Line, Column: node.Content[i].Column})
		}
		keyLocation := Location{File: file, Line: node.Content[i].Line, Column: node.Content[i].Column}
		if keyNode.Value == "<<" || strings.EqualFold(strings.TrimSpace(keyNode.Tag), "!!merge") {
			merged, err := collectMergeFields(valueNode, file, path, introduction, keyLocation, stack)
			if err != nil {
				return nil, err
			}
			out = append(out, merged...)
			continue
		}
		name := keyNode.Value
		out = append(out, MappingField{
			Name:                       name,
			KeyLocation:                keyLocation,
			Value:                      Value{node: valueNode, file: file, path: mappingChildPath(path, name), introduction: introduction},
			FromMerge:                  fromMerge,
			MergeLocation:              mergeLocation,
			MergeValueLocation:         mergeValueLocation,
			MergeResolvedValueLocation: mergeResolvedValueLocation,
		})
	}
	return out, nil
}

func collectMergeFields(node *yaml.Node, file, path string, introduction, mergeLocation Location, stack map[*yaml.Node]bool) ([]MappingField, error) {
	mergeValueLocation := nodeLocation(file, authoredValueNode(node))
	resolved := resolvedValueNode(node)
	if resolved == nil {
		return nil, fmt.Errorf("YAML merge at %s has no mapping value", mergeLocation)
	}
	mergeResolvedValueLocation := nodeLocation(file, resolved)
	switch resolved.Kind {
	case yaml.MappingNode:
		mappingIntroduction := introduction
		if !validLocation(mappingIntroduction) {
			mappingIntroduction = mergeValueLocation
		}
		return collectMappingFields(resolved, file, path, mappingIntroduction, true, mergeLocation, mergeValueLocation, mergeResolvedValueLocation, stack)
	case yaml.SequenceNode:
		var out []MappingField
		for _, item := range resolved.Content {
			itemValueLocation := nodeLocation(file, authoredValueNode(item))
			resolvedItem := resolvedValueNode(item)
			if resolvedItem == nil || resolvedItem.Kind != yaml.MappingNode {
				return nil, fmt.Errorf("YAML merge sequence at %s contains a non-mapping value", mergeLocation)
			}
			itemIntroduction := introduction
			if !validLocation(itemIntroduction) {
				itemIntroduction = itemValueLocation
			}
			fields, err := collectMappingFields(resolvedItem, file, path, itemIntroduction, true, mergeLocation, itemValueLocation, nodeLocation(file, resolvedItem), stack)
			if err != nil {
				return nil, err
			}
			out = append(out, fields...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("YAML merge at %s requires a mapping or sequence of mappings", mergeLocation)
	}
}

func (v Value) descendantIntroduction() Location {
	if validLocation(v.introduction) {
		return v.introduction
	}
	node := authoredValueNode(v.node)
	if node != nil && node.Kind == yaml.AliasNode {
		return v.Location()
	}
	return Location{}
}

func validLocation(location Location) bool {
	return location.Line > 0 || location.Column > 0
}

func authoredValueNode(node *yaml.Node) *yaml.Node {
	seen := map[*yaml.Node]bool{}
	for node != nil {
		if seen[node] {
			return nil
		}
		seen[node] = true
		if node.Kind != yaml.DocumentNode {
			return node
		}
		if len(node.Content) != 1 {
			return nil
		}
		node = node.Content[0]
	}
	return nil
}

func nodeLocation(file string, node *yaml.Node) Location {
	if node == nil {
		return Location{File: file}
	}
	return Location{File: file, Line: node.Line, Column: node.Column}
}

func resolvedValueNode(node *yaml.Node) *yaml.Node {
	seen := map[*yaml.Node]bool{}
	for node != nil {
		if seen[node] {
			return nil
		}
		seen[node] = true
		switch node.Kind {
		case yaml.DocumentNode:
			if len(node.Content) != 1 {
				return nil
			}
			node = node.Content[0]
		case yaml.AliasNode:
			node = node.Alias
		default:
			return node
		}
	}
	return nil
}

func mappingChildPath(parent, key string) string {
	return parent + "[" + strconv.Quote(key) + "]"
}
