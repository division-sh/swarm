package semanticvalue

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"unicode/utf8"
)

const MaxSafeInteger = 9007199254740991

type Kind uint8

const (
	KindNull Kind = iota
	KindBool
	KindNumber
	KindString
	KindArray
	KindObject
)

type Value struct {
	kind    Kind
	boolean bool
	number  float64
	text    string
	array   []Value
	object  []Member
}

type Member struct {
	name  string
	value Value
}

type ObjectEntry struct {
	Name  string
	Value Value
}

func Null() Value { return Value{} }

func Bool(value bool) Value {
	return Value{kind: KindBool, boolean: value}
}

func Number(value float64) (Value, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return Value{}, fmt.Errorf("semantic number must be finite")
	}
	if value == 0 && math.Signbit(value) {
		return Value{}, fmt.Errorf("semantic number cannot be negative zero")
	}
	if math.Trunc(value) == value && math.Abs(value) > MaxSafeInteger {
		return Value{}, fmt.Errorf("integer-valued semantic number is outside the I-JSON safe range")
	}
	return Value{kind: KindNumber, number: value}, nil
}

func String(value string) (Value, error) {
	if !utf8.ValidString(value) {
		return Value{}, fmt.Errorf("semantic string is not valid UTF-8")
	}
	return Value{kind: KindString, text: value}, nil
}

func MustString(value string) Value {
	out, err := String(value)
	if err != nil {
		panic(err)
	}
	return out
}

func Array(values []Value) Value {
	return Value{kind: KindArray, array: append([]Value(nil), values...)}
}

func Object(entries []ObjectEntry) (Value, error) {
	members := make([]Member, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if !utf8.ValidString(entry.Name) {
			return Value{}, fmt.Errorf("semantic object key is not valid UTF-8")
		}
		if _, exists := seen[entry.Name]; exists {
			return Value{}, fmt.Errorf("duplicate semantic object key %q", entry.Name)
		}
		seen[entry.Name] = struct{}{}
		members = append(members, Member{name: entry.Name, value: entry.Value})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].name < members[j].name })
	return Value{kind: KindObject, object: members}, nil
}

func ObjectFromMap(values map[string]Value) (Value, error) {
	entries := make([]ObjectEntry, 0, len(values))
	for name, value := range values {
		entries = append(entries, ObjectEntry{Name: name, Value: value})
	}
	return Object(entries)
}

func MustObject(values map[string]Value) Value {
	out, err := ObjectFromMap(values)
	if err != nil {
		panic(err)
	}
	return out
}

func EmptyObject() Value {
	return Value{kind: KindObject, object: []Member{}}
}

func (v Value) Kind() Kind { return v.kind }

func (v Value) Bool() (bool, bool) {
	return v.boolean, v.kind == KindBool
}

func (v Value) Number() (float64, bool) {
	return v.number, v.kind == KindNumber
}

func (v Value) String() (string, bool) {
	return v.text, v.kind == KindString
}

func (v Value) Len() int {
	switch v.kind {
	case KindArray:
		return len(v.array)
	case KindObject:
		return len(v.object)
	default:
		return 0
	}
}

func (v Value) At(index int) (Value, bool) {
	if v.kind != KindArray || index < 0 || index >= len(v.array) {
		return Value{}, false
	}
	return v.array[index], true
}

func (v Value) Lookup(name string) (Value, bool) {
	if v.kind != KindObject {
		return Value{}, false
	}
	index := sort.Search(len(v.object), func(i int) bool { return v.object[i].name >= name })
	if index >= len(v.object) || v.object[index].name != name {
		return Value{}, false
	}
	return v.object[index].value, true
}

func (v Value) Members() []ObjectEntry {
	if v.kind != KindObject {
		return nil
	}
	out := make([]ObjectEntry, len(v.object))
	for i, member := range v.object {
		out[i] = ObjectEntry{Name: member.name, Value: member.value}
	}
	return out
}

func (v Value) ObjectMap() (map[string]Value, bool) {
	if v.kind != KindObject {
		return nil, false
	}
	out := make(map[string]Value, len(v.object))
	for _, member := range v.object {
		out[member.name] = member.value
	}
	return out, true
}

func (v Value) With(name string, value Value) (Value, error) {
	if v.kind != KindObject {
		return Value{}, fmt.Errorf("semantic value is not an object")
	}
	entries := v.Members()
	for i := range entries {
		if entries[i].Name == name {
			entries[i].Value = value
			return Object(entries)
		}
	}
	entries = append(entries, ObjectEntry{Name: name, Value: value})
	return Object(entries)
}

func (v Value) Interface() any {
	switch v.kind {
	case KindNull:
		return nil
	case KindBool:
		return v.boolean
	case KindNumber:
		return v.number
	case KindString:
		return v.text
	case KindArray:
		out := make([]any, len(v.array))
		for i, value := range v.array {
			out[i] = value.Interface()
		}
		return out
	case KindObject:
		out := make(map[string]any, len(v.object))
		for _, member := range v.object {
			out[member.name] = member.value.Interface()
		}
		return out
	default:
		panic("invalid semantic value kind")
	}
}

func (v Value) Equal(other Value) bool {
	if v.kind != other.kind {
		return false
	}
	switch v.kind {
	case KindNull:
		return true
	case KindBool:
		return v.boolean == other.boolean
	case KindNumber:
		return v.number == other.number
	case KindString:
		return v.text == other.text
	case KindArray:
		if len(v.array) != len(other.array) {
			return false
		}
		for i := range v.array {
			if !v.array[i].Equal(other.array[i]) {
				return false
			}
		}
		return true
	case KindObject:
		if len(v.object) != len(other.object) {
			return false
		}
		for i := range v.object {
			if v.object[i].name != other.object[i].name || !v.object[i].value.Equal(other.object[i].value) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// MarshalJSON fails closed so semantic values cannot bypass the canonical JSON
// codec through an incidental encoding/json call.
func (Value) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("semanticvalue.Value must be encoded by canonicaljson")
}

// UnmarshalJSON fails closed so untrusted bytes cannot construct a semantic
// value without the canonical JSON codec's lexical checks.
func (*Value) UnmarshalJSON([]byte) error {
	return fmt.Errorf("semanticvalue.Value must be decoded by canonicaljson")
}

func NumberString(value Value) string {
	number, ok := value.Number()
	if !ok {
		return ""
	}
	return strconv.FormatFloat(number, 'g', -1, 64)
}

func IsJSONScalar(value Value) bool {
	switch value.Kind() {
	case KindNull, KindBool, KindNumber, KindString:
		return true
	default:
		return false
	}
}

var _ json.Marshaler = Value{}
var _ json.Unmarshaler = (*Value)(nil)
