// Package formatproof owns the single approved formatting-only finite successor.
package formatproof

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/format"
	"go/parser"
	"go/scanner"
	"go/token"
	"io"
	"reflect"
)

const (
	Rule       = "f1-go-horizontal-v1"
	Source     = "internal/runtime/testfixtures/canonicalrouting/notify_all_children.go"
	Origin     = "16534f83b35657cf53278d6bfe5223855ae89dd89240e370540104153312123c"
	Ledger     = "scripts/rewrite-stages-2566/finite-fixtures.json"
	Successors = "scripts/rewrite-stages-2566/accounting-successors.json"
)

type Edit struct {
	Offset int    `json:"offset"`
	Before string `json:"before"`
	After  string `json:"after"`
}

type Record struct {
	Rule         string `json:"rule"`
	File         string `json:"file"`
	Ledger       string `json:"ledger"`
	Origin       string `json:"origin"`
	Previous     string `json:"previous"`
	Role         string `json:"role"`
	Multiplicity int    `json:"multiplicity"`
	BeforeSHA256 string `json:"before_sha256"`
	AfterSHA256  string `json:"after_sha256"`
	Before       string `json:"before"`
	After        string `json:"after"`
	Edits        []Edit `json:"edits"`
}

type History struct {
	Version int      `json:"version"`
	Records []Record `json:"records"`
}

func Digest(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }

func RecordID(record Record) string {
	raw, _ := json.Marshal(record)
	return Digest(raw)
}

// Decode refuses duplicate keys as well as fields the typed owner does not know.
func Decode(raw []byte, out any) error {
	keys := json.NewDecoder(bytes.NewReader(raw))
	if err := uniqueJSON(keys); err != nil {
		return err
	}
	if _, err := keys.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON input")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := exactJSONNames(raw, reflect.TypeOf(out)); err != nil {
		return err
	}
	if err := decoder.Decode(out); err != nil {
		return err
	}
	return nil
}

// encoding/json otherwise accepts case-insensitive aliases for struct members.
func exactJSONNames(raw []byte, typ reflect.Type) error {
	if typ == nil {
		return fmt.Errorf("missing JSON target")
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Struct:
		return exactJSONMembers(raw, typ)
	case reflect.Slice, reflect.Array:
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := exactJSONNames(value, typ.Elem()); err != nil {
				return err
			}
		}
	case reflect.Map:
		var values map[string]json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := exactJSONNames(value, typ.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}

func exactJSONMembers(raw []byte, typ reflect.Type) error {
	if !bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{")) {
		return nil
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		return err
	}
	fields := map[string]reflect.Type{}
	exactJSONFields(typ, fields)
	for name, value := range members {
		field, ok := fields[name]
		if !ok {
			return fmt.Errorf("unknown JSON field %q", name)
		}
		if err := exactJSONNames(value, field); err != nil {
			return err
		}
	}
	return nil
}

func exactJSONFields(typ reflect.Type, fields map[string]reflect.Type) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		name := field.Tag.Get("json")
		for j, b := range name {
			if b == ',' {
				name = name[:j]
				break
			}
		}
		if name == "-" {
			continue
		}
		inner := field.Type
		if inner.Kind() == reflect.Pointer {
			inner = inner.Elem()
		}
		if field.Anonymous && name == "" && inner.Kind() == reflect.Struct {
			exactJSONFields(inner, fields)
			continue
		}
		if name == "" {
			name = field.Name
		}
		fields[name] = field.Type
	}
}

func uniqueJSON(decoder *json.Decoder) error {
	value, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, compound := value.(json.Delim)
	if !compound {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate or invalid JSON key %v", key)
			}
			seen[name] = true
			if err := uniqueJSON(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := uniqueJSON(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	_, err = decoder.Token()
	return err
}

func Read(raw []byte) (History, error) {
	var history History
	if err := Decode(raw, &history); err != nil {
		return history, err
	}
	_, _, err := history.Endpoint()
	return history, err
}

// Endpoint validates the complete origin-to-current chain, never a scalar fallback.
func (history History) Endpoint() ([]byte, string, error) {
	if history.Version != 1 || history.Records == nil {
		return nil, "", fmt.Errorf("unknown or incomplete finite successor schema")
	}
	previous, source := Origin, Origin
	var endpoint []byte
	seen := map[string]bool{Origin: true}
	for _, record := range history.Records {
		if record.Rule != Rule || record.File != Source || record.Ledger != Ledger || record.Origin != Origin || record.Previous != previous || record.Role != "finite-fixture" || record.Multiplicity != 1 {
			return nil, "", fmt.Errorf("unclassified finite successor identity, role or predecessor")
		}
		if record.BeforeSHA256 != source || Digest([]byte(record.Before)) != source || Digest([]byte(record.After)) != record.AfterSHA256 || seen[record.AfterSHA256] {
			return nil, "", fmt.Errorf("missing, repeated or changed finite source")
		}
		if endpoint != nil && !bytes.Equal(endpoint, []byte(record.Before)) {
			return nil, "", fmt.Errorf("finite chain source discontinuity")
		}
		edits, err := FormattingEdits([]byte(record.Before), []byte(record.After))
		if err != nil || !reflect.DeepEqual(edits, record.Edits) {
			return nil, "", fmt.Errorf("finite successor lacks exact executable formatting provenance: %v", err)
		}
		endpoint, source, previous = []byte(record.After), record.AfterSHA256, RecordID(record)
		seen[source] = true
	}
	return endpoint, previous, nil
}

func (history History) Append(before, after []byte) (History, error) {
	endpoint, previous, err := history.Endpoint()
	if err != nil {
		return history, err
	}
	if endpoint == nil {
		if Digest(before) != Origin {
			return history, fmt.Errorf("unsupported finite origin")
		}
	} else if !bytes.Equal(endpoint, before) {
		return history, fmt.Errorf("stale finite predecessor")
	}
	edits, err := FormattingEdits(before, after)
	if err != nil {
		return history, err
	}
	record := Record{Rule: Rule, File: Source, Ledger: Ledger, Origin: Origin, Previous: previous, Role: "finite-fixture", Multiplicity: 1, BeforeSHA256: Digest(before), AfterSHA256: Digest(after), Before: string(before), After: string(after), Edits: edits}
	result := History{Version: 1, Records: append(append([]Record{}, history.Records...), record)}
	_, _, err = result.Endpoint()
	return result, err
}

type lexical struct {
	Token        token.Token
	Literal      string
	Line, Column int
}

func lex(raw []byte) ([]lexical, error) {
	files := token.NewFileSet()
	file := files.AddFile("source.go", -1, len(raw))
	var parseErr error
	var scan scanner.Scanner
	scan.Init(file, raw, func(_ token.Position, message string) { parseErr = fmt.Errorf("Go scanner: %s", message) }, scanner.ScanComments)
	var out []lexical
	for {
		position, kind, literal := scan.Scan()
		if kind == token.EOF {
			break
		}
		item := lexical{Token: kind, Literal: literal}
		if kind == token.COMMENT {
			location := file.Position(position)
			item.Line, item.Column = location.Line, location.Column
		}
		out = append(out, item)
	}
	return out, parseErr
}

func horizontal(b byte) bool { return b == ' ' || b == '\t' }

// FormattingEdits retains literals, every comment/directive and implicit semicolons.
// Reproducing the exact whitespace patch is separate from normalizing the syntax.
func FormattingEdits(before, after []byte) ([]Edit, error) {
	if bytes.Equal(before, after) {
		return nil, fmt.Errorf("formatting successor is a no-op")
	}
	for _, raw := range [][]byte{before, after} {
		if _, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.ParseComments|parser.AllErrors); err != nil {
			return nil, err
		}
	}
	oldTokens, err := lex(before)
	if err != nil {
		return nil, err
	}
	newTokens, err := lex(after)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(oldTokens, newTokens) {
		return nil, fmt.Errorf("tokens, literals, semicolons or comment/directive anchors changed")
	}
	oldFormat, err := format.Source(before)
	if err != nil {
		return nil, err
	}
	newFormat, err := format.Source(after)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(oldFormat, newFormat) {
		return nil, fmt.Errorf("formatted syntax changed")
	}
	var edits []Edit
	i, j := 0, 0
	for i < len(before) || j < len(after) {
		a, b := i, j
		for i < len(before) && horizontal(before[i]) {
			i++
		}
		for j < len(after) && horizontal(after[j]) {
			j++
		}
		if !bytes.Equal(before[a:i], after[b:j]) {
			edits = append(edits, Edit{Offset: a, Before: string(before[a:i]), After: string(after[b:j])})
		}
		if i == len(before) && j == len(after) {
			break
		}
		if i == len(before) || j == len(after) || before[i] != after[j] {
			return nil, fmt.Errorf("non-horizontal source byte changed")
		}
		i++
		j++
	}
	if len(edits) == 0 {
		return nil, fmt.Errorf("no explained formatting edit")
	}
	return edits, nil
}
