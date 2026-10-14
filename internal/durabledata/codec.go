package durabledata

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

type CompiledVersion struct {
	Manifest        Manifest
	VersionID       VersionID
	CanonicalSchema []byte
	Rows            []Row
	CanonicalJSONL  []byte
}

func CompileJSONL(declaration DeclarationRef, schema map[string]any, businessKeyField string, input []byte) (CompiledVersion, []ValidationDefect) {
	return compileJSONL(declaration, schema, businessKeyField, input, true)
}

// CompileStoredJSONL revalidates admitted canonical bytes. The import-wire
// limit does not constrain canonical expansion after admission.
func CompileStoredJSONL(declaration DeclarationRef, schema map[string]any, businessKeyField string, input []byte) (CompiledVersion, []ValidationDefect) {
	return compileJSONL(declaration, schema, businessKeyField, input, false)
}

func compileJSONL(declaration DeclarationRef, schema map[string]any, businessKeyField string, input []byte, importWire bool) (CompiledVersion, []ValidationDefect) {
	if defects := validateResourceInput(declaration, input, importWire); len(defects) > 0 {
		return CompiledVersion{}, defects
	}
	businessKeyField = strings.TrimSpace(businessKeyField)
	canonicalSchema, err := canonicalResourceSchema(schema, businessKeyField)
	if err != nil {
		return CompiledVersion{}, []ValidationDefect{{Code: "invalid_schema", Message: err.Error()}}
	}
	lines, defects := resourceInputLines(input)
	if len(defects) > 0 {
		return CompiledVersion{}, defects
	}
	rows, defects := compileResourceRows(lines, schema, businessKeyField)
	if len(defects) > 0 {
		return CompiledVersion{}, defects
	}
	return compileResourceVersion(declaration, canonicalSchema, businessKeyField, rows)
}

func canonicalBusinessKey(value semanticvalue.Value) ([]byte, error) {
	switch value.Kind() {
	case semanticvalue.KindBool, semanticvalue.KindNumber, semanticvalue.KindString:
	default:
		return nil, fmt.Errorf("business key must be a non-null scalar")
	}
	canonical, err := canonicaljson.Encode(value)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaxBusinessKeyBytes {
		return nil, fmt.Errorf("business key exceeds %d bytes", MaxBusinessKeyBytes)
	}
	return canonical, nil
}

func normalizeImportOmissions(value semanticvalue.Value, schema map[string]any) (semanticvalue.Value, error) {
	switch value.Kind() {
	case semanticvalue.KindObject:
		properties, _ := schema["properties"].(map[string]any)
		required := schemaRequiredSet(schema)
		entries := make([]semanticvalue.ObjectEntry, 0, value.Len())
		for _, member := range value.Members() {
			property, _ := properties[member.Name].(map[string]any)
			if member.Value.Kind() == semanticvalue.KindNull {
				if _, isRequired := required[member.Name]; !isRequired && property != nil {
					continue
				}
			}
			if _, declared := properties[member.Name]; !declared && member.Value.Kind() != semanticvalue.KindNull {
				property, _ = schema["additionalProperties"].(map[string]any)
			}
			normalized, err := normalizeImportOmissions(member.Value, property)
			if err != nil {
				return semanticvalue.Value{}, err
			}
			entries = append(entries, semanticvalue.ObjectEntry{Name: member.Name, Value: normalized})
		}
		return semanticvalue.Object(entries)
	case semanticvalue.KindArray:
		items, _ := schema["items"].(map[string]any)
		values := make([]semanticvalue.Value, 0, value.Len())
		for index := 0; index < value.Len(); index++ {
			item, _ := value.At(index)
			normalized, err := normalizeImportOmissions(item, items)
			if err != nil {
				return semanticvalue.Value{}, err
			}
			values = append(values, normalized)
		}
		return semanticvalue.Array(values), nil
	default:
		return value, nil
	}
}

func schemaRequiredSet(schema map[string]any) map[string]struct{} {
	out := map[string]struct{}{}
	switch values := schema["required"].(type) {
	case []string:
		for _, value := range values {
			out[value] = struct{}{}
		}
	case []any:
		for _, value := range values {
			if text, ok := value.(string); ok {
				out[text] = struct{}{}
			}
		}
	}
	return out
}

func writeDigestField(dst interface{ Write([]byte) (int, error) }, value []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	_, _ = dst.Write(size[:])
	_, _ = dst.Write(value)
}

func writeDigestUint64(dst interface{ Write([]byte) (int, error) }, value uint64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	_, _ = dst.Write(raw[:])
}

func Delta(base, candidate []Row, keyed bool) (DeltaSummary, []BusinessKey, []BusinessKey, []BusinessKey) {
	if !keyed {
		baseCounts := canonicalMultiplicity(base)
		candidateCounts := canonicalMultiplicity(candidate)
		var added, removed uint64
		for row, count := range candidateCounts {
			if previous := baseCounts[row]; count > previous {
				added += count - previous
			}
		}
		for row, count := range baseCounts {
			if next := candidateCounts[row]; count > next {
				removed += count - next
			}
		}
		orderChanged := added == 0 && removed == 0 && !sameCanonicalOrder(base, candidate)
		return DeltaSummary{Added: added, Removed: removed, OrderChanged: orderChanged}, nil, nil, nil
	}
	baseByKey := make(map[BusinessKey][]byte, len(base))
	for _, row := range base {
		baseByKey[row.BusinessKey] = row.Canonical
	}
	candidateByKey := make(map[BusinessKey][]byte, len(candidate))
	for _, row := range candidate {
		candidateByKey[row.BusinessKey] = row.Canonical
	}
	var added, removed, changed []BusinessKey
	for key, raw := range candidateByKey {
		previous, exists := baseByKey[key]
		switch {
		case !exists:
			added = append(added, key)
		case !bytes.Equal(previous, raw):
			changed = append(changed, key)
		}
	}
	for key := range baseByKey {
		if _, exists := candidateByKey[key]; !exists {
			removed = append(removed, key)
		}
	}
	sortKeys := func(keys []BusinessKey) {
		sort.Slice(keys, func(i, j int) bool { return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0 })
	}
	sortKeys(added)
	sortKeys(removed)
	sortKeys(changed)
	return DeltaSummary{Added: uint64(len(added)), Removed: uint64(len(removed)), Changed: uint64(len(changed))}, added, removed, changed
}

func canonicalMultiplicity(rows []Row) map[string]uint64 {
	out := make(map[string]uint64, len(rows))
	for _, row := range rows {
		out[string(row.Canonical)]++
	}
	return out
}

func sameCanonicalOrder(left, right []Row) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index].Canonical, right[index].Canonical) {
			return false
		}
	}
	return true
}
