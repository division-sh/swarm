package durabledata

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
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
	if err := declaration.Validate(); err != nil {
		return CompiledVersion{}, []ValidationDefect{{Code: "invalid_declaration", Message: err.Error()}}
	}
	if len(input) > MaxDecodedImportBytes {
		return CompiledVersion{}, []ValidationDefect{{Code: "decoded_import_too_large", Message: fmt.Sprintf("decoded input exceeds %d bytes", MaxDecodedImportBytes)}}
	}
	if !utf8.Valid(input) {
		return CompiledVersion{}, []ValidationDefect{{Code: "invalid_utf8", Message: "JSONL input is not valid UTF-8"}}
	}
	if bytes.HasPrefix(input, []byte{0xef, 0xbb, 0xbf}) {
		return CompiledVersion{}, []ValidationDefect{{Code: "bom_forbidden", Message: "JSONL input must not contain a UTF-8 BOM"}}
	}
	businessKeyField = strings.TrimSpace(businessKeyField)
	canonicalOwner := eventschema.CanonicalAcceptanceSchema(schema)
	if businessKeyField != "" {
		canonicalOwner["x-swarm-dataset-key"] = businessKeyField
	}
	canonicalSchema, err := canonicaljson.Bytes(canonicalOwner)
	if err != nil {
		return CompiledVersion{}, []ValidationDefect{{Code: "invalid_schema", Message: err.Error()}}
	}
	lines := bytes.Split(input, []byte{'\n'})
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > MaxResourceRows {
		return CompiledVersion{}, []ValidationDefect{{Code: "row_limit_exceeded", Message: fmt.Sprintf("row count exceeds %d", MaxResourceRows)}}
	}
	rows := make([]Row, 0, len(lines))
	defects := make([]ValidationDefect, 0)
	physicalRows := make(map[BusinessKey]uint64, len(lines))
	for index, raw := range lines {
		line := uint64(index + 1)
		if len(bytes.TrimSpace(raw)) == 0 {
			defects = append(defects, ValidationDefect{Row: line, Path: "$", Code: "blank_line", Message: "blank JSONL rows are forbidden"})
			continue
		}
		value, err := canonicaljson.Decode(raw)
		if err != nil {
			defects = append(defects, ValidationDefect{Row: line, Path: "$", Code: "invalid_json", Message: err.Error()})
			continue
		}
		if value.Kind() != semanticvalue.KindObject {
			defects = append(defects, ValidationDefect{Row: line, Path: "$", Code: "row_not_object", Message: "each JSONL row must be one object"})
			continue
		}
		value, err = normalizeImportOmissions(value, schema)
		if err != nil {
			defects = append(defects, ValidationDefect{Row: line, Path: "$", Code: "normalization_failed", Message: err.Error()})
			continue
		}
		object, _ := value.ObjectMap()
		projected := make(map[string]any, len(object))
		for name, member := range object {
			projected[name] = member.Interface()
		}
		if err := eventschema.ValidatePayloadAgainstSchema(schema, projected); err != nil {
			defects = append(defects, ValidationDefect{Row: line, Path: "$", Code: "schema_rejected", Message: err.Error()})
			continue
		}
		var businessKey BusinessKey
		if businessKeyField != "" {
			keyValue, exists := value.Lookup(businessKeyField)
			if !exists {
				defects = append(defects, ValidationDefect{Row: line, Path: "$." + businessKeyField, Code: "business_key_missing", Message: "business key field is missing"})
				continue
			}
			keyBytes, err := canonicalBusinessKey(keyValue)
			if err != nil {
				defects = append(defects, ValidationDefect{Row: line, Path: "$." + businessKeyField, Code: "business_key_invalid", Message: err.Error()})
				continue
			}
			businessKey = BusinessKey(keyBytes)
			if previous, duplicate := physicalRows[businessKey]; duplicate {
				defects = append(defects, ValidationDefect{Row: line, Path: "$." + businessKeyField, Code: "duplicate_business_key", Message: fmt.Sprintf("business key %s duplicates physical row %d", keyBytes, previous)})
				continue
			}
			physicalRows[businessKey] = line
		}
		canonical, err := canonicaljson.Encode(value)
		if err != nil {
			defects = append(defects, ValidationDefect{Row: line, Path: "$", Code: "canonicalization_failed", Message: err.Error()})
			continue
		}
		if len(canonical) > MaxCanonicalRowBytes {
			defects = append(defects, ValidationDefect{Row: line, Path: "$", Code: "row_too_large", Message: fmt.Sprintf("canonical row exceeds %d bytes", MaxCanonicalRowBytes)})
			continue
		}
		rows = append(rows, Row{BusinessKey: businessKey, Canonical: canonical})
	}
	if len(defects) > 0 {
		return CompiledVersion{}, defects
	}
	if businessKeyField != "" {
		sort.Slice(rows, func(i, j int) bool {
			return bytes.Compare([]byte(rows[i].BusinessKey), []byte(rows[j].BusinessKey)) < 0
		})
	}
	contentHash := sha256.New()
	_, _ = contentHash.Write([]byte("swarm.resource.content.v1"))
	_, _ = contentHash.Write([]byte{0})
	if businessKeyField == "" {
		_, _ = contentHash.Write([]byte("positional"))
	} else {
		_, _ = contentHash.Write([]byte("keyed"))
	}
	_, _ = contentHash.Write([]byte{0})
	var jsonl bytes.Buffer
	for index := range rows {
		rows[index].Ordinal = uint64(index + 1)
		writeDigestUint64(contentHash, rows[index].Ordinal)
		if businessKeyField != "" {
			writeDigestField(contentHash, []byte(rows[index].BusinessKey))
		}
		writeDigestField(contentHash, rows[index].Canonical)
		_, _ = jsonl.Write(rows[index].Canonical)
		_ = jsonl.WriteByte('\n')
	}
	contentDigest := ContentDigest("resource-content-v1:sha256:" + hex.EncodeToString(contentHash.Sum(nil)))
	manifest := Manifest{
		ManifestFormat: ManifestFormat,
		Declaration:    declaration,
		SchemaDigest:   SchemaDigestFor(canonicalSchema),
		RowCodec:       RowCodec,
		ContentDigest:  contentDigest,
		RowCount:       uint64(len(rows)),
	}
	versionID, err := manifest.VersionID()
	if err != nil {
		return CompiledVersion{}, []ValidationDefect{{Code: "manifest_invalid", Message: err.Error()}}
	}
	canonicalJSONL := jsonl.Bytes()
	if canonicalJSONL == nil {
		canonicalJSONL = []byte{}
	}
	return CompiledVersion{Manifest: manifest, VersionID: versionID, CanonicalSchema: canonicalSchema, Rows: rows, CanonicalJSONL: canonicalJSONL}, nil
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
