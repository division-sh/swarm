package durabledata

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

func validateResourceInput(declaration DeclarationRef, input []byte, importWire bool) []ValidationDefect {
	if err := declaration.Validate(); err != nil {
		return []ValidationDefect{{Code: "invalid_declaration", Message: err.Error()}}
	}
	if importWire && len(input) > MaxDecodedImportBytes {
		return []ValidationDefect{{Code: "decoded_import_too_large", Message: fmt.Sprintf("decoded input exceeds %d bytes", MaxDecodedImportBytes)}}
	}
	if !utf8.Valid(input) {
		return []ValidationDefect{{Code: "invalid_utf8", Message: "JSONL input is not valid UTF-8"}}
	}
	if bytes.HasPrefix(input, []byte{0xef, 0xbb, 0xbf}) {
		return []ValidationDefect{{Code: "bom_forbidden", Message: "JSONL input must not contain a UTF-8 BOM"}}
	}
	return nil
}

func canonicalResourceSchema(schema map[string]any, businessKeyField string) ([]byte, error) {
	canonicalOwner := eventschema.CanonicalAcceptanceSchema(schema)
	if businessKeyField != "" {
		canonicalOwner["x-swarm-dataset-key"] = businessKeyField
	}
	return canonicaljson.Bytes(canonicalOwner)
}

func resourceInputLines(input []byte) ([][]byte, []ValidationDefect) {
	lines := bytes.Split(input, []byte{'\n'})
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > MaxResourceRows {
		return nil, []ValidationDefect{{Code: "row_limit_exceeded", Message: fmt.Sprintf("row count exceeds %d", MaxResourceRows)}}
	}
	return lines, nil
}

func compileResourceRows(lines [][]byte, schema map[string]any, businessKeyField string) ([]Row, []ValidationDefect) {
	rows := make([]Row, 0, len(lines))
	defects := make([]ValidationDefect, 0)
	physicalRows := make(map[BusinessKey]uint64, len(lines))
	for index, raw := range lines {
		row, defect := compileResourceRow(raw, uint64(index+1), schema, businessKeyField, physicalRows)
		if defect != nil {
			defects = append(defects, *defect)
			continue
		}
		rows = append(rows, row)
	}
	return rows, defects
}

func compileResourceRow(raw []byte, line uint64, schema map[string]any, businessKeyField string, physicalRows map[BusinessKey]uint64) (Row, *ValidationDefect) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return Row{}, &ValidationDefect{Row: line, Path: "$", Code: "blank_line", Message: "blank JSONL rows are forbidden"}
	}
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return Row{}, &ValidationDefect{Row: line, Path: "$", Code: "invalid_json", Message: err.Error()}
	}
	if value.Kind() != semanticvalue.KindObject {
		return Row{}, &ValidationDefect{Row: line, Path: "$", Code: "row_not_object", Message: "each JSONL row must be one object"}
	}
	value, err = normalizeImportOmissions(value, schema)
	if err != nil {
		return Row{}, &ValidationDefect{Row: line, Path: "$", Code: "normalization_failed", Message: err.Error()}
	}
	object, _ := value.ObjectMap()
	projected := make(map[string]any, len(object))
	for name, member := range object {
		projected[name] = member.Interface()
	}
	if err := eventschema.ValidatePayloadAgainstSchema(schema, projected); err != nil {
		return Row{}, &ValidationDefect{Row: line, Path: "$", Code: "schema_rejected", Message: err.Error()}
	}
	// Schema refusal cannot reserve a key; a later row-size refusal still must.
	businessKey, defect := admitResourceBusinessKey(value, line, businessKeyField, physicalRows)
	if defect != nil {
		return Row{}, defect
	}
	canonical, err := canonicaljson.Encode(value)
	if err != nil {
		return Row{}, &ValidationDefect{Row: line, Path: "$", Code: "canonicalization_failed", Message: err.Error()}
	}
	if len(canonical) > MaxCanonicalRowBytes {
		return Row{}, &ValidationDefect{Row: line, Path: "$", Code: "row_too_large", Message: fmt.Sprintf("canonical row has %d bytes; limit is %d bytes", len(canonical), MaxCanonicalRowBytes)}
	}
	return Row{BusinessKey: businessKey, Canonical: canonical}, nil
}

func admitResourceBusinessKey(value semanticvalue.Value, line uint64, businessKeyField string, physicalRows map[BusinessKey]uint64) (BusinessKey, *ValidationDefect) {
	if businessKeyField == "" {
		return "", nil
	}
	keyValue, exists := value.Lookup(businessKeyField)
	if !exists {
		return "", &ValidationDefect{Row: line, Path: "$." + businessKeyField, Code: "business_key_missing", Message: "business key field is missing"}
	}
	keyBytes, err := canonicalBusinessKey(keyValue)
	if err != nil {
		return "", &ValidationDefect{Row: line, Path: "$." + businessKeyField, Code: "business_key_invalid", Message: err.Error()}
	}
	businessKey := BusinessKey(keyBytes)
	if previous, duplicate := physicalRows[businessKey]; duplicate {
		return "", &ValidationDefect{Row: line, Path: "$." + businessKeyField, Code: "duplicate_business_key", Message: fmt.Sprintf("business key %s duplicates physical row %d", keyBytes, previous)}
	}
	physicalRows[businessKey] = line
	return businessKey, nil
}

func compileResourceVersion(declaration DeclarationRef, canonicalSchema []byte, businessKeyField string, rows []Row) (CompiledVersion, []ValidationDefect) {
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
