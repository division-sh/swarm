package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/durabledata"
)

// fileImportShape is the CLI's view of the admitted, exact-bundle import shape.
// Its fields are populated only from the selected store's compiled projection.
type fileImportShape struct {
	BusinessKey string
	Fields      map[string]fileImportField
}

type fileImportField struct {
	Text     bool
	Optional bool
}

type fileAssignment struct {
	Field string
	Path  string
}

type resolvedDataOperand struct {
	Declaration durabledata.DeclarationSummary
	Field       string // empty means an ordinary JSONL operand
	Path        string
}

func loadFileImportShape(ctx context.Context, client *cliAPIClient, bundleHash string, summary durabledata.DeclarationSummary) (fileImportShape, error) {
	var detail durabledata.ImportShape
	err := client.call(ctx, dataShowMethod, map[string]any{
		"view": "import_shape", "bundle_hash": bundleHash, "declaration": summary.Declaration,
		"schema_digest": summary.SchemaDigest,
	}, &detail)
	if err != nil {
		return fileImportShape{}, err
	}
	if err := detail.Validate(); err != nil {
		return fileImportShape{}, fmt.Errorf("data import shape is invalid: %w", err)
	}
	if detail.BundleHash != bundleHash || detail.Declaration != summary.Declaration || detail.SchemaDigest != summary.SchemaDigest {
		return fileImportShape{}, fmt.Errorf("data import shape contradicts selected declaration %s", dataDeclarationLabel(summary))
	}
	shape := fileImportShape{BusinessKey: detail.BusinessKey, Fields: make(map[string]fileImportField, len(detail.Fields))}
	for _, field := range detail.Fields {
		shape.Fields[field.Name] = fileImportField{Text: field.Text, Optional: !field.Required}
	}
	return shape, nil
}

func loadRunCreationRequestBinding(ctx context.Context, client *cliAPIClient, runID string) (*durabledata.RunCreationRequestBinding, error) {
	var binding durabledata.RunCreationRequestBinding
	err := client.call(ctx, dataShowMethod, map[string]any{
		"view": "operation", "operation_ref": map[string]any{"kind": "run_creation", "run_id": runID},
		"detail": "request_binding",
	}, &binding)
	if err != nil {
		var rpc *jsonRPCError
		if errors.As(err, &rpc) && applicationErrorCode(rpc.Data) == string(durabledata.CodeOperationMissing) {
			return nil, nil
		}
		return nil, err
	}
	if err := binding.Validate(); err != nil || binding.RunID != runID {
		return nil, fmt.Errorf("run %s request binding is invalid or contradictory: %v", runID, err)
	}
	return &binding, nil
}

func isRunCreationInvocationConflict(err error) bool {
	var rpc *jsonRPCError
	if !errors.As(err, &rpc) {
		return false
	}
	switch applicationErrorCode(rpc.Data) {
	case string(durabledata.CodeInvocationConflict), "IDEMPOTENCY_CONFLICT":
		return true
	default:
		return false
	}
}

func resolveHeadPinRefs(ctx context.Context, client *cliAPIClient, bundleHash string, pins []string) (map[string]bool, error) {
	result := make(map[string]bool)
	if len(pins) == 0 {
		return result, nil
	}
	declarations, err := listDataDeclarations(ctx, client, bundleHash)
	if err != nil {
		return nil, err
	}
	for _, raw := range pins {
		name, selector, err := splitDataVersionSelector(raw, false)
		if err != nil {
			return nil, err
		}
		declaration, err := resolveDataDeclarationFromList(bundleHash, name, declarations)
		if err != nil {
			return nil, err
		}
		result[declaration.Declaration.Key()] = selector == "head"
	}
	return result, nil
}

func validateRunDataBindingSchemas(ctx context.Context, client *cliAPIClient, binding durabledata.RunCreationRequestBinding) error {
	if len(binding.Imports) == 0 {
		return nil
	}
	declarations, err := listDataDeclarations(ctx, client, binding.BundleHash)
	if err != nil {
		return err
	}
	byRef := make(map[string]durabledata.DeclarationSummary, len(declarations))
	for _, declaration := range declarations {
		byRef[declaration.Declaration.Key()] = declaration
	}
	for _, original := range binding.Imports {
		current, found := byRef[original.Declaration.Key()]
		if !found || current.Declaration != original.Declaration || current.SchemaDigest != original.SchemaDigest {
			return fmt.Errorf("run %s import %s contradicts its permanent schema binding", binding.RunID, original.Declaration.Key())
		}
	}
	return nil
}

// resolveRunDataOperand checks both grammar interpretations before reading any
// host path. A dotted event name is never silently reinterpreted as a field.
func resolveRunDataOperand(raw string, declarations []durabledata.DeclarationSummary, eligible func(durabledata.DeclarationSummary, string) (bool, error)) (resolvedDataOperand, error) {
	selector, path, ok := strings.Cut(raw, "=")
	if !ok || strings.TrimSpace(selector) == "" || strings.TrimSpace(path) == "" {
		return resolvedDataOperand{}, fmt.Errorf("--data must be event=file.jsonl or event.field=file-or-directory")
	}
	var matches []resolvedDataOperand
	var fieldPrefix bool
	for _, declaration := range declarations {
		labels := []string{declaration.LocalName, dataDeclarationLabel(declaration)}
		for _, label := range labels {
			if selector == label {
				matches = append(matches, resolvedDataOperand{Declaration: declaration, Path: path})
				break
			}
		}
		for _, label := range labels {
			prefix := label + "."
			if !strings.HasPrefix(selector, prefix) {
				continue
			}
			fieldPrefix = true
			field := strings.TrimPrefix(selector, prefix)
			admitted, err := eligible(declaration, field)
			if err != nil {
				return resolvedDataOperand{}, err
			}
			if admitted {
				matches = append(matches, resolvedDataOperand{Declaration: declaration, Field: field, Path: path})
			}
			break
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		labels := make([]string, 0, len(matches))
		for _, candidate := range matches {
			label := dataDeclarationLabel(candidate.Declaration)
			if candidate.Field != "" {
				label += "." + candidate.Field + " (file field)"
			} else {
				label += " (JSONL event)"
			}
			labels = append(labels, label)
		}
		sort.Strings(labels)
		return resolvedDataOperand{}, fmt.Errorf("data operand %q is ambiguous: %s; use standalone data import with separate event and field arguments, then pin", selector, strings.Join(labels, ", "))
	}
	if fieldPrefix {
		return resolvedDataOperand{}, fmt.Errorf("data operand %q does not name an admitted top-level text field", selector)
	}
	return resolvedDataOperand{}, fmt.Errorf("data operand %q does not name an exact selected declaration", selector)
}

func validateFileAssignments(name string, shape fileImportShape, assignments []fileAssignment) (map[string]fileAssignment, []string, error) {
	if len(assignments) == 0 {
		return nil, nil, fmt.Errorf("data %s requires at least one field assignment", name)
	}
	selected := make(map[string]fileAssignment, len(assignments))
	for _, item := range assignments {
		field, ok := shape.Fields[item.Field]
		if !ok || !field.Text {
			return nil, nil, fmt.Errorf("data %s field %q is not an admitted top-level text field", name, item.Field)
		}
		if item.Field == shape.BusinessKey {
			return nil, nil, fmt.Errorf("data %s key field %q is supplied by directory filenames, not a field assignment", name, item.Field)
		}
		if _, exists := selected[item.Field]; exists {
			return nil, nil, fmt.Errorf("data %s repeats field %q", name, item.Field)
		}
		selected[item.Field] = item
	}
	allFields := make([]string, 0, len(shape.Fields))
	for field := range shape.Fields {
		allFields = append(allFields, field)
	}
	sort.Strings(allFields)
	for _, field := range allFields {
		entry := shape.Fields[field]
		if !entry.Optional && field != shape.BusinessKey {
			if !entry.Text {
				return nil, nil, fmt.Errorf("data %s requires non-text field %q; import complete JSONL rows instead", name, field)
			}
			if _, ok := selected[field]; !ok {
				return nil, nil, fmt.Errorf("data %s requires field %q", name, field)
			}
		}
	}
	return selected, allFields, nil
}

func lowerFileAssignments(root InvocationRoot, name string, shape fileImportShape, assignments []fileAssignment) ([]byte, error) {
	selected, allFields, err := validateFileAssignments(name, shape, assignments)
	if err != nil {
		return nil, err
	}
	fields := make([]string, 0, len(selected))
	for field := range selected {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	paths := make(map[string]string, len(fields))
	directory := false
	for index, field := range fields {
		path := root.Resolve(selected[field].Path)
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("data %s field %q: stat %s: %w", name, field, path, err)
		}
		if !info.Mode().IsRegular() && !info.IsDir() {
			return nil, fmt.Errorf("data %s field %q path %s must be a regular file or directory, not a symlink or special file", name, field, path)
		}
		if index == 0 {
			directory = info.IsDir()
		} else if directory != info.IsDir() {
			return nil, fmt.Errorf("data %s cannot mix file and directory field assignments", name)
		}
		paths[field] = path
	}
	if directory && shape.BusinessKey == "" {
		return nil, fmt.Errorf("data %s needs a declared required text key to import a directory", name)
	}
	if directory {
		key := shape.Fields[shape.BusinessKey]
		if !key.Text || key.Optional {
			return nil, fmt.Errorf("data %s directory key %q is not a declared required text field", name, shape.BusinessKey)
		}
	}
	if !directory && shape.BusinessKey != "" {
		return nil, fmt.Errorf("data %s is keyed by %s; import a directory (filenames become keys) or a JSONL row", name, shape.BusinessKey)
	}
	rows := make([]map[string]string, 0, 1)
	rawBytes := 0
	if !directory {
		row := make(map[string]string, len(fields))
		for _, field := range fields {
			content, err := readFileAssignment(paths[field])
			if err != nil {
				return nil, fmt.Errorf("data %s field %q: %w", name, field, err)
			}
			rawBytes += len(content)
			if rawBytes > durabledata.MaxDecodedImportBytes {
				return nil, fmt.Errorf("data %s field contents have %d bytes; decoded import limit is %d bytes", name, rawBytes, durabledata.MaxDecodedImportBytes)
			}
			row[field] = content
		}
		rows = append(rows, row)
	} else {
		byKey := make(map[string]map[string]string)
		for _, field := range fields {
			entries, err := readDirectoryAssignment(paths[field], durabledata.MaxDecodedImportBytes-rawBytes)
			if err != nil {
				return nil, fmt.Errorf("data %s field %q: %w", name, field, err)
			}
			for key, content := range entries {
				rawBytes += len(content)
				row := byKey[key]
				if row == nil {
					row = map[string]string{shape.BusinessKey: key}
					byKey[key] = row
				}
				row[field] = content
			}
		}
		keys := make([]string, 0, len(byKey))
		for key := range byKey {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			row := byKey[key]
			for _, field := range allFields {
				entry := shape.Fields[field]
				if field != shape.BusinessKey && !entry.Optional {
					if _, ok := row[field]; !ok {
						return nil, fmt.Errorf("data %s key %q is missing required field %q", name, key, field)
					}
				}
			}
			rows = append(rows, row)
		}
	}
	if len(rows) > durabledata.MaxResourceRows {
		return nil, fmt.Errorf("data %s has %d rows; limit is %d", name, len(rows), durabledata.MaxResourceRows)
	}
	if len(rows) == 0 {
		return []byte{}, nil
	}
	var out bytes.Buffer
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("data %s: encode row: %w", name, err)
		}
		if out.Len()+len(encoded)+1 > durabledata.MaxDecodedImportBytes {
			return nil, fmt.Errorf("data %s generated JSONL has %d bytes across %d rows; decoded import limit is %d bytes", name, out.Len()+len(encoded)+1, len(rows), durabledata.MaxDecodedImportBytes)
		}
		out.Write(encoded)
		out.WriteByte('\n')
	}
	return out.Bytes(), nil
}

func readFileAssignment(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular file", path)
	}
	content, err := io.ReadAll(io.LimitReader(file, durabledata.MaxDecodedImportBytes+1))
	if err != nil {
		return "", err
	}
	if len(content) > durabledata.MaxDecodedImportBytes {
		return "", fmt.Errorf("%s has %d bytes; decoded import limit is %d bytes", path, len(content), durabledata.MaxDecodedImportBytes)
	}
	if !utf8.Valid(content) {
		return "", fmt.Errorf("%s must contain valid UTF-8 text", path)
	}
	return string(content), nil
}

func readDirectoryAssignment(path string, remainingBytes int) (map[string]string, error) {
	directory, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	names, err := directory.Readdirnames(durabledata.MaxResourceRows + 1)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(names) > durabledata.MaxResourceRows {
		return nil, fmt.Errorf("directory %s has more than %d files", path, durabledata.MaxResourceRows)
	}
	sort.Strings(names)
	rows := make(map[string]string, len(names))
	seen := make(map[string]string, len(names))
	readBytes := 0
	for _, filename := range names {
		if !utf8.ValidString(filename) {
			return nil, fmt.Errorf("directory %s contains a non-UTF-8 filename", path)
		}
		full := filepath.Join(path, filename)
		info, err := os.Lstat(full)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("directory %s entry %q must be a regular non-symlink file", path, filename)
		}
		key := strings.TrimSuffix(filename, filepath.Ext(filename))
		if key == "" || len(key) > durabledata.MaxBusinessKeyBytes {
			return nil, fmt.Errorf("directory %s filename %q has invalid or over-limit key (%d bytes; limit %d)", path, filename, len(key), durabledata.MaxBusinessKeyBytes)
		}
		if prior, exists := seen[key]; exists {
			return nil, fmt.Errorf("directory %s has duplicate stem %q in %q and %q", path, key, prior, filename)
		}
		seen[key] = filename
		content, err := readFileAssignment(full)
		if err != nil {
			return nil, fmt.Errorf("directory %s entry %q: %w", path, filename, err)
		}
		readBytes += len(content)
		if readBytes > remainingBytes {
			return nil, fmt.Errorf("directory %s field contents exceed remaining decoded import budget: %d bytes read, %d bytes remain", path, readBytes, remainingBytes)
		}
		rows[key] = content
	}
	return rows, nil
}

// rebindRunDataEnvelope replaces only the original implicit import-head fact.
// Explicit pins and input bytes remain the caller's current intent and must
// agree with the permanent request before the store decides exact replay.
func rebindRunDataEnvelope(envelope map[string]any, binding durabledata.RunCreationRequestBinding, headPins map[string]bool) error {
	imports, ok := envelope["imports"].([]any)
	if !ok || len(imports) != len(binding.Imports) {
		return fmt.Errorf("run %s import set contradicts its permanent request", binding.RunID)
	}
	pins, ok := envelope["pins"].([]any)
	if !ok || len(pins) != len(binding.Pins) {
		return fmt.Errorf("run %s pin set contradicts its permanent request", binding.RunID)
	}
	for index, raw := range imports {
		item, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("run %s has malformed prepared import", binding.RunID)
		}
		original := binding.Imports[index]
		if item["declaration"] != original.Declaration || item["source_invocation_id"] != original.SourceInvocationID {
			return fmt.Errorf("run %s import %d contradicts its permanent request", binding.RunID, index)
		}
		item["expected_head"] = original.ExpectedHead
	}
	for index, raw := range pins {
		item, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("run %s has malformed prepared pin", binding.RunID)
		}
		original := binding.Pins[index]
		if item["declaration"] != original.Declaration {
			return fmt.Errorf("run %s pin %d contradicts its permanent request", binding.RunID, index)
		}
		if item["version_id"] != original.VersionID {
			if !headPins[original.Declaration.Key()] {
				return fmt.Errorf("run %s explicit pin %d contradicts its permanent request", binding.RunID, index)
			}
			item["version_id"] = original.VersionID
		}
	}
	return nil
}
