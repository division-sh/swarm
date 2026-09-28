package cliapp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
)

func writeFileImportTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func decodeFileImportTestRows(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	rows := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

func TestFileRowLoweringPreservesTextAndOptionalOmission(t *testing.T) {
	root := mustInvocationRootForTest(t.TempDir())
	writeFileImportTestFile(t, root.Resolve("resume.md"), "  hello\n\n\x00world\n")
	shape := fileImportShape{Fields: map[string]fileImportField{
		"resume": {Text: true}, "cover": {Text: true, Optional: true},
	}}
	got, err := lowerFileAssignments(root, "application", shape, []fileAssignment{{Field: "resume", Path: "resume.md"}})
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeFileImportTestRows(t, got)
	if len(rows) != 1 || rows[0]["resume"] != "  hello\n\n\x00world\n" {
		t.Fatalf("rows = %#v", rows)
	}
	if _, exists := rows[0]["cover"]; exists {
		t.Fatalf("optional cover was materialized: %#v", rows[0])
	}
}

func TestFileRowLoweringDirectoryUnionAndStemRules(t *testing.T) {
	root := mustInvocationRootForTest(t.TempDir())
	for _, name := range []string{"resumes", "covers"} {
		if err := os.Mkdir(root.Resolve(name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeFileImportTestFile(t, root.Resolve("resumes/zeta.md"), "z")
	writeFileImportTestFile(t, root.Resolve("resumes/alpha.txt"), "a")
	writeFileImportTestFile(t, root.Resolve("covers/alpha.md"), "ca")
	shape := fileImportShape{BusinessKey: "candidate_id", Fields: map[string]fileImportField{
		"candidate_id": {Text: true}, "resume": {Text: true}, "cover": {Text: true, Optional: true},
	}}
	assignments := []fileAssignment{{Field: "cover", Path: "covers"}, {Field: "resume", Path: "resumes"}}
	got, err := lowerFileAssignments(root, "application", shape, assignments)
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeFileImportTestRows(t, got)
	if len(rows) != 2 || rows[0]["candidate_id"] != "alpha" || rows[0]["cover"] != "ca" || rows[1]["candidate_id"] != "zeta" {
		t.Fatalf("joined rows = %#v", rows)
	}
	if _, exists := rows[1]["cover"]; exists {
		t.Fatalf("missing optional field not omitted: %#v", rows[1])
	}
	writeFileImportTestFile(t, root.Resolve("covers/orphan.md"), "orphan")
	if _, err := lowerFileAssignments(root, "application", shape, assignments); err == nil || !strings.Contains(err.Error(), "orphan") || !strings.Contains(err.Error(), "resume") {
		t.Fatalf("orphan required-field error = %v", err)
	}
	writeFileImportTestFile(t, root.Resolve("resumes/alpha.md"), "duplicate")
	if _, err := lowerFileAssignments(root, "application", shape, assignments); err == nil || !strings.Contains(err.Error(), "alpha.txt") || !strings.Contains(err.Error(), "alpha.md") {
		t.Fatalf("within-field duplicate error = %v", err)
	}
}

func TestFileRowLoweringRefusesContradictoryShapesAndPaths(t *testing.T) {
	root := mustInvocationRootForTest(t.TempDir())
	writeFileImportTestFile(t, root.Resolve("resume.md"), "a")
	if err := os.Mkdir(root.Resolve("covers"), 0o700); err != nil {
		t.Fatal(err)
	}
	shape := fileImportShape{BusinessKey: "key", Fields: map[string]fileImportField{
		"key": {Text: true}, "resume": {Text: true}, "cover": {Text: true, Optional: true},
	}}
	for _, test := range []struct {
		name        string
		assignments []fileAssignment
		want        string
	}{
		{"keyed singleton", []fileAssignment{{"resume", "resume.md"}}, "keyed by key"},
		{"mixed shapes", []fileAssignment{{"resume", "resume.md"}, {"cover", "covers"}}, "cannot mix"},
		{"key assignment", []fileAssignment{{"resume", "resume.md"}, {"key", "resume.md"}}, "key field"},
		{"duplicate field", []fileAssignment{{"resume", "resume.md"}, {"resume", "resume.md"}}, "repeats field"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := lowerFileAssignments(root, "application", shape, test.assignments); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
	if err := os.Symlink("resume.md", root.Resolve("link.md")); err != nil {
		t.Fatal(err)
	}
	keyless := fileImportShape{Fields: map[string]fileImportField{"resume": {Text: true}}}
	if _, err := lowerFileAssignments(root, "application", keyless, []fileAssignment{{"resume", "link.md"}}); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink error = %v", err)
	}
	if err := os.WriteFile(root.Resolve("invalid.txt"), []byte{0xff}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := lowerFileAssignments(root, "application", keyless, []fileAssignment{{"resume", "invalid.txt"}}); err == nil || !strings.Contains(err.Error(), "UTF-8") {
		t.Fatalf("invalid UTF-8 error = %v", err)
	}
	if err := os.Mkdir(root.Resolve("hostile"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(root.Resolve("resume.md"), root.Resolve("hostile/link.md")); err != nil {
		t.Fatal(err)
	}
	if _, err := lowerFileAssignments(root, "application", shape, []fileAssignment{{"resume", "hostile"}}); err == nil || !strings.Contains(err.Error(), "regular non-symlink") {
		t.Fatalf("directory symlink error = %v", err)
	}
	if err := os.Remove(root.Resolve("hostile/link.md")); err != nil {
		t.Fatal(err)
	}
	writeFileImportTestFile(t, root.Resolve("hostile/.md"), "empty stem")
	if _, err := lowerFileAssignments(root, "application", shape, []fileAssignment{{"resume", "hostile"}}); err == nil || !strings.Contains(err.Error(), "invalid or over-limit key") {
		t.Fatalf("empty key stem error = %v", err)
	}
}

func TestFileRowLoweringEmptyDirectoryAndEscapingBound(t *testing.T) {
	root := mustInvocationRootForTest(t.TempDir())
	writeFileImportTestFile(t, root.Resolve("empty.txt"), "")
	keyless := fileImportShape{Fields: map[string]fileImportField{"body": {Text: true}}}
	emptyFile, err := lowerFileAssignments(root, "profile", keyless, []fileAssignment{{"body", "empty.txt"}})
	if err != nil {
		t.Fatal(err)
	}
	emptyRows := decodeFileImportTestRows(t, emptyFile)
	if len(emptyRows) != 1 || emptyRows[0]["body"] != "" {
		t.Fatalf("empty file must be one empty-string row, got %#v", emptyRows)
	}
	if err := os.Mkdir(root.Resolve("empty"), 0o700); err != nil {
		t.Fatal(err)
	}
	shape := fileImportShape{BusinessKey: "key", Fields: map[string]fileImportField{"key": {Text: true}, "body": {Text: true}}}
	got, err := lowerFileAssignments(root, "profile", shape, []fileAssignment{{"body", "empty"}})
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty directory = %q, %v", got, err)
	}
	writeFileImportTestFile(t, root.Resolve("nul.txt"), strings.Repeat("\x00", 125000))
	if _, err := lowerFileAssignments(root, "profile", keyless, []fileAssignment{{"body", "nul.txt"}}); err == nil || !strings.Contains(err.Error(), "decoded import limit") {
		t.Fatalf("escaped-size error = %v", err)
	}
	if err := os.Mkdir(root.Resolve("nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root.Resolve("nested"), "child"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := lowerFileAssignments(root, "profile", shape, []fileAssignment{{"body", "nested"}}); err == nil || !strings.Contains(err.Error(), "regular non-symlink") {
		t.Fatalf("nested-directory error = %v", err)
	}
}

func TestFileRowLoweringBoundsCombinedRawFields(t *testing.T) {
	root := mustInvocationRootForTest(t.TempDir())
	first := strings.Repeat("a", durabledata.MaxDecodedImportBytes/2)
	second := strings.Repeat("b", durabledata.MaxDecodedImportBytes/2+1)
	writeFileImportTestFile(t, root.Resolve("first.txt"), first)
	writeFileImportTestFile(t, root.Resolve("second.txt"), second)
	shape := fileImportShape{Fields: map[string]fileImportField{"first": {Text: true}, "second": {Text: true}}}
	_, err := lowerFileAssignments(root, "pair", shape, []fileAssignment{{Field: "first", Path: "first.txt"}, {Field: "second", Path: "second.txt"}})
	if err == nil || !strings.Contains(err.Error(), "decoded import limit") {
		t.Fatalf("combined raw byte admission = %v", err)
	}
}

func TestFileRowDottedSelectorCollision(t *testing.T) {
	parentRef, err := durabledata.ParseDeclarationRef(".", "profile")
	if err != nil {
		t.Fatal(err)
	}
	dottedRef, err := durabledata.ParseDeclarationRef(".", "profile.body")
	if err != nil {
		t.Fatal(err)
	}
	parent := durabledata.DeclarationSummary{Declaration: parentRef, LocalName: "profile"}
	dotted := durabledata.DeclarationSummary{Declaration: dottedRef, LocalName: "profile.body"}
	eligible := func(item durabledata.DeclarationSummary, field string) (bool, error) {
		return item.Declaration == parentRef && field == "body", nil
	}
	if _, err := resolveRunDataOperand("profile.body=resume.md", []durabledata.DeclarationSummary{parent, dotted}, eligible); err == nil || !strings.Contains(err.Error(), "ambiguous") || !strings.Contains(err.Error(), "JSONL event") || !strings.Contains(err.Error(), "file field") {
		t.Fatalf("collision error = %v", err)
	}
	got, err := resolveRunDataOperand("profile.body=rows.jsonl", []durabledata.DeclarationSummary{dotted}, eligible)
	if err != nil || got.Declaration.Declaration != dottedRef || got.Field != "" {
		t.Fatalf("ordinary dotted JSONL = %#v, %v", got, err)
	}
	got, err = resolveRunDataOperand("profile.body=resume.md", []durabledata.DeclarationSummary{parent}, eligible)
	if err != nil || got.Declaration.Declaration != parentRef || got.Field != "body" {
		t.Fatalf("field operand = %#v, %v", got, err)
	}
}

func TestStandaloneDataImportPositionalJSONLPathWithEquals(t *testing.T) {
	for _, path := range []string{"./rows=backup.jsonl", "../rows=backup.jsonl", "/tmp/rows=backup.jsonl", `C:\rows=backup.jsonl`, "rows/subset=backup.jsonl"} {
		if !standaloneDataJSONLPath([]string{path}) {
			t.Fatalf("positional JSONL path %q was reinterpreted as a field assignment", path)
		}
	}
	for _, operands := range [][]string{{"body=resume.md"}, {"body=resume.md", "cover=cover.md"}} {
		if standaloneDataJSONLPath(operands) {
			t.Fatalf("file assignments %v were reinterpreted as JSONL", operands)
		}
	}
}
