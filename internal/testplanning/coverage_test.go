package testplanning

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoProofPartitionCensus(t *testing.T) {
	for _, tt := range []struct {
		name string
		runs []string
		want string
	}{
		{"unfiltered", []string{""}, ""},
		{"disjoint", []string{"^TestA$", "^(TestB|Example.*|Fuzz.*)$"}, ""},
		{"missing owners", nil, "no execution units"},
		{"omission", []string{"^TestA$"}, "matches 0"},
		{"overlap", []string{".*", "^TestA$"}, "matches 2"},
		{"unfiltered overlap", []string{"", "^TestA$"}, "cannot coexist"},
		{"dead unit", []string{".*", "^TestAbsent$"}, "matches no proof"},
		{"partial subtests", []string{"^TestA$/sqlite", ".*"}, "partial-subtest"},
		{"invalid pattern", []string{"["}, "error parsing regexp"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeProofSource(t, dir, "proof_test.go", `package proof
func TestMain(m any) {}
func TestA(t any) {}
func TestB(t any) {}
func ExampleProof() {}
func FuzzProof(f any) {}
func helper() {}
type receiver struct{}
func (receiver) TestMethod() {}
`)
			err := ValidateGoProofPartition(dir, tt.runs)
			if tt.want == "" && err != nil || tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestGoProofPartitionNewDeclarationMustBeSelected(t *testing.T) {
	dir := t.TempDir()
	writeProofSource(t, dir, "proof_test.go", "package proof\nfunc TestA(t any) {}\n")
	if err := ValidateGoProofPartition(dir, []string{"^TestA$"}); err != nil {
		t.Fatal(err)
	}
	writeProofSource(t, dir, "new_test.go", "package proof\nfunc TestNew(t any) {}\n")
	if err := ValidateGoProofPartition(dir, []string{"^TestA$"}); err == nil || !strings.Contains(err.Error(), "TestNew matches 0") {
		t.Fatalf("new proof silently omitted: %v", err)
	}
	if err := ValidateGoProofPartition(dir, []string{"^TestA$", "^TestNew$"}); err != nil {
		t.Fatal(err)
	}
}

func TestGoProofPartitionEmptyAndMalformedCensus(t *testing.T) {
	dir := t.TempDir()
	if err := ValidateGoProofPartition(dir, []string{""}); err == nil || !strings.Contains(err.Error(), "empty census") {
		t.Fatalf("empty census: %v", err)
	}
	writeProofSource(t, dir, "bad_test.go", "invalid Go")
	if err := ValidateGoProofPartition(dir, []string{""}); err == nil {
		t.Fatal("malformed census admitted")
	}
}

func writeProofSource(t *testing.T, dir, name, source string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}
