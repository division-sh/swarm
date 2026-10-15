package store_test

import (
	"bytes"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

func registryRetainedControl() (registryRetainedAdmission, map[string]authorityFinding, map[string]string, []string) {
	finding := authorityFinding{Kind: "raw-operation", File: "fixture.go", Enclosing: "original", Member: "call:original#1", Resolved: "exact signature", RawSQL: true}
	row := "unclassified\t" + finding.key()
	origin := map[string]bool{row: true}
	return registryRetainedAdmission{Active: maps.Clone(origin), Origin: origin, Previous: maps.Clone(origin)},
		map[string]authorityFinding{finding.key(): finding}, map[string]string{finding.key(): "unclassified"}, []string{row, row}
}

func TestPersistenceAuthorityRetainedRegistryPreservesStrictDiagnostics(t *testing.T) {
	admission, actual, registered, strict := registryRetainedControl()
	before := slices.Clone(strict)
	if err := validateRegistryRetainedAdmission(admission, actual, registered, nil, nil, strict); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, strict) {
		t.Fatal("admission mutated the original strict diagnostic stream")
	}
	for key, finding := range actual {
		if _, err := indexAuthorityRegistryFindings([]authorityFinding{finding, finding}); err == nil {
			t.Fatal("duplicate actual findings gained completion admission")
		}
		finding.RawSQL = false
		row := "unclassified\t" + finding.key()
		typed := map[string]bool{row: true}
		if err := validateRegistryRetainedAdmission(registryRetainedAdmission{Active: typed, Origin: typed, Previous: typed},
			map[string]authorityFinding{finding.key(): finding}, map[string]string{finding.key(): "unclassified"}, nil, nil, []string{row}); err != nil {
			t.Fatalf("typed completion reason: %v", err)
		}
		delete(admission.Active, "unclassified\t"+key)
		delete(actual, key)
		delete(registered, key)
	}
	if err := validateRegistryRetainedAdmission(admission, actual, registered, nil, nil, nil); err != nil {
		t.Fatalf("lawful source removal toward zero refused: %v", err)
	}
}

func TestPersistenceAuthorityRetainedRegistryAllowsLawfulClassificationShrink(t *testing.T) {
	admission, actual, registered, _ := registryRetainedControl()
	clear(admission.Active)
	for key := range registered {
		registered[key] = "private-domain-adapter"
	}
	if err := validateRegistryRetainedAdmission(admission, actual, registered, nil, nil, nil); err != nil {
		t.Fatalf("existing lawful role could not retire its retained completion row: %v", err)
	}
}

func TestPersistenceAuthorityRetainedRegistryRejectsForeignReasonsAndResurrection(t *testing.T) {
	for _, name := range []string{"unknown", "stale", "foreign-role", "missing-reason", "extra-reason", "changed-reason", "artifact-only-removal", "resurrection", "changed-signature", "changed-role", "new-after-offsetting-removal", "foreign-previous"} {
		t.Run(name, func(t *testing.T) {
			admission, actual, registered, invalid := registryRetainedControl()
			before := slices.Clone(invalid)
			var unknown, stale []string
			key := strings.TrimPrefix(invalid[0], "unclassified\t")
			switch name {
			case "unknown":
				unknown = []string{invalid[0]}
			case "stale":
				stale = []string{invalid[0]}
			case "foreign-role":
				registered[key] = "forbidden"
			case "missing-reason":
				invalid = invalid[:1]
			case "extra-reason":
				invalid = append(invalid, invalid[0])
			case "changed-reason":
				invalid[0] = "forbidden\t" + key
			case "artifact-only-removal":
				clear(admission.Active)
			case "resurrection":
				clear(admission.Previous)
			case "foreign-previous":
				admission.Previous["unclassified\tforeign"] = true
			default:
				registryRetainedChangeIdentity(name, admission, actual, registered, &invalid, key)
			}
			unchanged := slices.Clone(invalid)
			if err := validateRegistryRetainedAdmission(admission, actual, registered, unknown, stale, invalid); err == nil {
				t.Fatal("foreign identity/reason/multiplicity or resurrection gained admission")
			}
			if !reflect.DeepEqual(invalid, unchanged) || len(before) != 2 {
				t.Fatal("admission concealed a strict diagnostic")
			}
		})
	}
}

func registryRetainedChangeIdentity(name string, admission registryRetainedAdmission, actual map[string]authorityFinding, registered map[string]string, invalid *[]string, key string) {
	finding := actual[key]
	delete(actual, key)
	delete(registered, key)
	delete(admission.Active, "unclassified\t"+key)
	switch name {
	case "changed-signature":
		finding.Resolved += " changed"
	case "changed-role":
		finding.RawSQL = false
	default:
		finding.Enclosing = "new operation"
	}
	row := "unclassified\t" + finding.key()
	actual[finding.key()], registered[finding.key()], admission.Active[row] = finding, "unclassified", true
	*invalid = []string{row}
	if finding.RawSQL {
		*invalid = append(*invalid, row)
	}
}

func TestPersistenceAuthorityRetainedRegistryPinsOriginAndCanonicalBytes(t *testing.T) {
	admission, err := loadRegistryRetainedAdmission(persistenceAuthorityRepoRoot(t), registryRetainedAuthorization{registryRetainedOrigin, registryRetainedDecision})
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for row := range admission.Origin {
		lines = append(lines, row)
	}
	sort.Strings(lines)
	origin := []byte(strings.Join(lines, "\n") + "\n")
	if _, err := validateRegistryRetainedOrigin(registryRetainedAuthorization{registryRetainedOrigin, registryRetainedDecision}, origin); err != nil {
		t.Fatal(err)
	}
	for _, authority := range []registryRetainedAuthorization{{"foreign", registryRetainedDecision}, {registryRetainedOrigin, "foreign"}} {
		if _, err := validateRegistryRetainedOrigin(authority, origin); err == nil {
			t.Fatal("wrong origin or decision accepted")
		}
	}
	if _, err := validateRegistryRetainedOrigin(registryRetainedAuthorization{registryRetainedOrigin, registryRetainedDecision}, append(slices.Clone(origin), '\n')); err == nil {
		t.Fatal("tampered immutable origin accepted")
	}
	row := []byte(lines[0] + "\n")
	for _, corrupt := range [][]byte{bytes.TrimSuffix(row, []byte("\n")), append(slices.Clone(row), row...), []byte("# header\n"), bytes.ReplaceAll(row, []byte("\t"), []byte(" ")), bytes.ReplaceAll(row, []byte("unclassified"), []byte("private-domain-adapter")), append([]byte(lines[1]+"\n"), row...), bytes.ReplaceAll(row, []byte("\n"), []byte("\r\n"))} {
		if _, err := parseRegistryRetainedRows(corrupt); err == nil {
			t.Fatal("duplicate, normalized, relabelled or malformed artifact accepted")
		}
	}
}

func TestPersistenceAuthorityRetainedRegistryPreviousCannotBeReseeded(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.invalid"}, {"config", "commit.gpgsign", "false"}, {"commit", "--allow-empty", "-qm", "test: previous source"}} {
		if _, err := debtGit(root, args...); err != nil {
			t.Fatal(err)
		}
	}
	origin := []byte("exact original rows\n")
	if _, err := registryRetainedPrevious(root, "HEAD", origin); err == nil {
		t.Fatal("missing active artifact accepted")
	}
	path := filepath.Join(root, registryRetainedPath)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := registryRetainedPrevious(root, "HEAD", origin); err == nil {
		t.Fatal("first artifact differed from immutable origin")
	}
	if err := os.WriteFile(path, origin, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := registryRetainedPrevious(root, "HEAD", origin); err != nil || !bytes.Equal(got, origin) {
		t.Fatalf("exact initial origin: %q %v", got, err)
	}
	for _, args := range [][]string{{"add", "--", registryRetainedPath}, {"commit", "-qm", "test: accepted retained set"}, {"rm", "--", registryRetainedPath}, {"commit", "-qm", "test: illegal removed evidence"}} {
		if _, err := debtGit(root, args...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := registryRetainedPrevious(root, "HEAD", origin); err == nil {
		t.Fatal("removed landed evidence was allowed to reseed")
	}
}
