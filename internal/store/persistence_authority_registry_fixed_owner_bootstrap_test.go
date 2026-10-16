package store_test

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

// PR2698/6207927446: the 107 exact relocations carry zero deletion credit.
// These finite inputs bind the initial shrink; they cannot classify future SQL.
var registryFixedOwnerInputs = [3][3]string{
	{"retirements", "107", "603bc985e775d1e0db3f83a177c6ac275c621df415cf08f526b27266d731bf82"},
	{"replacements", "117", "ae92dc3f1cfbc317778047e60bdd6106beee48d143e04397ffd19945590605e2"},
	{"pins", "74", "e99a072d46540fb1beeb2d5410d68e5de8ab20882a2ea6c01b91be08cdbc2151"},
}

func registryFixedOwnerInput(root string, input [3]string) ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(root, "internal/store/testdata/persistence_authority_registry_2698_"+input[0]+".tsv"))
	if err != nil {
		return nil, err
	}
	rows := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if debtBaselineChecksum(raw) != input[2] || fmt.Sprint(len(rows)) != input[1] {
		return nil, fmt.Errorf("fixed-owner bootstrap %s differs from its exact reviewed inventory", input[0])
	}
	return rows, nil
}

func registryRetainedFixedOwnerInitial(root string, h19 []byte) ([]byte, error) {
	rows, err := parseRegistryRetainedRows(h19)
	if err != nil {
		return nil, err
	}
	retirements, err := registryFixedOwnerInput(root, registryFixedOwnerInputs[0])
	if err != nil {
		return nil, err
	}
	for _, retired := range retirements {
		if !rows[retired] {
			return nil, fmt.Errorf("fixed-owner retirement is absent from the exact retained origin")
		}
		delete(rows, retired)
	}
	if err := registryFixedOwnerSourceAndRoles(root, retirements); err != nil {
		return nil, err
	}
	var kept []string
	for row := range rows {
		kept = append(kept, row)
	}
	sort.Strings(kept)
	return []byte(strings.Join(kept, "\n") + "\n"), nil
}

func registryFixedOwnerSourceAndRoles(root string, retirements []string) error {
	pins, err := registryFixedOwnerInput(root, registryFixedOwnerInputs[2])
	if err != nil {
		return err
	}
	if err := registryFixedOwnerSourcePins(root, pins); err != nil {
		return err
	}
	replacements, err := registryFixedOwnerInput(root, registryFixedOwnerInputs[1])
	if err != nil {
		return err
	}
	roles, err := os.ReadFile(filepath.Join(root, "internal/store/testdata/persistence_authority_findings.tsv"))
	if err != nil {
		return err
	}
	for _, replacement := range replacements {
		if strings.Count(string(roles), replacement+"\n") != 1 {
			return fmt.Errorf("fixed-owner bootstrap replacement role/signature is missing or duplicated")
		}
	}
	for _, retired := range retirements {
		if strings.Contains(string(roles), retired+"\n") {
			return fmt.Errorf("fixed-owner retirement still exists at its original full identity")
		}
	}
	return nil
}

func registryFixedOwnerSourcePins(root string, pins []string) error {
	byFile := map[string]map[string]string{}
	for _, pin := range pins {
		parts := strings.Split(pin, "\t")
		if byFile[parts[0]] == nil {
			byFile[parts[0]] = map[string]string{}
		}
		byFile[parts[0]][parts[1]] = parts[2]
	}
	// This image lives for one validation only. Every hostile invocation rereads
	// actual source; no path/mtime or cross-invocation success cache exists.
	for path, wanted := range byFile {
		got, err := registryFixedOwnerDeclarations(root, path, wanted)
		if err != nil {
			return err
		}
		for name, digest := range wanted {
			if got[name] == "" {
				got[name] = "absent"
			}
			if got[name] != digest {
				return fmt.Errorf("fixed-owner bootstrap source differs: %s/%s", path, name)
			}
		}
	}
	return nil
}

func registryFixedOwnerDeclarations(root, path string, wanted map[string]string) (map[string]string, error) {
	declarations := map[string]string{}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, parser.AllErrors)
	if os.IsNotExist(err) {
		return declarations, nil
	}
	if err != nil {
		return nil, err
	}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || wanted[fn.Name.Name] == "" {
			continue
		}
		if declarations[fn.Name.Name] != "" {
			return nil, fmt.Errorf("ambiguous fixed-owner declaration")
		}
		var body bytes.Buffer
		if err := format.Node(&body, token.NewFileSet(), fn); err != nil {
			return nil, err
		}
		declarations[fn.Name.Name] = debtBaselineChecksum(body.Bytes())
	}
	return declarations, nil
}

func TestPersistenceAuthorityRetainedRegistryFixedOwnerBootstrapIsExact(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	admission, err := loadRegistryRetainedAdmission(root, registryRetainedAuthorization{registryRetainedOrigin, registryRetainedDecision})
	if err != nil {
		t.Fatal(err)
	}
	var original []string
	for row := range admission.Origin {
		original = append(original, row)
	}
	sort.Strings(original)
	origin := []byte(strings.Join(original, "\n") + "\n")
	h19, err := registryRetainedH19Initial(origin)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := registryRetainedFixedOwnerInitial(root, h19)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := parseRegistryRetainedRows(exact); err != nil || len(rows) != 4704 {
		t.Fatalf("reviewed composed retirement count=%d: %v", len(rows), err)
	}
	if err := registryRetainedH19Bootstrap(root, origin, exact); err != nil {
		t.Fatal(err)
	}
	kept := bytes.SplitN(exact, []byte("\n"), 2)[0]
	retired, err := registryFixedOwnerInput(root, registryFixedOwnerInputs[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, mutant := range [][]byte{
		bytes.Replace(exact, append(slices.Clone(kept), '\n'), nil, 1),
		append(slices.Clone(exact), []byte(retired[0]+"\n")...),
		bytes.Replace(exact, kept, []byte(retired[0]), 1),
	} {
		if err := registryRetainedH19Bootstrap(root, origin, mutant); err == nil {
			t.Fatal("additional deletion, incomplete retirement or substitution gained bootstrap authority")
		}
	}
	copy := registryFixedOwnerBootstrapCopy(t, root)
	if err := registryRetainedH19Bootstrap(copy, origin, exact); err != nil {
		t.Fatal(err)
	}
	registryFixedOwnerBootstrapMutants(t, copy, origin, exact, retired)
}

func registryFixedOwnerBootstrapCopy(t *testing.T, root string) string {
	t.Helper()
	copy := t.TempDir()
	paths := map[string]bool{"internal/store/testdata/persistence_authority_findings.tsv": true}
	for _, input := range registryFixedOwnerInputs {
		paths["internal/store/testdata/persistence_authority_registry_2698_"+input[0]+".tsv"] = true
	}
	for _, pin := range registryH19SourcePins {
		paths[pin[0]] = true
	}
	pins, err := registryFixedOwnerInput(root, registryFixedOwnerInputs[2])
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		if parts := strings.Split(pin, "\t"); parts[2] != "absent" {
			paths[parts[0]] = true
		}
	}
	for path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	return copy
}

func registryFixedOwnerBootstrapMutants(t *testing.T, root string, origin, exact []byte, retired []string) {
	t.Helper()
	for _, input := range registryFixedOwnerInputs {
		path := "internal/store/testdata/persistence_authority_registry_2698_" + input[0] + ".tsv"
		original, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, root, path, string(original)+"foreign\n")
		if _, err := registryRetainedFixedOwnerInitial(root, mustRegistryH19Initial(t, origin)); err == nil {
			t.Fatal("altered retirement/replacement/source inventory gained bootstrap authority")
		}
		debtWriteModuleSource(t, root, path, string(original))
	}
	roles := "internal/store/testdata/persistence_authority_findings.tsv"
	original, err := os.ReadFile(filepath.Join(root, roles))
	if err != nil {
		t.Fatal(err)
	}
	replacements, err := registryFixedOwnerInput(root, registryFixedOwnerInputs[1])
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []string{
		strings.Replace(string(original), replacements[0]+"\n", "", 1),
		strings.Replace(string(original), replacements[0], strings.Replace(replacements[0], "private-domain-adapter", "unclassified", 1), 1),
		string(original) + retired[0] + "\n",
	} {
		debtWriteModuleSource(t, root, roles, changed)
		if err := registryRetainedH19Bootstrap(root, origin, exact); err == nil {
			t.Fatal("missing/altered replacement or surviving predecessor gained bootstrap authority")
		}
	}
	debtWriteModuleSource(t, root, roles, string(original))
	registryFixedOwnerBootstrapSourceMutants(t, root, origin, exact)
}

func mustRegistryH19Initial(t *testing.T, origin []byte) []byte {
	t.Helper()
	raw, err := registryRetainedH19Initial(origin)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func registryFixedOwnerBootstrapSourceMutants(t *testing.T, root string, origin, exact []byte) {
	t.Helper()
	pins, err := registryFixedOwnerInput(root, registryFixedOwnerInputs[2])
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		parts := strings.Split(pin, "\t")
		path, name := parts[0], parts[1]
		original, err := os.ReadFile(filepath.Join(root, path))
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if parts[2] == "absent" {
			debtWriteModuleSource(t, root, path, string(original)+"\nfunc "+name+"() {}\n")
		} else {
			debtWriteModuleSource(t, root, path, strings.Replace(string(original), "func "+name+"(", "func foreign"+name+"(", 1))
			// The one method is pinned just as strictly as ordinary declarations.
			if name == "set" {
				debtWriteModuleSource(t, root, path, strings.Replace(string(original), ") set(", ") foreignSet(", 1))
			}
		}
		if err := registryRetainedH19Bootstrap(root, origin, exact); err == nil {
			t.Fatal("changed current source gained bootstrap authority after a warm pass:", path, name)
		}
		if len(original) == 0 {
			if err := os.Remove(filepath.Join(root, path)); err != nil {
				t.Fatal(err)
			}
		} else {
			debtWriteModuleSource(t, root, path, string(original))
		}
	}
}
