package store_test

import (
	"bytes"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The fixed control isolates the old kind-first interleaving from live inventory churn.
func TestPersistenceAuthorityFileFirstConflictReplay(t *testing.T) {
	files := []string{"internal/runtime/layout_a_test.go", "internal/runtime/layout_separator_test.go", "internal/runtime/layout_z_test.go"}
	registry := map[string]string{}
	for _, kind := range []string{"callback-type", "context-value", "context-withvalue", "effective-method", "interface-method", "local-raw-type", "raw-operation", "raw-type"} {
		for _, file := range files {
			registry[strings.Join([]string{kind, "raw-sql", file, "fixtureOwner", "value#1", "*database/sql.DB"}, "\t")] = "test-fixture-only"
		}
	}
	counts := map[bool]int{}
	for _, fileFirst := range []bool{false, true} {
		base := registryLayout(registry, fileFirst, "", "")
		left := registryLayout(registry, fileFirst, files[0], "left")
		right := registryLayout(registry, fileFirst, files[2], "right")
		root := t.TempDir()
		for name, data := range map[string][]byte{"base": base, "left": left, "right": right} {
			if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		output, err := exec.Command("git", "merge-file", "-p", filepath.Join(root, "left"), filepath.Join(root, "base"), filepath.Join(root, "right")).CombinedOutput()
		if failure, ok := err.(*exec.ExitError); err != nil && (!ok || failure.ExitCode() > 127) {
			t.Fatalf("merge-file: %v %s", err, output)
		}
		counts[fileFirst] = bytes.Count(output, []byte("<<<<<<<"))
	}
	t.Logf("fixed %d-row layout control: kind-first=%d file-first=%d", len(registry), counts[false], counts[true])
	if counts[false] == 0 || counts[true] >= counts[false] {
		t.Fatal("single-TSV file-first proposal did not demonstrate conflict reduction")
	}
}

func TestPersistenceAuthorityFileFirstRetainsLiveInventory(t *testing.T) {
	path := filepath.Join(persistenceAuthorityRepoRoot(t), "internal/store/testdata/persistence_authority_findings.tsv")
	registry := readAuthorityRegistry(t, path)
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte("# disposition\tkind\tauthority\tfile\tenclosing\tmember\tresolved-type\n"), registryLayout(registry, true, "", "")...)
	if !bytes.Equal(committed, want) {
		t.Fatal("live registry is not in exact file-first order")
	}
	for _, fileFirst := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "layout.tsv")
		if err := os.WriteFile(path, registryLayout(registry, fileFirst, "", ""), 0600); err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(readAuthorityRegistry(t, path), registry) {
			t.Fatalf("file-first=%t changed live identities or reviewed dispositions", fileFirst)
		}
	}
	t.Logf("both layouts preserve all %d live reviewed rows; committed ordering is exact", len(registry))
}

func registryLayout(registry map[string]string, fileFirst bool, changed, marker string) []byte {
	var keys []string
	for key := range registry {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if fileFirst {
			a, b := strings.Split(keys[i], "\t")[2], strings.Split(keys[j], "\t")[2]
			if a != b {
				return a < b
			}
		}
		return keys[i] < keys[j]
	})
	var output bytes.Buffer
	for _, key := range keys {
		fmt.Fprint(&output, registry[key], "\t", key)
		if strings.Split(key, "\t")[2] == changed {
			fmt.Fprint(&output, "-", marker)
		}
		output.WriteByte('\n')
	}
	return output.Bytes()
}
