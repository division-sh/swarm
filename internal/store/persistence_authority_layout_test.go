package store_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// This is a layout proof, not an updater: every reviewed disposition is retained.
func TestPersistenceAuthorityFileFirstConflictReplay(t *testing.T) {
	registry := readAuthorityRegistry(t, filepath.Join(persistenceAuthorityRepoRoot(t), "internal/store/testdata/persistence_authority_findings.tsv"))
	files := map[string]int{}
	for key := range registry {
		files[strings.Split(key, "\t")[2]]++
	}
	var eligible []string
	for file, count := range files {
		if count >= 20 {
			eligible = append(eligible, file)
		}
	}
	sort.Strings(eligible)
	if len(eligible) < 12 {
		t.Fatal("representative registry no longer has six independent source pairs")
	}
	oldConflicts, fileConflicts := 0, 0
	for pair := 0; pair < 6; pair++ {
		a, b := eligible[pair*2], eligible[pair*2+1]
		counts := map[bool]int{}
		for _, fileFirst := range []bool{false, true} {
			base := registryLayout(registry, fileFirst, "", "")
			left := registryLayout(registry, fileFirst, a, "left")
			right := registryLayout(registry, fileFirst, b, "right")
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
		oldConflicts += counts[false]
		fileConflicts += counts[true]
		t.Logf("pair %d: %s / %s; kind-first conflicts=%d file-first conflicts=%d", pair+1, a, b, counts[false], counts[true])
	}
	t.Logf("%d reviewed rows; %d sources; six disjoint whole-source edit pairs: kind-first=%d file-first=%d", len(registry), len(files), oldConflicts, fileConflicts)
	if oldConflicts == 0 || fileConflicts >= oldConflicts {
		t.Fatal("single-TSV file-first proposal did not demonstrate conflict reduction")
	}
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
