package store_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPersistenceAuthorityRegistryReaderRefusesInvalidArtifacts(t *testing.T) {
	const key = "SWARM_REGISTRY_READER_NEGATIVE"
	if path := os.Getenv(key); path != "" {
		readAuthorityRegistry(t, path)
		return
	}
	for _, row := range []struct{ name, data, diagnostic string }{
		{"malformed", "without-delimiter\n", "invalid persistence authority registry line"},
		{"duplicate", "private-backend\tkey\nprivate-backend\tkey\n", "duplicate persistence authority registry finding"},
		{"missing", "", "open persistence authority registry"},
	} {
		t.Run(row.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "registry.tsv")
			if row.name != "missing" {
				if err := os.WriteFile(path, []byte(row.data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPersistenceAuthorityRegistryReaderRefusesInvalidArtifacts$")
			command.Env = append(os.Environ(), key+"="+path)
			raw, err := command.CombinedOutput()
			if err == nil || !strings.Contains(string(raw), row.diagnostic) {
				t.Fatalf("%s accepted or wrong refusal: %v %s", row.name, err, raw)
			}
		})
	}
	t.Run("updater_preserves_judgment_and_refuses_to_approve_new_identity", func(t *testing.T) {
		known := authorityFinding{Kind: "local-raw-type", File: "source/a.go", Enclosing: "F", Member: "db", Resolved: "*database/sql.DB", RawSQL: true}
		fresh := known
		fresh.File = "source/b.go"
		path := filepath.Join(t.TempDir(), "registry.tsv")
		writeAuthorityRegistry(t, path, []authorityFinding{fresh, known}, map[string]string{known.key(): "private-backend"})
		got := readAuthorityRegistry(t, path)
		if got[known.key()] != "private-backend" || got[fresh.key()] != "unclassified" || rawSQLDispositionAllowed(got[fresh.key()]) {
			t.Fatal("updater granted authority", got)
		}
	})
}
