package counterprojection

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var retiredCounterSQL = regexp.MustCompile(`(?is)\bevent_count\s*=\s*\(\s*SELECT\s+COUNT\s*\(|\bNULLIF\s*\(\s*(?:r\.)?event_count\s*,\s*0\s*\)|\bentity_count\s+(?:INTEGER|INT|BIGINT)\b`)

func TestRunCounterRetirementGuardRejectsRecountAndUnknownZero(t *testing.T) {
	for _, source := range []string{
		`UPDATE runs SET event_count = (SELECT COUNT(*) FROM events)`,
		`COALESCE(NULLIF(r.event_count, 0), alternate, 0)`,
		`entity_count INTEGER NOT NULL DEFAULT 0`,
	} {
		if !retiredCounterSQL.MatchString(source) {
			t.Fatalf("retired counter interpreter escaped: %s", source)
		}
	}
	if retiredCounterSQL.MatchString(`UPDATE runs SET event_count = event_count + $1`) {
		t.Fatal("guard rejected the canonical atomic delta")
	}
}

func TestRunCounterRetiredInterpretersAreAbsent(t *testing.T) {
	for _, root := range []string{"../../..", "../../../../testutil"} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if retiredCounterSQL.Match(raw) {
				t.Errorf("%s retains a retired counter interpreter", path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
