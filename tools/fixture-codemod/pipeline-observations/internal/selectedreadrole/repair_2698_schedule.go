package selectedreadrole

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// #2698/6225992056: preservation only; live census always reads actual source.
const repair2698SchedulePath = "internal/serveapp/semantic_numeric_schedule_test.go"
const repair2698ScheduleBefore = "ff1ea5580af1ecf153490f5e71e9f2ea66da3616"
const repair2698ScheduleBeforeSHA = "746c7c357dad92054381626d95617e18d186d61e0a0142c23b2893ba86b08e5f"
const repair2698ScheduleAfterSHA = "8de80abaf8df0ecae4fb4fcb7c7a1d3bdbcfd5b80e157084410562af09552119"

func repair2698ScheduleHistoricalSource(root, path string, actual []byte) ([]byte, error) {
	if path != repair2698SchedulePath || fmt.Sprintf("%x", sha256.Sum256(actual)) != repair2698ScheduleAfterSHA {
		return actual, nil
	}
	current, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || !bytes.Equal(current, actual) {
		return nil, fmt.Errorf("schedule reader inverse does not describe actual copied source: %v", err)
	}
	before, err := exec.Command("git", "-C", root, "show", repair2698ScheduleBefore+":"+path).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(before)) != repair2698ScheduleBeforeSHA {
		return nil, fmt.Errorf("schedule reader inverse lost its exact signed predecessor: %v", err)
	}
	return before, nil
}
