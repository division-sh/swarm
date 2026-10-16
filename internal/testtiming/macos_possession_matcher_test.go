package testtiming

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func assertMacOSPossessionSubtestSelection(t *testing.T, skip string) {
	t.Helper()
	dir := t.TempDir()
	for name, source := range map[string]string{
		"go.mod": "module possessionmatcher\n\ngo 1.26.8\n",
		"matcher_test.go": `package possessionmatcher
import ("fmt"; "testing")
func TestAgentRecoveryNativePreBindWriteMustJoinBothStores(t *testing.T) {
    for _, entry := range []struct { backend string; retained bool; suffix string }{
        {"sqlite",false,""}, {"postgres",false,""}, {"postgres",true,""},
        {"sqlite",false,"/route"}, {"sqlite",false,"/status"},
        {"sqlite",false,"/installed"}, {"postgres",false,"/installed"},
    } {
        for _, rollback := range []bool{false,true} {
            t.Run(fmt.Sprintf("%s/retained=%t/rollback=%t%s", entry.backend,entry.retained,rollback,entry.suffix),func(t *testing.T){})
        }
    }
    t.Run("postgresSibling",func(t *testing.T){})
}
func TestPossessionObservationExternalProcessReleaseAndCrashBothStores(t *testing.T) {
    t.Run("sqlite",func(t *testing.T){})
    t.Run("postgres",func(t *testing.T){})
}
func TestAgentRecoveryNativePreBindWriteMustJoinBothStoresSibling(t *testing.T) {
    t.Run("postgres",func(t *testing.T){})
}
func TestUnrelated(t *testing.T) { t.Run("postgres",func(t *testing.T){}) }
`,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("go", "test", "-json", "-count=1", "-skip", skip, ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Go subtest matcher control: %v\n%s", err, output)
	}
	got := map[string]bool{}
	for _, line := range bytes.Split(output, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record struct{ Action, Test string }
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record.Action == "pass" && strings.Contains(record.Test, "/") {
			got[record.Test] = true
		}
	}
	want := map[string]bool{
		"TestPossessionObservationExternalProcessReleaseAndCrashBothStores/sqlite": true,
		"TestAgentRecoveryNativePreBindWriteMustJoinBothStores/postgresSibling":    true,
		"TestAgentRecoveryNativePreBindWriteMustJoinBothStoresSibling/postgres":    true,
		"TestUnrelated/postgres": true,
	}
	for _, rollback := range []string{"false", "true"} {
		for _, suffix := range []string{"", "/route", "/status", "/installed"} {
			want["TestAgentRecoveryNativePreBindWriteMustJoinBothStores/sqlite/retained=false/rollback="+rollback+suffix] = true
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Go matcher changed retained leaves: got=%v want=%v", got, want)
	}
}
