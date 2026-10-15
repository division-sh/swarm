//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func precompileRealGoFixture(t *testing.T) (string, string) {
	t.Helper()
	requirePrecompileSupport(t)
	root, state := t.TempDir(), filepath.Join(t.TempDir(), "state")
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("SWARM_TEST_POSTGRES_DSN", signalFixtureStateDSN)
	t.Setenv("SWARM_TEST_RUN_SLOTS", "1")
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOENV", "off")
	t.Setenv("GOWORK", "off")
	t.Setenv("GOTOOLCHAIN", "local")
	t.Setenv("PATH", filepath.Join(runtime.GOROOT(), "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	marker := filepath.Join(root, "main-ran")
	t.Setenv("PRECOMPILE_REAL_MARKER", marker)
	for name, source := range map[string]string{
		"go.mod":   "module example.com/precompile\n\ngo 1.26.0\n",
		"value.go": "package fixture\nfunc Value() int { return 42 }\n",
		"value_test.go": `package fixture
import ("encoding/json"; "fmt"; "os"; "path/filepath"; "testing")
func TestMain(m *testing.M) {
  raw, err := os.ReadFile(filepath.Join(os.Getenv("XDG_STATE_HOME"), "swarm", "test-postgres", "runs-v1.json"))
  var state struct { Active []json.RawMessage }
  if err != nil || json.Unmarshal(raw, &state) != nil || len(state.Active) != 1 {
    fmt.Fprintln(os.Stderr, "TestMain ran without admission"); os.Exit(72)
  }
  f, err := os.OpenFile(os.Getenv("PRECOMPILE_REAL_MARKER"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
  if err != nil { panic(err) }
  if _, err := f.WriteString("main\n"); err != nil { panic(err) }
  if err := f.Close(); err != nil { panic(err) }
  os.Exit(m.Run())
}
func TestSelected(t *testing.T) { if Value() != 42 { t.Fatal("wrong value") }; t.Log("selected output retained") }
func TestOmitted(t *testing.T) { t.Fatal("selector lost") }
`,
		"tag_test.go": "//go:build compileprobe\n\npackage fixture\nimport \"testing\"\nfunc TestTagged(t *testing.T) { t.Log(\"tagged output retained\") }\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)
	return marker, filepath.Join(state, "swarm", "test-postgres")
}

func TestPrecompileRealGoPreservesExecutionOutputAndResultCache(t *testing.T) {
	var control [][]string
	load, _ := os.ReadFile("/proc/loadavg")
	t.Logf("host=%s/%s concurrent host load=%s; cold means new workload objects, shared dependency cache retained", runtime.GOOS, runtime.GOARCH, strings.TrimSpace(string(load)))
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprint("warmup=", warm), func(t *testing.T) {
			marker, state := precompileRealGoFixture(t)
			for pass, wantMains := range []int{1, 1, 2, 3} {
				args := []string{"-json", "-x", "-run=^TestSelected$", "."}
				if pass >= 2 {
					args = append(args, "-count=1")
				}
				started := time.Now()
				var stdout, diagnostics string
				if warm {
					code, out, errout := capturePrecompileRun(t, args)
					if code != 0 {
						t.Fatalf("real-Go warm/actual command failed: code=%d\n%s\n%s", code, out, errout)
					}
					stdout, diagnostics = out, errout
				} else {
					stdout, _ = runPrecompileRealGoControl(t, args)
				}
				endToEnd := time.Since(started)
				normalized, compiled, linked, cached := precompileObservedOutput(t, stdout)
				if warm {
					if compiled != 0 || !reflect.DeepEqual(normalized, control[pass]) {
						t.Fatalf("warm build changed actual output or did not warm objects: compile=%d output=%q control=%q", compiled, normalized, control[pass])
					}
				} else {
					control = append(control, normalized)
				}
				if cached != (pass == 1) {
					t.Fatalf("actual result-cache/count semantics changed: pass=%d cached=%t", pass, cached)
				}
				raw, err := os.ReadFile(marker)
				if err != nil || string(raw) != strings.Repeat("main\n", wantMains) {
					t.Fatalf("warm-up ran TestMain or actual execution was skipped: mains=%q want=%d err=%v", raw, wantMains, err)
				}
				var registry struct {
					Active, Waiting []json.RawMessage
					History         []struct {
						DurationSeconds float64 `json:"duration_seconds"`
					}
				}
				raw, err = os.ReadFile(filepath.Join(state, "runs-v1.json"))
				if err != nil || json.Unmarshal(raw, &registry) != nil || len(registry.Active)+len(registry.Waiting) != 0 || len(registry.History) == 0 {
					t.Fatalf("real-Go command did not settle its own slot: %s %v", raw, err)
				}
				t.Logf("pass=%d warmup=%t end_to_end=%s history_hold=%+v actual_compile_commands=%d actual_links=%d cached=%t\n%s", pass, warm, endToEnd, registry.History, compiled, linked, cached, diagnostics)
			}
		})
	}
}

func TestPrecompileRealGoRaceTagsAndFileList(t *testing.T) {
	for _, mode := range []string{"race_tags", "file_list"} {
		t.Run(mode, func(t *testing.T) {
			marker, _ := precompileRealGoFixture(t)
			args := []string{"-json", "-count=1", "-run=^TestSelected$", "value.go", "value_test.go"}
			if mode == "race_tags" {
				t.Setenv("CGO_ENABLED", "1")
				args = []string{"-json", "-race", "-tags=compileprobe", "-count=3", "-run=^TestTagged$", "."}
			}
			code, stdout, diagnostics := capturePrecompileRun(t, args)
			if code != 0 || strings.Contains(stdout, "TestOmitted") {
				t.Fatalf("real-Go arguments changed: code=%d\n%s\n%s", code, stdout, diagnostics)
			}
			want, root := 1, "TestSelected"
			if mode == "race_tags" {
				want, root = 3, "TestTagged"
			}
			passes := 0
			decoder := json.NewDecoder(strings.NewReader(stdout))
			for decoder.More() {
				var event struct{ Action, Test string }
				if err := decoder.Decode(&event); err != nil {
					t.Fatal(err)
				}
				if event.Action == "pass" && event.Test == root {
					passes++
				}
			}
			if raw, err := os.ReadFile(marker); err != nil || string(raw) != "main\n" || passes != want {
				t.Fatalf("count/tag/file selection changed: mains=%q passes=%d want=%d err=%v", raw, passes, want, err)
			}
		})
	}
}

func TestPrecompileRealGoBuildFailureHasNoAuthority(t *testing.T) {
	for _, mode := range []string{"compile_error", "race_without_cgo"} {
		t.Run(mode, func(t *testing.T) {
			marker, state := precompileRealGoFixture(t)
			args, want := []string{"-json", "-count=1", "."}, "undefined: missingSymbol"
			if mode == "compile_error" {
				if err := os.WriteFile("value.go", []byte("package fixture\nfunc Value() int { return missingSymbol }\n"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				t.Setenv("CGO_ENABLED", "0")
				args, want = append(args, "-race"), "-race requires cgo"
			}
			code, stdout, diagnostics := capturePrecompileRun(t, args)
			if code != 1 || stdout != "" || !strings.Contains(diagnostics, want) {
				t.Fatalf("failed real compile earned proof/lost diagnostic: code=%d stdout=%q diagnostics=%s", code, stdout, diagnostics)
			}
			for _, path := range []string{marker, state} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("compile failure executed TestMain or published authority: %s: %v", path, err)
				}
			}
		})
	}
}

func precompileObservedOutput(t *testing.T, stdout string) (normalized []string, compiled, linked int, cached bool) {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(stdout))
	for {
		var event struct{ Action, Test, Output string }
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		if event.Action == "build-output" {
			if strings.Contains(event.Output, "/compile ") {
				compiled++
			}
			if strings.Contains(event.Output, "/link ") {
				linked++
			}
		}
		cached = cached || strings.Contains(event.Output, "(cached)")
		if event.Test != "" {
			if event.Test != "TestSelected" {
				t.Fatalf("omitted root executed: %+v", event)
			}
			output := event.Output
			if strings.HasPrefix(output, "--- PASS:") {
				output = strings.Split(output, " (")[0]
			}
			normalized = append(normalized, event.Action+" "+event.Test+" "+output)
		}
	}
	if len(normalized) == 0 {
		t.Fatal("actual command emitted no selected-root evidence")
	}
	return
}
