package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func nativeAttemptDownloads(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Uses string
				With map[string]string
			}
		}
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	names := map[string]string{}
	for _, step := range workflow.Jobs["unused-checks"].Steps {
		if !strings.Contains(step.Uses, "download-artifact") {
			continue
		}
		for _, platform := range []string{"linux", "darwin"} {
			if step.With["path"] == "test-results/unused/"+platform {
				names[platform] = strings.NewReplacer("${{ github.run_id }}", "123", "${{ github.run_attempt }}", "2").Replace(step.With["name"])
			}
		}
	}
	if len(names) != 2 {
		t.Fatal("missing real native partner acquisition")
	}
	return names
}

func TestAttemptQualifiedNativeDownloadsUseRealUnionAndRefuseOldPartners(t *testing.T) {
	names := nativeAttemptDownloads(t)
	root := fixtureRepo(t, nil)
	// Transport/merge needs a real matrix binary, not an unrelated Go test-main
	// import graph. Keep all production/tagged/race sources and the full matrix.
	if err := os.Remove(filepath.Join(root, "fixture_test.go")); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "-u"}, {"-c", "user.name=Guard Fixture", "-c", "user.email=fixture@example.com", "commit", "--quiet", "--no-gpg-sign", "-m", "fixture: native transport library"}} {
		if _, err := gitOutput(root, args...); err != nil {
			t.Fatal(err)
		}
	}
	var stdout, stderr bytes.Buffer
	g := guard{root: root, stdout: &stdout, stderr: &stderr}
	binary := filepath.Join(t.TempDir(), resultFile)
	analysisStart := time.Now()
	n, err := g.analyze(binary)
	t.Logf("real native matrix analysis: %s", time.Since(analysisStart))
	if err != nil {
		t.Fatalf("real upstream binary: %v %s", err, &stderr)
	}
	head, err := cleanHead(root)
	if err != nil {
		t.Fatal(err)
	}
	hash, size, err := hashResult(binary)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(previous); err != nil {
			t.Error(err)
		}
	})
	for _, cut := range []string{"current_pair", "no_current_pair", "missing_linux", "missing_darwin", "failed_linux", "failed_darwin", "skipped_linux", "skipped_darwin", "cancelled_linux", "cancelled_darwin", "partial_linux", "partial_darwin"} {
		t.Run(cut, func(t *testing.T) {
			catalogue := map[string]map[string][]byte{}
			for _, platform := range []string{"linux", "darwin"} {
				// This synthetic platform envelope tests transport/real merge only;
				// it does NOT claim native Darwin collection on the Linux test host.
				native := n
				native.GOOS, native.GOHOSTOS = platform, platform
				e := evidence{Schema: 1, State: "complete", SourceSHA: head, Config: pinnedConfig(), Native: native, ResultSHA256: hash, ResultBytes: size}
				old, err := json.Marshal(e)
				if err != nil {
					t.Fatal(err)
				}
				catalogue[fmt.Sprintf("unused-%s-123-1", platform)] = map[string][]byte{metaFile: old, resultFile: data}
				catalogue["unused-"+platform] = map[string][]byte{metaFile: old, resultFile: data}
				if cut == "no_current_pair" || cut == "missing_"+platform {
					continue
				}
				for _, state := range []string{"failed", "skipped", "cancelled"} {
					if cut == state+"_"+platform {
						e.State = state
					}
				}
				current, err := json.Marshal(e)
				if err != nil {
					t.Fatal(err)
				}
				files := map[string][]byte{metaFile: current, resultFile: data}
				if cut == "partial_"+platform {
					delete(files, metaFile)
				}
				catalogue[fmt.Sprintf("unused-%s-123-2", platform)] = files
			}
			target := t.TempDir()
			for _, platform := range []string{"linux", "darwin"} {
				for name, bytes := range catalogue[names[platform]] {
					writeFixtureFile(t, target, filepath.Join(platform, name), string(bytes))
				}
			}
			stdout.Reset()
			err := run([]string{"-merge", "-in", target}, &stdout, &stderr)
			if cut == "current_pair" {
				if err != nil || !strings.Contains(stdout.String(), "zero U1000 for "+head) {
					t.Fatalf("U05 real union refused current pair: %v %s %s", err, &stdout, &stderr)
				}
			} else if err == nil || strings.Contains(stdout.String(), "zero U1000") {
				t.Fatalf("U03/U04/U06/U07 %s borrowed old native success", cut)
			}
		})
	}
}
