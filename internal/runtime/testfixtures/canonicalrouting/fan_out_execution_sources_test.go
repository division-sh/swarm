package canonicalrouting

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestFanOutExecutionSourcesLoadExactOverlays(t *testing.T) {
	Prove(t, ArtifactID("internal/runtime/testfixtures/canonicalrouting/testdata/fan-out-execution"))
	for _, tc := range []struct {
		name string
		copy func(testing.TB) string
	}{
		{"served-reporter", CopyServedFanOutReporter},
		{"mixed-agent", CopyFanOutMixedAgentRoute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := tc.copy(t)
			overlay := filepath.Join(RepoRoot(t), "internal/runtime/testfixtures/canonicalrouting/testdata/fan-out-execution", tc.name)
			files := 0
			err := filepath.WalkDir(overlay, func(path string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				relative, err := filepath.Rel(overlay, path)
				if err != nil {
					return err
				}
				want, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				got, err := os.ReadFile(filepath.Join(root, relative))
				if err != nil {
					return err
				}
				if !bytes.Equal(got, want) {
					t.Errorf("overlay bytes changed: %s", relative)
				}
				files++
				return nil
			})
			if err != nil || files != 7 {
				t.Fatalf("overlay inventory: files=%d err=%v", files, err)
			}
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			repo := RepoRoot(t)
			if _, err := contracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
