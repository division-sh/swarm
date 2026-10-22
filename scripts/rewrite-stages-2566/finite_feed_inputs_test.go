package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const finiteFixtureFeedPredecessor = "9beb615299e14741752ea6c92128b8e6036e9ee6"

// #2702/6210051885 permits only these two input transformations; the historical
// ledger still owns the original stage rewrite and is never rewritten here.
func finiteFixtureRootInputTransform(file string) (before, after, predecessorHash string, listed bool) {
	switch file {
	case "internal/runtime/testfixtures/canonicalrouting/deployment_resource_feeds.go":
		return `inputs: [fixture.close.requested]\n`, `inputs: [root.ready, fixture.close.requested]\n`, "820a86b78782db7cb4a905c9fa218f52f13422bef7aa4895d24ec603ec9b5ab8", true
	case "tests/tier11-flow-composition/test-numeric-data-scatter-park/schema.yaml":
		return "pins:\n  outputs:\n    - item.registered\n", "pins:\n  inputs:\n    - item.registered\n", "c813aef240ac75db7ba249faf2675520e037a636c96de33e866f63881dc05963", true
	default:
		return "", "", "", false
	}
}

func finiteFixtureGitSource(t *testing.T, repo, revision, file string) []byte {
	t.Helper()
	command := exec.Command("git", "show", revision+":"+file)
	command.Dir = repo
	body, err := command.Output()
	if err != nil {
		t.Fatalf("read immutable fixture %s:%s: %v", revision, file, err)
	}
	return body
}

func finiteFixtureRecordedChange(t *testing.T, repo, file string) change {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repo, "scripts/rewrite-stages-2566/finite-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var p plan
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	for _, c := range p.Changes {
		if c.File == file {
			return c
		}
	}
	t.Fatalf("missing historical fixture row %s", file)
	return change{}
}

func applyFiniteFixtureRootInput(file string, predecessor []byte) ([]byte, error) {
	before, after, _, listed := finiteFixtureRootInputTransform(file)
	if !listed {
		return nil, fmt.Errorf("%s: no reviewed root-input transformation", file)
	}
	if count := bytes.Count(predecessor, []byte(before)); count != 1 {
		return nil, fmt.Errorf("%s: root-input target count=%d, want exactly one", file, count)
	}
	return bytes.Replace(predecessor, []byte(before), []byte(after), 1), nil
}

func checkFiniteFixtureCurrentOutput(root string, c change, historical, predecessor []byte) error {
	_, _, predecessorHash, listed := finiteFixtureRootInputTransform(c.File)
	if !listed {
		_, err := pendingChange(root, c, true)
		return err
	}
	if (len(c.Equivalence) == 0 && c.Review == "") || len(c.Edits) == 0 || c.BeforeHash == c.AfterHash {
		return fmt.Errorf("%s: missing edit/equivalence decision", c.File)
	}
	if digest(historical) != c.AfterHash || c.AfterHash != predecessorHash || digest(predecessor) != predecessorHash || !bytes.Equal(historical, predecessor) {
		return fmt.Errorf("%s: historical After and pinned master predecessor disagree", c.File)
	}
	expected, err := applyFiniteFixtureRootInput(c.File, predecessor)
	if err != nil {
		return err
	}
	current, err := os.ReadFile(filepath.Join(root, c.File))
	if err != nil {
		return err
	}
	if !bytes.Equal(current, expected) {
		return fmt.Errorf("%s: current source differs from the exact reviewed root-input composition", c.File)
	}
	return nil
}

func TestRewrite2566FiniteFeedInputCompositionRejectsDrift(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	for _, file := range []string{
		"internal/runtime/testfixtures/canonicalrouting/deployment_resource_feeds.go",
		"tests/tier11-flow-composition/test-numeric-data-scatter-park/schema.yaml",
	} {
		t.Run(file, func(t *testing.T) {
			predecessor := finiteFixtureGitSource(t, repo, finiteFixtureFeedPredecessor, file)
			historical := bytes.Clone(predecessor)
			before, after, _, _ := finiteFixtureRootInputTransform(file)
			current, err := applyFiniteFixtureRootInput(file, predecessor)
			if err != nil {
				t.Fatal(err)
			}
			c := finiteFixtureRecordedChange(t, repo, file)
			root := t.TempDir()
			path := filepath.Join(root, file)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			for _, mutation := range []string{"none", "wrong-predecessor", "changed-historical-after", "missing-target", "duplicate-target", "unrelated-edit", "reversion", "missing-decision", "missing-edits", "changed-ledger-after"} {
				t.Run(mutation, func(t *testing.T) {
					candidate, parent, original := bytes.Clone(current), bytes.Clone(predecessor), bytes.Clone(historical)
					record := c
					switch mutation {
					case "wrong-predecessor":
						parent = append(parent, '\n')
					case "changed-historical-after":
						original = append(original, '\n')
					case "missing-target":
						candidate = bytes.Replace(candidate, []byte(after), nil, 1)
					case "duplicate-target":
						candidate = bytes.Replace(candidate, []byte(after), []byte(after+after), 1)
					case "unrelated-edit":
						candidate = append(candidate, []byte("# unrelated source edit\n")...)
					case "reversion":
						candidate = bytes.Replace(candidate, []byte(after), []byte(before), 1)
					case "missing-decision":
						record.Review, record.Equivalence = "", nil
					case "missing-edits":
						record.Edits = nil
					case "changed-ledger-after":
						record.AfterHash = strings.Repeat("0", 64)
					}
					if err := os.WriteFile(path, candidate, 0o600); err != nil {
						t.Fatal(err)
					}
					if err := checkFiniteFixtureCurrentOutput(root, record, original, parent); (err == nil) != (mutation == "none") {
						t.Fatalf("composition %s: %v", mutation, err)
					}
				})
			}
			for _, mutation := range []string{"missing", "duplicate"} {
				t.Run("predecessor-target-"+mutation, func(t *testing.T) {
					replacement := ""
					want := "target count=0"
					if mutation == "duplicate" {
						replacement, want = before+before, "target count=2"
					}
					candidate := bytes.Replace(predecessor, []byte(before), []byte(replacement), 1)
					if _, err := applyFiniteFixtureRootInput(file, candidate); err == nil || !strings.Contains(err.Error(), want) {
						t.Fatalf("did not isolate exact target multiplicity: %v", err)
					}
				})
			}
		})
	}
}

func TestRewrite2566FiniteFeedInputCompositionKeepsUnlistedFileStrict(t *testing.T) {
	root := t.TempDir()
	c := fixtureChange("unlisted-schema.yaml")
	before := []byte(c.Edits[0].Before + "instance_variables: {variables: {data: {type: json, default: {initial: true, terminal: false}}}}\n")
	after, err := rewrite(c, before)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"none", "unrelated-edit", "reversion", "cross-listed-composition"} {
		t.Run(mutation, func(t *testing.T) {
			candidate := bytes.Clone(after)
			switch mutation {
			case "unrelated-edit":
				candidate = append(candidate, '\n')
			case "reversion":
				candidate = bytes.Clone(before)
			case "cross-listed-composition":
				file := "tests/tier11-flow-composition/test-numeric-data-scatter-park/schema.yaml"
				predecessor := finiteFixtureGitSource(t, canonicalrouting.RepoRoot(t), finiteFixtureFeedPredecessor, file)
				candidate, err = applyFiniteFixtureRootInput(file, predecessor)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(root, c.File), candidate, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := checkFiniteFixtureCurrentOutput(root, c, after, nil); (err == nil) != (mutation == "none") {
				t.Fatalf("unlisted file %s: %v", mutation, err)
			}
		})
	}
	if _, err := applyFiniteFixtureRootInput(c.File, after); err == nil {
		t.Fatal("unlisted file gained a root-input transformation")
	}
}
