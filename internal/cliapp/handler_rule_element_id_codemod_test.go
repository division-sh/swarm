package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMintElementIDsCommandSupportsSharedOutputModes(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag string
	}{
		{name: "text"},
		{name: "json", flag: "--json"},
		{name: "quiet", flag: "--quiet"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeMintElementIDsCommandFixture(t, `node:
  event_handlers:
    event:
      rules:
        selected: {condition: else}
`)
			args := []string{"mint-element-ids", "--contracts", root}
			if tc.flag != "" {
				args = append(args, tc.flag)
			}
			var out, errOut bytes.Buffer
			code := executeRootCommand(context.Background(), RepoRoot(), args, &out, &errOut)
			if code != 0 || errOut.Len() != 0 {
				t.Fatalf("mint code/stdout/stderr = %d/%q/%q", code, out.String(), errOut.String())
			}
			switch tc.flag {
			case "--json":
				var result mintElementIDsResult
				if err := json.Unmarshal(out.Bytes(), &result); err != nil || result.FilesChanged != 1 || result.IDsMinted != 1 || result.ContractsPath != root {
					t.Fatalf("json result = %#v err=%v output=%q", result, err, out.String())
				}
			case "--quiet":
				if out.String() != "1\n" {
					t.Fatalf("quiet output = %q", out.String())
				}
			default:
				if !strings.Contains(out.String(), "minted 1 stable element ID across 1 file") {
					t.Fatalf("text output = %q", out.String())
				}
			}
			raw, err := os.ReadFile(filepath.Join(root, "nodes.yaml"))
			if err != nil || strings.Count(string(raw), "element_id:") != 1 {
				t.Fatalf("rewritten nodes = %s err=%v", raw, err)
			}
		})
	}
}

func TestMintElementIDsCommandRejectsRetiredScalarWithoutMutation(t *testing.T) {
	root := writeMintElementIDsCommandFixture(t, `node:
  event_handlers:
    event:
      rules: retired-scalar
`)
	path := filepath.Join(root, "nodes.yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := executeRootCommand(context.Background(), RepoRoot(), []string{"mint-element-ids", "--contracts", root}, &out, &errOut)
	if code == 0 || !strings.Contains(errOut.String(), "rule collection must use mapping rows") {
		t.Fatalf("mint code/stderr = %d/%q", code, errOut.String())
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("blocked mint changed file: err=%v\nbefore=%s\nafter=%s", err, before, after)
	}
}

func writeMintElementIDsCommandFixture(t *testing.T, body string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "nodes.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}
