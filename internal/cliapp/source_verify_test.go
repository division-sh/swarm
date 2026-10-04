package cliapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func Test2376VerifyEvidenceUsesAdmittedGeneration(t *testing.T) {
	root := outputModeVerifyFixture(t)
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	before := verifyCommandOutput(true, root, runtimepkg.WorkflowContractValidationResult{}, packInventoryReadback{}, artifact)
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "notes.txt"), []byte("replacement rejected by admission"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceartifact.AdmitDirectory(root); err == nil {
		t.Fatal("replacement tree unexpectedly admitted")
	}
	after := verifyCommandOutput(true, root, runtimepkg.WorkflowContractValidationResult{}, packInventoryReadback{}, artifact)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("verify reread source instead of admitted generation: before=%+v after=%+v", before, after)
	}
}

func Test2376SourceInvocationParity(t *testing.T) {
	root := outputModeVerifyFixture(t)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	config := writeTestVerifyRuntimeConfig(t)
	var first verifyCommandResult
	for _, row := range []struct{ name, invocation, operand string }{
		{"bare", root, ""}, {"dot", root, "."}, {"relative", filepath.Dir(root), filepath.Base(root)}, {"absolute", t.TempDir(), root}, {"root_alias", filepath.Dir(alias), "alias"}, {"symlink_invocation", alias, "."},
	} {
		t.Run(row.name, func(t *testing.T) {
			args := []string{"verify", "--portable"}
			if row.operand != "" {
				args = append(args, row.operand)
			}
			args = append(args, "--json", "--config", config)
			var out, errOut bytes.Buffer
			if code := executeRootCommandWithOptions(context.Background(), row.invocation, args, &out, &errOut, defaultRootCommandOptions()); code != 0 {
				t.Fatalf("code=%d %s / %s", code, &out, &errOut)
			}
			var got verifyCommandResult
			if err := json.Unmarshal(out.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if first.BundleHash == "" {
				first = got
			}
			if got.BundleHash != first.BundleHash || !reflect.DeepEqual(got.Members, first.Members) || !reflect.DeepEqual(got.Manifest, first.Manifest) {
				t.Fatalf("source spelling changed exact evidence: %+v / %+v", first, got)
			}
		})
	}
}

func Test2376VerifyJSONExactMemberTable(t *testing.T) {
	for _, named := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "present"}[named], func(t *testing.T) {
			root := outputModeVerifyFixture(t)
			if named {
				if err := os.WriteFile(filepath.Join(root, "manifest.yaml"), []byte("name: Reception\nversion: 1.2.3\nplatform_version: '>=0.7.0 <0.8.0'\n"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Remove(filepath.Join(root, "manifest.yaml")); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			var out, errOut bytes.Buffer
			opts := defaultVerifyCommandOptions()
			opts.portable = true
			opts.sourceRoot, opts.configPath, opts.output.asJSON = root, writeTestVerifyRuntimeConfig(t), true
			if code := runVerifyCommandWithOutput(context.Background(), RepoRoot(), opts, &out, &errOut); code != 0 {
				t.Fatalf("code=%d out=%s err=%s", code, &out, &errOut)
			}
			var got struct {
				Hash    string `json:"bundle_hash"`
				Members []struct {
					Label       string `json:"label"`
					Disposition string `json:"disposition"`
					Bytes       int    `json:"bytes"`
				} `json:"members"`
				Manifest *sourceartifact.Manifest `json:"manifest"`
			}
			if err := json.Unmarshal(out.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			entries := artifact.Entries()
			if got.Hash != artifact.BundleHash() || len(got.Members) != len(entries) || (got.Manifest != nil) != named {
				t.Fatalf("missing exact evidence: %s", &out)
			}
			// Literal spec framing, independent of the artifact encoder.
			var framing bytes.Buffer
			framing.WriteString("swarm-bundle-v2\x00")
			binary.Write(&framing, binary.BigEndian, uint64(len(entries)))
			for i, entry := range entries {
				member := got.Members[i]
				if member.Label != entry.Label() || member.Disposition != entry.Disposition().String() || member.Bytes != entry.Size() {
					t.Fatalf("member[%d]=%#v", i, member)
				}
				code := map[string]byte{"declaration": 1, "manifest": 2, "resource": 3, "document": 4}[member.Disposition]
				framing.WriteByte(code)
				binary.Write(&framing, binary.BigEndian, uint64(len(member.Label)))
				framing.WriteString(member.Label)
				binary.Write(&framing, binary.BigEndian, uint64(member.Bytes))
				framing.Write(entry.Bytes())
			}
			digest := sha256.Sum256(framing.Bytes())
			if got.Hash != "bundle-v2:sha256:"+hex.EncodeToString(digest[:]) {
				t.Fatal("outsider framing differs")
			}
		})
	}
}
