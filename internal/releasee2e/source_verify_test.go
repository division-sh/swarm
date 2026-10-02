package releasee2e

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func Test2376SourceVerificationBinaryExactMemberTable(t *testing.T) {
	work := t.TempDir()
	executable := buildReleaseBinary(t, work)
	root := filepath.Join(work, "source")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "examples/routing/root-ingress"), root)
	writeReleaseFile(t, filepath.Join(root, "README.md"), "Exact published source evidence.\n")
	env := goldenProcessEnv(t, work, "", 0)
	var previous string
	for _, invocation := range []string{"bare", "dot", "mutated"} {
		if invocation == "mutated" {
			writeReleaseFile(t, filepath.Join(root, "README.md"), "Exact changed source evidence.\n")
		}
		args := []string{"verify", "--json"}
		if invocation != "bare" {
			args = []string{"verify", ".", "--json"}
		}
		output := readProofCompiledCommand(t, executable, root, env, args...)
		var evidence struct {
			Hash    string `json:"bundle_hash"`
			Members []struct {
				Label       string `json:"label"`
				Disposition string `json:"disposition"`
				Bytes       uint64 `json:"bytes"`
			} `json:"members"`
		}
		if err := json.Unmarshal([]byte(output), &evidence); err != nil || len(evidence.Members) == 0 {
			t.Fatalf("compiled verify evidence: err=%v output=%s", err, output)
		}
		// Deliberately independent of sourceartifact's encoder and hash helpers.
		var framed bytes.Buffer
		framed.WriteString("swarm-bundle-v2\x00")
		putLength := func(n uint64) {
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], n)
			framed.Write(encoded[:])
		}
		putLength(uint64(len(evidence.Members)))
		codes := map[string]byte{"declaration": 1, "manifest": 2, "resource": 3, "document": 4}
		last := ""
		for _, member := range evidence.Members {
			code, ok := codes[member.Disposition]
			if !ok || member.Label <= last {
				t.Fatalf("invalid or unsorted public member: %+v", member)
			}
			last = member.Label
			body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(member.Label)))
			if err != nil || uint64(len(body)) != member.Bytes {
				t.Fatalf("public table disagrees with authored bytes: %+v err=%v", member, err)
			}
			framed.WriteByte(code)
			putLength(uint64(len(member.Label)))
			framed.WriteString(member.Label)
			putLength(uint64(len(body)))
			framed.Write(body)
		}
		digest := sha256.Sum256(framed.Bytes())
		if want := "bundle-v2:sha256:" + hex.EncodeToString(digest[:]); evidence.Hash != want {
			t.Fatalf("public hash=%s independent=%s", evidence.Hash, want)
		}
		if invocation == "dot" && evidence.Hash != previous {
			t.Fatal("bare and dot source invocations disagree")
		}
		if invocation == "mutated" && evidence.Hash == previous {
			t.Fatal("included document mutation did not change public identity")
		}
		previous = evidence.Hash
	}
}
