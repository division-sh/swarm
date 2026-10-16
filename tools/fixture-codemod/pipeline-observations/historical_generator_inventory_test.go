package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type historicalGeneratorCase struct {
	name, predecessor, manifest, prefix string
	trimNewline                         bool
}

var historicalGeneratorCases = []historicalGeneratorCase{
	{"migrate-fork-companions", "f0ce6f7bea8f8f89d44f98a317ed0432e2ba90ce", "family145-fork-companion-preimages.json", "calls=22 bodies=7 manifest_sha256=", false},
	{"migrate-fork-notices", "62f09eb5e15afb9a9b9ed9838336254944dd6be7", "family145-fork-notice-preimages.json", "calls=16 bodies=4 manifest_sha256=", false},
	{"migrate-public-a2-endpoints", "9c964b502e52681d01d02c678b67ba996e5aed8a", "internal/publicendpoint/preimages.json", "calls=67 bodies=16 source_manifest_sha256=", false},
	{"migrate-public-join", "c760d28bd28b84c7b62578cbe7037833d441747f", "family145-public-join-preimages.json", "calls=31 helpers=7 bodies=10 manifest_sha256=", false},
	{"migrate-receiver-public-readback", "13f976220fbe24e0271434c10e2f6eba29665657", "family145-receiver-public-readback-preimages.json", "calls=15 bodies=15 manifest_sha256=", false},
	{"migrate-served-debug", "bfdcdc112f7053494ea597c053aaa49678804bbc", "family145-served-debug-preimages.json", "debug=63 bodies=85 preimages_sha256=", true},
	{"migrate-fork-completion", "daf6590ef1013decce2a7562ffb480dd9ca5cf32", "family145-fork-completion-preimages.json", "callers=30 bodies=17 preimages_sha256=", true},
}

func TestHistoricalGeneratorsIntersectOwnedCheckoutWithImmutablePredecessorFiles(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	const added = "internal/serveapp/channel_intent_identity_test.go"
	if _, err := os.Stat(filepath.Join(root, added)); err != nil {
		t.Fatal("new owned checkout source is missing:", err)
	}
	for _, proof := range historicalGeneratorCases {
		t.Run(proof.name, func(t *testing.T) {
			check := exec.Command("git", "show", proof.predecessor+":"+added)
			check.Dir = root
			if check.Run() == nil {
				t.Fatal("new checkout source unexpectedly existed in historical inventory")
			}
			raw, err := os.ReadFile(proof.manifest)
			if err != nil {
				t.Fatal(err)
			}
			if proof.trimNewline {
				raw = []byte(strings.TrimSuffix(string(raw), "\n"))
			}
			want := proof.prefix + fmt.Sprintf("%x", sha256.Sum256(raw))
			output, err := runHistoricalGenerator(root, proof.name, nil)
			if err != nil || !strings.Contains(output, want+"\n") {
				t.Fatalf("historical generator lost pinned inventory/body evidence: %v\n%s\nwant %s", err, output, want)
			}
		})
	}
}

func TestHistoricalGeneratorInventoryNeverSwallowsMissingOrChangedHistoricalInputs(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	const wrapper = `#!/usr/bin/env bash
set -euo pipefail
if [ "$1" = ls-tree ] && [ "$HISTORICAL_INPUT_FAULT" = missing_commit ]; then
  printf 'deliberate missing historical commit\n' >&2
  exit 128
fi
if [ "$1" = show ] && [ "$HISTORICAL_INPUT_FAULT" = missing_source ]; then
  printf 'deliberate missing pinned historical source\n' >&2
  exit 128
fi
if [ "$1" = show ] && [ "$HISTORICAL_INPUT_FAULT" = changed_source ] && [ "$2" = "$HISTORICAL_INPUT_PIN:internal/serveapp/fork_receiver_ownership_test.go" ]; then
  printf 'package serveapp\n'
  exit 0
fi
exec "$HISTORICAL_INPUT_REAL_GIT" "$@"
`
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"missing_commit", "missing_source", "changed_source"} {
		t.Run(fault, func(t *testing.T) {
			environment := []string{
				"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
				"HISTORICAL_INPUT_FAULT=" + fault,
				"HISTORICAL_INPUT_REAL_GIT=" + realGit,
				"HISTORICAL_INPUT_PIN=" + historicalGeneratorCases[0].predecessor,
			}
			output, err := runHistoricalGenerator(root, historicalGeneratorCases[0].name, environment)
			if err == nil {
				t.Fatalf("historical %s became accepted output:\n%s", fault, output)
			}
			if fault == "changed_source" && !strings.Contains(output, "companion inventory") || fault != "changed_source" && !strings.Contains(output, "exit status 128") {
				t.Fatalf("refused for the wrong historical-input reason: %v\n%s", err, output)
			}
		})
	}
}

func runHistoricalGenerator(root, name string, environment []string) (string, error) {
	command := exec.Command("go", "run", "./tools/fixture-codemod/pipeline-observations/"+name)
	command.Dir = root
	command.Env = append(os.Environ(), environment...)
	body, err := command.CombinedOutput()
	return string(body), err
}

func TestHistoricalGeneratorInventoryRepairPreservesEveryImmutableManifest(t *testing.T) {
	unchanged := map[string]string{
		"migrate-fork-companions":          "24a214ba558ea44202656a48ad6695f6344254c5f9c335fda2a392d16c34054a",
		"migrate-fork-notices":             "2c569e9fcb4d63d943f640c11245a4d3d00cdcf62653ec768ec70af713abb03e",
		"migrate-public-a2-endpoints":      "4f37a50d17478fdbd4cb64bb8df0d73c5722c7e92578878e20e277950cc83d1c",
		"migrate-public-join":              "d1f25c42a603866353f1f33a5cc479205eee723b49afa4166565462d1eb13a94",
		"migrate-receiver-public-readback": "a75db13f04646436e22522c83c9a04d929d12e384d9b48bfa585fe5bdf499709",
		"migrate-served-debug":             "5bc30573335e4c9efc8915f7db12eb345938e927b347bf368007c1b899a8bd4a",
		"migrate-fork-completion":          "3353893f3bba3f0fc8e91e567ad6a0400790a3b92d8ca66489557e11c6249ff3",
	}
	for _, proof := range historicalGeneratorCases {
		raw, err := os.ReadFile(proof.manifest)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(raw)) != unchanged[proof.name] || !json.Valid(raw) {
			t.Fatal("historical inventory/body manifest changed:", proof.manifest, err)
		}
	}
}
