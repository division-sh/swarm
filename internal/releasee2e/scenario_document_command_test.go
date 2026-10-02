package releasee2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPublicScenarioDocumentCommandAdmissionAndMaterialization(t *testing.T) {
	releaseRoot := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, releaseRoot)
	source := filepath.Join(t.TempDir(), "bundle")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "examples", "routing", "parent-connect"), source)
	writeReleaseFile(t, filepath.Join(source, "tests", "fixture.json"), `{"work_id":"${vars.marker}"}`)
	writeReleaseFile(t, filepath.Join(source, "tests", "canonical.yaml"), `
name: release scenario
seed: recorded
vars: {marker: "${'${1 + 1}'}"}
steps:
  - publish: work.requested
    payload: {work_id: "${vars.marker}"}
  - publish: work.requested
    payload: {from: fixture.json}
  - publish: work.requested
    payload: "${{'work_id': vars.marker}}"
invalid:
  base: {publish: work.requested, payload: {work_id: valid}}
  cases: [{name: wrong-type, set: {work_id: 7}}]
expect: {events: [work.requested], no_dead_letters: true}
`)
	env := goldenProcessEnv(t, releaseRoot, "", 0)
	assertGoldenProcessHasNoExternalExecutables(t, env)
	result := runReleaseCommand(t, 30*time.Second, releaseE2ERepoRoot(t), env, "", binary, "test", source, "tests/canonical.yaml", "--timeout", "10s", "--poll-interval", "25ms")
	if result.err != nil || !strings.Contains(result.output, "swarm test ok: scenarios=1") {
		t.Fatalf("public command: %v\n%s", result.err, result.output)
	}
	for _, row := range []struct{ name, body, teaching string }{
		{"version", "version: 1\nsteps: [{publish: work.requested, payload: {work_id: valid}}]\n", "remove `version`; the scenario format has one version"},
		{"invalid-expect", "steps: [{publish: work.requested, payload: {work_id: valid}}]\ninvalid: {base: {publish: work.requested, payload: {work_id: valid}}, cases: [{expect: reject}]}\n", "remove `expect`"},
		{"invalid-base", "steps: [{publish: work.requested, payload: {work_id: valid}}]\ninvalid: {base: {publish: missing, payload: {}}, cases: [{set: {work_id: 7}}]}\n", "invalid.base must be valid"},
		{"empty-exact", "steps: [{publish: work.requested, payload: {work_id: valid}}]\nexpect: {events: {exact: []}}\n", "event exact expectation mismatch"},
	} {
		label := filepath.Join("tests", row.name+".yaml")
		writeReleaseFile(t, filepath.Join(source, label), row.body)
		got := runReleaseCommand(t, 30*time.Second, releaseE2ERepoRoot(t), env, "", binary, "test", source, label, "--timeout", "10s", "--poll-interval", "25ms")
		if got.err == nil || !strings.Contains(got.output, row.teaching) {
			t.Fatalf("%s: %v\n%s", row.name, got.err, got.output)
		}
		if err := os.Remove(filepath.Join(source, label)); err != nil {
			t.Fatal(err)
		}
	}
}
