package releasee2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestPublicScenarioDocumentCommandAdmissionAndMaterialization(t *testing.T) {
	releaseRoot := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, releaseRoot)
	source := canonicalrouting.WriteNovelDerivedScenarioBundleWithRootInput(t)
	writeReleaseFile(t, filepath.Join(source, "tests", "fixture.json"), `{"order_id":"${vars.marker}"}`)
	writeReleaseFile(t, filepath.Join(source, "tests", "canonical.yaml"), `
name: release scenario
seed: recorded
vars: {marker: "${'${1 + 1}'}"}
steps:
  - publish: fulfillment.requested
    payload: {order_id: "${vars.marker}"}
  - publish: fulfillment.requested
    payload: {from: fixture.json}
  - publish: fulfillment.requested
    payload: "${{'order_id': vars.marker}}"
invalid:
  base: {publish: fulfillment.requested, payload: {order_id: valid}}
  cases: [{name: wrong-type, set: {order_id: 7}}]
expect: {events: [fulfillment.requested], no_dead_letters: true}
`)
	env := goldenProcessEnv(t, releaseRoot, "", 0)
	assertGoldenProcessHasNoExternalExecutables(t, env)
	result := runReleaseCommand(t, 30*time.Second, releaseE2ERepoRoot(t), env, "", binary, "test", source, "tests/canonical.yaml", "--timeout", "10s", "--poll-interval", "25ms")
	if result.err != nil || !strings.Contains(result.output, "swarm test ok: scenarios=1") {
		t.Fatalf("public command: %v\n%s", result.err, result.output)
	}
	for _, row := range []struct{ name, body, teaching string }{
		{"version", "version: 1\nsteps: [{publish: fulfillment.requested, payload: {order_id: valid}}]\n", "remove `version`; the scenario format has one version"},
		{"invalid-expect", "steps: [{publish: fulfillment.requested, payload: {order_id: valid}}]\ninvalid: {base: {publish: fulfillment.requested, payload: {order_id: valid}}, cases: [{expect: reject}]}\n", "remove `expect`"},
		{"invalid-base", "steps: [{publish: fulfillment.requested, payload: {order_id: valid}}]\ninvalid: {base: {publish: missing, payload: {}}, cases: [{set: {order_id: 7}}]}\n", "invalid.base must be valid"},
		{"empty-exact", "steps: [{publish: fulfillment.requested, payload: {order_id: valid}}]\nexpect: {events: {exact: []}}\n", "event exact expectation mismatch"},
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
