package canonicalrouting

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestArtifactPublicationVerificationConsumersAgree(t *testing.T) {
	for _, mode := range []string{"root", "static"} {
		t.Run(mode, func(t *testing.T) {
			repo := RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, CopyPublicationArtifact(t, mode), contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			for _, finding := range bootverify.Run(context.Background(), semanticview.Wrap(bundle), bootverify.Options{}).HardInvalidities() {
				t.Errorf("%s: %s", finding.CheckID, finding.Message)
			}
		})
	}
}

func TestArtifactPublicationRejectsMissingAndForgedEvidence(t *testing.T) {
	type mutation struct{ name, file, from, to, want string }
	cases := []mutation{
		{"missing_success_addition", "nodes.yaml", "success_payload: {result_kind: {literal: ready}}", "success_payload: {}", "requires payload field result_kind"},
		{"missing_failure_addition", "nodes.yaml", "failure_payload: {result_kind: {literal: failed}}", "failure_payload: {}", "requires payload field result_kind"},
		{"success_runtime_override", "nodes.yaml", "success_payload: {result_kind: {literal: ready}}", "success_payload: {result_kind: {literal: ready}, request_id: {literal: forged}}", "must not override runtime-owned field request_id"},
		{"failure_runtime_override", "nodes.yaml", "failure_payload: {result_kind: {literal: failed}}", "failure_payload: {result_kind: {literal: failed}, failure: {literal: forged}}", "must not override runtime-owned field failure"},
		{"success_missing_runtime_schema", "events.yaml", "  current_ref: text\n", "", "schema is missing runtime-owned field current_ref"},
		{"failure_missing_runtime_schema", "events.yaml", "  failure: platform.failure/v1 envelope\n", "", "schema is missing runtime-owned field failure"},
	}
	for _, field := range []string{"repo_url", "current_ref", "file_manifest", "status", "failure", "last_request_id", "last_source_event_id"} {
		line := "            " + field + ": " + field + "\n"
		cases = append(cases,
			mutation{"missing_output_" + field, "nodes.yaml", line, "", "missing artifact_repo.output." + field},
			mutation{"wrong_output_" + field, "nodes.yaml", line, "            " + field + ": not_declared\n", "not_declared"},
		)
	}
	for _, mode := range []string{"root", "static"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				root := CopyPublicationArtifact(t, mode)
				owner := root
				if mode == "static" {
					owner = filepath.Join(root, "source")
				}
				path := filepath.Join(owner, tc.file)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Count(string(data), tc.from) != 1 {
					t.Fatal("hostile mutation must change exactly one authored declaration")
				}
				if err := os.WriteFile(path, []byte(strings.Replace(string(data), tc.from, tc.to, 1)), 0600); err != nil {
					t.Fatal(err)
				}
				repo := RepoRoot(t)
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatalf("hostile case must reach verifier: %v", err)
				}
				findings := bootverify.Run(context.Background(), semanticview.Wrap(bundle), bootverify.Options{}).HardInvalidities()
				for _, finding := range findings {
					if strings.Contains(finding.Message, tc.want) {
						return
					}
				}
				t.Fatalf("missing exact rejection %q: %+v", tc.want, findings)
			})
		}
	}
}
