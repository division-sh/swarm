package batchagentcoordinatorpilot

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	CoordinatorFlowID = "coordinator"
	AccountFlowID     = "accounts"
	NodeID            = "batch-classifier"
	InputEvent        = "account.profile_observed"
	OutputEvent       = "account.classified"
	OutputPin         = "account_classified"
	AgentID           = "profile-classifier"
)

func LoadBundle(t testing.TB) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := LoadBundleResult(t)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadBundleResult(t testing.TB) (*runtimecontracts.WorkflowContractBundle, error) {
	t.Helper()
	root := Write(t)
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRoot(t)))
}

func LoadSource(t testing.TB) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t))
}

func Write(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "package.yaml"), `
name: batch-agent-coordinator-pilot
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: coordinator
    flow: coordinator
    mode: singleton
  - id: accounts
    flow: accounts
    mode: template
connect:
  - from: coordinator.account_classified
    to: accounts.account_classified
    delivery: one
`)
	writeRootFiles(t, root)
	writeCoordinator(t, root)
	writeAccounts(t, root)
	return root
}

func writeRootFiles(t testing.TB, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "schema.yaml"), "name: batch-agent-coordinator-pilot\n")
	writeFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
}

func writeCoordinator(t testing.TB, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "coordinator", "schema.yaml"), `
name: coordinator
mode: singleton
initial_state: collecting
states: [collecting, classified]
required_agents:
  - role: profile-classifier
    subscribes_to: []
    emits: []
pins:
  inputs:
    events:
      - name: profile_observed
        event: account.profile_observed
  outputs:
    events:
      - name: account_classified
        event: account.classified
        key: account_id
        carries: [account_id, bucket, score, handle]
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "coordinator", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "coordinator", "agents.yaml"), `
profile-classifier:
  id: profile-classifier
  role: profile-classifier
  model: cheap
  mode: task
  subscriptions: [account.profile_observed]
  max_turns_per_task: 1
  prompt_ref: prompts/profile_classifier.md
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "prompts", "profile_classifier.md"), `
Classify each profile row and return strict JSON:
{"results":[{"account_id":"...","bucket":"...","score":0}]}
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "types.yaml"), `
types:
  ProfileObservation:
    account_id: text
    handle: text
    followers: integer
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "entities.yaml"), `
coordinator_state:
  coordinator_id: text
  pending_profiles:
    type: "[ProfileObservation]"
    initial: []
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "events.yaml"), `
account.profile_observed:
  swarm:
    source: external (batch-agent coordinator pilot ingress)
  coordinator_id: text
  expected_count: integer
  account_id: text
  handle: text
  followers: integer
account.classified:
  account_id: text
  bucket: text
  score: integer
  handle: text
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "nodes.yaml"), `
batch-classifier:
  id: batch-classifier
  execution_type: system_node
  subscribes_to: [account.profile_observed]
  produces: [account.classified]
  event_handlers:
    account.profile_observed:
      select_entity:
        by:
          coordinator_id: payload.coordinator_id
      data_accumulation:
        writes:
          - source_field: coordinator_id
            target_field: coordinator_id
          - op: append
            target: entity.pending_profiles
            value:
              account_id: fixture-account
              handle: fixture-handle
              followers: 0
      accumulate:
        into: pending_profiles
        expected_from: payload.expected_count
        completion: all
        dedup_by: payload.account_id
        on_complete:
          - id: classify-ready
            condition: else
            advances_to: classified
            batch_agent:
              agent: profile-classifier
              items_from: accumulated.items
              input:
                instruction: Classify each account profile by sales fit.
                coordinator_id:
                  ref: entity.coordinator_id
              result:
                items_from: results
                correlation_key: account_id
                required_fields: [account_id, bucket, score]
              emit:
                event: account.classified
                fields:
                  account_id: batch_agent.result.account_id
                  bucket: batch_agent.result.bucket
                  score: batch_agent.result.score
                  handle: batch_agent.source_item.handle
  state_schema:
    fields:
      pending_profiles: "[ProfileObservation]"
`)
}

func writeAccounts(t testing.TB, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "accounts", "schema.yaml"), `
name: accounts
mode: template
initial_state: pending
states: [pending, classified]
terminal_states: [classified]
instance:
  by: account_id
  on_missing: create
  on_conflict: reuse
pins:
  inputs:
    events:
      - name: account_classified
        event: account.classified
  outputs:
    events: []
`)
	writeFile(t, filepath.Join(root, "flows", "accounts", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "accounts", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "accounts", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "accounts", "types.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "accounts", "entities.yaml"), `
account:
  account_id: text
  bucket: text
  score: integer
  handle: text
`)
	writeFile(t, filepath.Join(root, "flows", "accounts", "events.yaml"), `
account.classified:
  account_id: text
  bucket: text
  score: integer
  handle: text
`)
	writeFile(t, filepath.Join(root, "flows", "accounts", "nodes.yaml"), `
classification-recorder:
  id: classification-recorder
  execution_type: system_node
  subscribes_to: [account.classified]
  event_handlers:
    account.classified:
      data_accumulation:
        writes:
          - source_field: account_id
            target_field: account_id
          - source_field: bucket
            target_field: bucket
          - source_field: score
            target_field: score
          - source_field: handle
            target_field: handle
      advances_to: classified
`)
}

func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repo root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func writeFile(t testing.TB, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
