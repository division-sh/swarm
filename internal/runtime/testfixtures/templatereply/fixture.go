package templatereply

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	RequesterFlowID     = "requester"
	RequesterRequestPin = "provider_requested"
	RequesterReplyPin   = "provider_replied"
	RequesterNodeID     = "requester-node"
	ProviderFlowID      = "provider"
	ProviderRequestPin  = "provider_requested"
	ProviderReplyPin    = "provider_replied"
	ProviderNodeID      = "provider-node"
	RequestEvent        = "provider.requested"
	ReplyEvent          = "provider.replied"
	CorrelationKey      = "provider_request_id"
	ContinuationMailbox = "mailbox"
	ContinuationHuman   = "human_task"
)

type Options struct {
	MissingRepliesTo          bool
	MissingCorrelationCarry   bool
	AmbiguousRequestEdge      bool
	MismatchedProvider        bool
	DefaultEventIDCorrelation bool
	ProviderContinuation      string
	ContinuationRequestKey    string
	ContinuationAccountID     string
}

func LoadBundle(t testing.TB, opts Options) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := LoadBundleResult(t, opts)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadBundleResult(t testing.TB, opts Options) (*runtimecontracts.WorkflowContractBundle, error) {
	t.Helper()
	root := Write(t, opts)
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRoot(t)))
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "package.yaml"), packageYAML(opts))
	writeFile(t, filepath.Join(root, "schema.yaml"), "name: template-reply\n")
	for _, name := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml"} {
		writeFile(t, filepath.Join(root, name), "{}\n")
	}
	writeRequester(t, root, opts)
	writeProvider(t, root, "provider", opts)
	if opts.MismatchedProvider {
		writeProvider(t, root, "other-provider", Options{})
	}
	return root
}

func packageYAML(opts Options) string {
	extraFlow := ""
	replyFrom := "provider.provider_replied"
	if opts.MismatchedProvider {
		extraFlow = "  - id: other-provider\n    flow: other-provider\n    mode: static\n"
		replyFrom = "other-provider.provider_replied"
	}
	extraRequest := ""
	if opts.AmbiguousRequestEdge {
		extraRequest = "  - from: requester.provider_requested\n    to: provider.provider_requested\n"
	}
	return `name: template-reply
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: requester
    flow: requester
    mode: template
  - id: provider
    flow: provider
    mode: static
` + extraFlow + `connect:
  - from: requester.provider_requested
    to: provider.provider_requested
` + extraRequest + `  - from: ` + replyFrom + `
    to: requester.provider_replied
`
}

func writeRequester(t testing.TB, root string, opts Options) {
	repliesTo := "          replies_to: provider_requested\n"
	if opts.MissingRepliesTo {
		repliesTo = ""
	}
	carries := "        carries: [provider_request_id]\n"
	if opts.MissingCorrelationCarry {
		carries = ""
	}
	correlation := "          correlation_key: provider_request_id\n"
	if opts.DefaultEventIDCorrelation {
		correlation = ""
	}
	writeFile(t, filepath.Join(root, "flows", "requester", "schema.yaml"), `
name: requester
mode: template
instance:
  by: account_id
  on_missing: reject
  on_conflict: reject
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: provider_replied
        event: provider.replied
        resolution:
          mode: reply
`+repliesTo+correlation+`  outputs:
    events:
      - name: provider_requested
        event: provider.requested
        key: provider_request_id
`+carries)
	writeFlowSupportFiles(t, root, "requester")
	writeFile(t, filepath.Join(root, "flows", "requester", "entities.yaml"), `
requester_state:
  account_id:
    type: text
    _unused_reason: template reply origin identity fixture field
`)
	writeFile(t, filepath.Join(root, "flows", "requester", "events.yaml"), eventContractsYAML())
	writeFile(t, filepath.Join(root, "flows", "requester", "nodes.yaml"), `
requester-node:
  id: requester-node
  execution_type: system_node
  subscribes_to: [provider.replied]
  event_handlers:
    provider.replied:
      advances_to: active
`)
}

func writeProvider(t testing.TB, root, flowID string, opts Options) {
	writeFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
mode: static
pins:
  inputs:
    events:
      - name: provider_requested
        event: provider.requested
`+providerContinuationInputsYAML(opts)+`  outputs:
    events:
      - name: provider_replied
        event: provider.replied
        key: provider_request_id
        carries: [provider_request_id]
`)
	writeFlowSupportFiles(t, root, flowID)
	writeFile(t, filepath.Join(root, "flows", flowID, "entities.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), eventContractsYAML())
	writeFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), providerNodesYAML(opts))
}

func providerContinuationInputsYAML(opts Options) string {
	switch opts.ProviderContinuation {
	case ContinuationMailbox:
		return `      - name: mailbox_deferred
        event: mailbox.item_deferred
      - name: mailbox_decided
        event: mailbox.item_decided
`
	case ContinuationHuman:
		return `      - name: human_task_deferred
        event: human_task.deferred
      - name: human_task_approved
        event: human_task.approved
`
	default:
		return ""
	}
}

func providerNodesYAML(opts Options) string {
	switch opts.ProviderContinuation {
	case ContinuationMailbox:
		return `
provider-node:
  id: provider-node
  execution_type: system_node
  subscribes_to: [provider.requested, mailbox.item_deferred, mailbox.item_decided]
  produces: [provider.replied]
  event_handlers:
    provider.requested:
      action:
        id: mailbox_write
        mailbox:
          item_type:
            literal: approval
          summary:
            literal: Review provider response
          payload:
            provider_request_id:
              ref: payload.provider_request_id
            account_id:
              ref: payload.account_id
    mailbox.item_deferred: {}
    mailbox.item_decided:
      emit:
        event: provider.replied
        fields:
          provider_request_id: payload.mailbox_payload.provider_request_id
          account_id: payload.mailbox_payload.account_id
          result:
            literal: approved
`
	case ContinuationHuman:
		requestKey := opts.ContinuationRequestKey
		if requestKey == "" {
			requestKey = "human-request"
		}
		accountID := opts.ContinuationAccountID
		if accountID == "" {
			accountID = "account-a"
		}
		return `
provider-node:
  id: provider-node
  execution_type: system_node
  subscribes_to: [provider.requested, human_task.deferred, human_task.approved]
  produces: [provider.replied]
  event_handlers:
    provider.requested: {}
    human_task.deferred: {}
    human_task.approved:
      emit:
        event: provider.replied
        fields:
          provider_request_id:
            literal: ` + requestKey + `
          account_id:
            literal: ` + accountID + `
          result:
            literal: approved
`
	default:
		return `
provider-node:
  id: provider-node
  execution_type: system_node
  subscribes_to: [provider.requested]
  event_handlers:
    provider.requested: {}
`
	}
}

func eventContractsYAML() string {
	return `
provider.requested:
  provider_request_id: text
  account_id: text
  required: [provider_request_id, account_id]
provider.replied:
  provider_request_id: text
  account_id: text
  result: text
  required: [provider_request_id, account_id, result]
`
}

func writeFlowSupportFiles(t testing.TB, root, flowID string) {
	for _, name := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeFile(t, filepath.Join(root, "flows", flowID, name), "{}\n")
	}
}

func writeFile(t testing.TB, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve template reply fixture path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}
