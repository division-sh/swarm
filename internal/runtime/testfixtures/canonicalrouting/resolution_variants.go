package canonicalrouting

import (
	"path/filepath"
	"testing"
)

type SelectResolutionMode uint8

const (
	SelectResolutionSelect SelectResolutionMode = iota + 1
	SelectResolutionSelectOrCreate
)

type SelectResolutionInvalidity uint8

const (
	SelectResolutionValid SelectResolutionInvalidity = iota
	SelectResolutionUndeclaredSource
	SelectResolutionSourceTypeMismatch
	SelectResolutionStaticReceiver
	SelectResolutionExtraAggregation
	SelectResolutionEntityTypeMismatch
	SelectResolutionSourceTypeMismatchWithoutCarryType
	SelectResolutionNumberSourceToIntegerReceiver
)

type TemplateSelectResolutionOptions struct {
	Mode       SelectResolutionMode
	Invalidity SelectResolutionInvalidity
}

// CopyTemplateSelectOrCreateFinalAuthoring returns the fixed stateful
// authoring variant used by the final-flow conformance surface.
func CopyTemplateSelectOrCreateFinalAuthoring(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, TemplateSelectOrCreate)
	applyTemplateSelectOrCreateAccumulation(t, root, "reviewed")
	return root
}

// CopyTemplateSelectOrCreatePilot returns the fixed stateful pilot variant.
func CopyTemplateSelectOrCreatePilot(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, TemplateSelectOrCreate)
	applyTemplateSelectOrCreateAccumulation(t, root, "done")
	return root
}

// CopyTemplateCreateThenSelectSameEvent proves that one event may create a
// template instance and then select that request-local instance on a later edge.
func CopyTemplateCreateThenSelectSameEvent(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, TemplateSelectExisting)
	applyClosedReplacement(t, filepath.Join(root, "flows/account/schema.yaml"),
		"      - event: account.setup\n",
		"      - event: account.create\n")
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"),
		"  - event: account.setup\n    from: producer\n    to: account\n  - event: account.ready\n    from: producer\n    to: account\n",
		"  - event: account.setup\n    from: producer\n    to: account\n    rename: account.create\n  - event: account.setup\n    from: producer\n    to: account\n    rename: account.ready\n")
	writeClosedVariantFile(t, root, "flows/account/nodes.yaml", `account-setup-node:
  id: account-setup-node-{instance_id}
  execution_type: system_node
  subscribes_to: [account.create]
  event_handlers:
    account.create: {}
account-ready-node:
  id: account-ready-node-{instance_id}
  execution_type: system_node
  subscribes_to: [account.ready]
  event_handlers:
    account.ready: {}
`)
	return root
}

// CopyTemplateSelectAgentOnlyWithUnrelatedNode proves that a connect-selected
// agent delivery does not imply delivery authority for another node in the flow.
func CopyTemplateSelectAgentOnlyWithUnrelatedNode(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, TemplateSelectExisting)
	writeClosedVariantFile(t, root, "flows/account/nodes.yaml", `account-setup-node:
  id: account-setup-node-{instance_id}
  execution_type: system_node
  subscribes_to: [account.setup]
  event_handlers:
    account.setup: {}
`)
	writeClosedVariantFile(t, root, "flows/account/agents.yaml", `account-agent:
  id: account-agent
  model: regular
  intent:
    inline: Consume account readiness events.
  subscriptions: [account.ready]
`)
	return root
}

func applyTemplateSelectOrCreateAccumulation(t testing.TB, root, terminalState string) {
	t.Helper()
	producerEvents := filepath.Join(root, "flows", "producer", "events.yaml")
	for _, event := range []string{"account.requested", "account.ready"} {
		key, fieldType := "", "text?"
		if event == "account.ready" {
			key = "  key: account_id\n"
			fieldType = "text"
		}
		applyClosedReplacement(t, producerEvents,
			event+":\n"+key+"  account_id: "+fieldType+"\n",
			event+":\n"+key+"  account_id: "+fieldType+"\n  score: text\n  decision: text\n")
	}
	applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "nodes.yaml"),
		"          account_id: payload.account_id\n",
		"          account_id: payload.account_id\n          score: payload.score\n          decision: payload.decision\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "account", "entities.yaml"),
		"    _unused_reason: receiver instance identity\n",
		"    _unused_reason: receiver instance identity\n  score:\n    type: text\n  decision:\n    type: text\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "account", "nodes.yaml"),
		"    account.ready: {}\n",
		`    account.ready:
      data_accumulation:
        writes:
          - source_field: account_id
            target_field: account_id
          - source_field: score
            target_field: score
          - source_field: decision
            target_field: decision
      advances_to: `+terminalState+"\n")
}

// CopyTemplateSelectResolution derives the closed select validation/lowering
// matrix from the checked-in select-existing artifact.
func CopyTemplateSelectResolution(t testing.TB, opts TemplateSelectResolutionOptions) string {
	t.Helper()
	if opts.Mode == 0 {
		opts.Mode = SelectResolutionSelect
	}
	mode := "select"
	if opts.Mode == SelectResolutionSelectOrCreate {
		mode = "select-or-create"
	} else if opts.Mode != SelectResolutionSelect {
		t.Fatalf("unsupported select resolution mode %d", opts.Mode)
	}
	root := CopyExample(t, TemplateSelectExisting)
	packageFile := filepath.Join(root, "package.yaml")
	accountSchema := filepath.Join(root, "flows", "account", "schema.yaml")
	applyClosedReplacement(t, filepath.Join(root, "flows", "account", "nodes.yaml"),
		"  id: account-node\n", "  id: account-node-{instance_id}\n")
	applyClosedReplacement(t, packageFile, "  - event: account.setup\n    from: producer\n    to: account\n", "")
	selectedPin := "      - event: account.ready\n        resolution:\n          mode: " + mode + "\n"
	applyClosedReplacement(t, accountSchema,
		"      - event: account.ready\n        resolution:\n          mode: select\n",
		selectedPin)

	switch opts.Invalidity {
	case SelectResolutionValid:
	case SelectResolutionUndeclaredSource:
		applyClosedReplacement(t, accountSchema, selectedPin,
			"      - event: account.ready\n        resolution:\n          mode: "+mode+"\n          from: payload.missing_account_id\n")
	case SelectResolutionSourceTypeMismatch:
		applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "events.yaml"), "account.ready:\n  key: account_id\n  account_id: text\n", "account.ready:\n  key: account_id\n  account_id: integer\n")
	case SelectResolutionStaticReceiver:
		applyClosedReplacement(t, packageFile, "    mode: template\nconnect:\n", "    mode: static\nconnect:\n")
		applyClosedReplacement(t, accountSchema, "mode: template\n", "mode: static\n")
	case SelectResolutionExtraAggregation:
		applyClosedReplacement(t, accountSchema, selectedPin,
			"      - event: account.ready\n        resolution:\n          mode: "+mode+"\n          aggregation: stream\n")
	case SelectResolutionEntityTypeMismatch:
		applyClosedReplacement(t, filepath.Join(root, "flows", "account", "entities.yaml"), "    type: text\n", "    type: integer\n")
	case SelectResolutionSourceTypeMismatchWithoutCarryType:
		applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "events.yaml"), "account.ready:\n  key: account_id\n  account_id: text\n", "account.ready:\n  key: account_id\n  account_id: integer\n")
	case SelectResolutionNumberSourceToIntegerReceiver:
		applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "events.yaml"), "account.ready:\n  key: account_id\n  account_id: text\n", "account.ready:\n  key: account_id\n  account_id: number\n")
		applyClosedReplacement(t, filepath.Join(root, "flows", "account", "entities.yaml"), "    type: text\n", "    type: integer\n")
	default:
		t.Fatalf("unsupported select resolution invalidity %d", opts.Invalidity)
	}
	return root
}

// CopyTemplateSelectResolutionRenamedSource keeps the receiver identity name
// while sourcing it from a differently named, schema-declared payload field.
func CopyTemplateSelectResolutionRenamedSource(t testing.TB, opts TemplateSelectResolutionOptions) string {
	t.Helper()
	root := CopyTemplateSelectResolution(t, opts)
	mode := "select"
	if opts.Mode == SelectResolutionSelectOrCreate {
		mode = "select-or-create"
	}
	applyClosedReplacement(t, filepath.Join(root, "flows", "account", "schema.yaml"),
		"event: account.ready\n        resolution:\n          mode: "+mode+"\n",
		"event: account.ready\n        resolution:\n          mode: "+mode+"\n          from: payload.external_account_id\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "events.yaml"),
		"account.ready:\n  key: account_id\n  account_id: text\n", "account.ready:\n  key: account_id\n  account_id: text\n  external_account_id: text\n")
	return root
}

type CreateMint uint8

const (
	CreateMintUUID CreateMint = iota + 1
	CreateMintEventID
	CreateMintPayload
)

type CreateResolutionInvalidity uint8

const (
	CreateResolutionValid CreateResolutionInvalidity = iota
	CreateResolutionNonRunnableMode
	CreateResolutionInvalidMint
	CreateResolutionProducerCollision
	CreateResolutionSourceTypeMismatchWithoutCarryType
	CreateResolutionNumberSourceToIntegerReceiver
)

type TemplateCreateResolutionOptions struct {
	Mint       CreateMint
	Invalidity CreateResolutionInvalidity
}

func CopyTemplateCreateResolution(t testing.TB, opts TemplateCreateResolutionOptions) string {
	t.Helper()
	if opts.Mint == 0 {
		opts.Mint = CreateMintUUID
	}
	root := CopyExample(t, TemplateCreateMintedKey)
	validatorSchema := filepath.Join(root, "flows", "validator", "schema.yaml")
	switch opts.Mint {
	case CreateMintUUID:
	case CreateMintEventID:
		applyClosedReplacement(t, validatorSchema, "          from: generated.uuid\n", "          from: event.id\n")
	case CreateMintPayload:
		applyClosedReplacement(t, validatorSchema, "          from: generated.uuid\n", "          from: payload.candidate\n")
		applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "events.yaml"),
			"validation.requested:\n  candidate: text?\n",
			"validation.requested:\n  key: candidate\n  candidate: text\n")
	default:
		t.Fatalf("unsupported create mint %d", opts.Mint)
	}
	switch opts.Invalidity {
	case CreateResolutionValid:
	case CreateResolutionNonRunnableMode:
		applyClosedReplacement(t, validatorSchema, "          mode: create\n", "          mode: fan-out\n")
	case CreateResolutionInvalidMint:
		applyClosedReplacement(t, validatorSchema, "          from: generated.uuid\n", "          from: generated.random\n")
	case CreateResolutionProducerCollision:
		applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "events.yaml"), "validation.requested:\n  candidate: text?\n", "validation.requested:\n  candidate: text?\n  validation_case_id: uuid\n")
		applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "          candidate: payload.candidate\n", "          candidate: payload.candidate\n          validation_case_id: payload.candidate\n")
	case CreateResolutionSourceTypeMismatchWithoutCarryType:
		if opts.Mint == CreateMintPayload {
			applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "events.yaml"), "validation.requested:\n  key: candidate\n  candidate: text\n", "validation.requested:\n  key: candidate\n  candidate: integer\n")
		} else {
			applyClosedReplacement(t, filepath.Join(root, "flows", "validator", "entities.yaml"), "    type: uuid\n", "    type: integer\n")
		}
	case CreateResolutionNumberSourceToIntegerReceiver:
		if opts.Mint != CreateMintPayload {
			t.Fatal("number-to-integer create fixture requires payload minting")
		}
		applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "events.yaml"), "validation.requested:\n  key: candidate\n  candidate: text\n", "validation.requested:\n  key: candidate\n  candidate: number\n")
		applyClosedReplacement(t, filepath.Join(root, "flows", "validator", "entities.yaml"), "    type: uuid\n", "    type: integer\n")
	default:
		t.Fatalf("unsupported create resolution invalidity %d", opts.Invalidity)
	}
	return root
}
