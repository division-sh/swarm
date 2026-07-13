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
	SelectResolutionUndeclaredCarry
	SelectResolutionCompositeKey
	SelectResolutionCarryTypeMismatch
	SelectResolutionLegacyAddress
	SelectResolutionLegacyUsingInstance
	SelectResolutionLegacyConnectMap
	SelectResolutionStaticReceiver
	SelectResolutionManyDelivery
	SelectResolutionExtraAggregation
	SelectResolutionEntityTypeMismatch
)

type TemplateSelectResolutionOptions struct {
	Mode       SelectResolutionMode
	Invalidity SelectResolutionInvalidity
	Missing    LegacyInstancePolicy
	Conflict   LegacyInstancePolicy
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

func applyTemplateSelectOrCreateAccumulation(t testing.TB, root, terminalState string) {
	t.Helper()
	producerEvents := filepath.Join(root, "flows", "producer", "events.yaml")
	for _, event := range []string{"account.requested", "account.ready"} {
		applyClosedReplacement(t, producerEvents,
			event+":\n  account_id: text\n",
			event+":\n  account_id: text\n  score: text\n  decision: text\n")
	}
	applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "nodes.yaml"),
		"          account_id: payload.account_id\n",
		"          account_id: payload.account_id\n          score: payload.score\n          decision: payload.decision\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "account", "events.yaml"),
		"  account_id: text\n",
		"  account_id: text\n  score: text\n  decision: text\n")
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
	missing := legacyInstancePolicy(t, opts.Missing)
	conflict := legacyInstancePolicy(t, opts.Conflict)
	if missing != "" || conflict != "" {
		policy := "instance:\n  by: account_id\n"
		if missing != "" {
			policy += "  on_missing: " + missing + "\n"
		}
		if conflict != "" {
			policy += "  on_conflict: " + conflict + "\n"
		}
		applyClosedReplacement(t, accountSchema, "instance:\n  by: account_id\n", policy)
	}
	applyClosedReplacement(t, packageFile, "  - from: producer.account_setup\n    to: account.account_setup\n", "")
	// The historical lowering matrix deliberately exercises the accepted
	// string/text alias while keeping the checked example's entity type.
	selectedPin := "      - name: account_ready\n        event: account.ready\n        resolution:\n          mode: " + mode + "\n          instance_key: account_id\n        carries:\n          account_id:\n            from: payload.account_id\n            type: string\n"
	applyClosedReplacement(t, accountSchema,
		"      - name: account_ready\n        event: account.ready\n        resolution:\n          mode: select\n          instance_key: account_id\n        carries:\n          account_id:\n            from: payload.account_id\n            type: text\n",
		selectedPin)

	switch opts.Invalidity {
	case SelectResolutionValid:
	case SelectResolutionUndeclaredCarry:
		applyClosedReplacement(t, accountSchema, selectedPin,
			"      - name: account_ready\n        event: account.ready\n        resolution:\n          mode: "+mode+"\n          instance_key: missing_account_id\n        carries:\n          account_id:\n            from: payload.account_id\n            type: string\n")
	case SelectResolutionCompositeKey:
		applyClosedReplacement(t, accountSchema, "  by: account_id\n", "  by: [account_id, region]\n")
		applyClosedReplacement(t, filepath.Join(root, "flows", "account", "entities.yaml"),
			"    _unused_reason: receiver instance identity\n",
			"    _unused_reason: receiver instance identity\n  region:\n    type: text\n    _unused_reason: composition select composite-key test field\n")
	case SelectResolutionCarryTypeMismatch:
		applyClosedReplacement(t, accountSchema, selectedPin,
			"      - name: account_ready\n        event: account.ready\n        resolution:\n          mode: "+mode+"\n          instance_key: account_id\n        carries:\n          account_id:\n            from: payload.account_id\n            type: integer\n")
	case SelectResolutionLegacyAddress:
		applyClosedReplacement(t, accountSchema, selectedPin,
			"      - name: account_ready\n        event: account.ready\n        address:\n          by: account_id\n          source: payload.account_id\n          target: entity.account_id\n          cardinality: one\n        resolution:\n          mode: "+mode+"\n          instance_key: account_id\n        carries:\n          account_id:\n            from: payload.account_id\n            type: string\n")
	case SelectResolutionLegacyUsingInstance:
		applyClosedReplacement(t, packageFile, "    to: account.account_ready\n", "    to: account.account_ready\n    using:\n      instance:\n        source: account_id\n        target: account_id\n")
	case SelectResolutionLegacyConnectMap:
		applyClosedReplacement(t, packageFile, "    to: account.account_ready\n", "    to: account.account_ready\n    map:\n      account_id:\n        source: payload.account_id\n        target: entity.account_id\n")
	case SelectResolutionStaticReceiver:
		applyClosedReplacement(t, packageFile, "    mode: template\nconnect:\n", "    mode: static\nconnect:\n")
		applyClosedReplacement(t, accountSchema, "mode: template\n", "mode: static\n")
	case SelectResolutionManyDelivery:
		applyClosedReplacement(t, packageFile, "    to: account.account_ready\n", "    to: account.account_ready\n    delivery: many\n")
	case SelectResolutionExtraAggregation:
		applyClosedReplacement(t, accountSchema, selectedPin,
			"      - name: account_ready\n        event: account.ready\n        resolution:\n          mode: "+mode+"\n          instance_key: account_id\n          aggregation: stream\n        carries:\n          account_id:\n            from: payload.account_id\n            type: string\n")
	case SelectResolutionEntityTypeMismatch:
		applyClosedReplacement(t, filepath.Join(root, "flows", "account", "entities.yaml"), "    type: text\n", "    type: integer\n")
	default:
		t.Fatalf("unsupported select resolution invalidity %d", opts.Invalidity)
	}
	return root
}

type CreateMint uint8

const (
	CreateMintUUID CreateMint = iota + 1
	CreateMintEventID
)

type CreateResolutionInvalidity uint8

const (
	CreateResolutionValid CreateResolutionInvalidity = iota
	CreateResolutionNonRunnableMode
	CreateResolutionInvalidMint
	CreateResolutionMissingCarry
	CreateResolutionLegacyUsingInstance
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
		applyClosedReplacement(t, validatorSchema, "            mint: uuid\n", "            mint: event_id\n")
	default:
		t.Fatalf("unsupported create mint %d", opts.Mint)
	}
	switch opts.Invalidity {
	case CreateResolutionValid:
	case CreateResolutionNonRunnableMode:
		applyClosedReplacement(t, validatorSchema, "          mode: create\n", "          mode: fan-out\n")
	case CreateResolutionInvalidMint:
		applyClosedReplacement(t, validatorSchema, "            mint: uuid\n", "            mint: random\n")
	case CreateResolutionMissingCarry:
		applyClosedReplacement(t, validatorSchema, "        carries:\n          validation_case_id:\n            from: instance.key.validation_case_id\n            type: uuid\n", "")
	case CreateResolutionLegacyUsingInstance:
		applyClosedReplacement(t, filepath.Join(root, "package.yaml"), "    to: validator.validation_requested\n", "    to: validator.validation_requested\n    using:\n      instance:\n        source: validation_case_id\n        target: validation_case_id\n")
	default:
		t.Fatalf("unsupported create resolution invalidity %d", opts.Invalidity)
	}
	return root
}

type ParentConnectAddressVariant uint8

const (
	ParentConnectAddressLowering ParentConnectAddressVariant = iota + 1
	ParentConnectAddressSemanticView
)

func CopyParentConnectAddressVariant(t testing.TB, variant ParentConnectAddressVariant) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	packageFile := filepath.Join(root, "package.yaml")
	producerSchema := filepath.Join(root, "flows", "producer", "schema.yaml")
	consumerSchema := filepath.Join(root, "flows", "consumer", "schema.yaml")
	switch variant {
	case ParentConnectAddressLowering:
		applyClosedReplacement(t, packageFile, "    to: consumer.work_ready\n", "    to: consumer.work_ready\n    adapter: work_ready_projection\n    map:\n      work_id:\n        source: payload.work_id\n        target: entity.work_id\n")
	case ParentConnectAddressSemanticView:
		applyClosedReplacement(t, packageFile, "    to: consumer.work_ready\n", "    to: consumer.work_ready\n    map:\n      work_id:\n        source: payload.work_id\n        target: entity.work_id\n")
		applyClosedReplacement(t, producerSchema, "        event: work.ready\n", "        event: work.ready\n        key: work_id\n        carries: [work_id]\n")
	default:
		t.Fatalf("unsupported parent-connect address variant %d", variant)
	}
	applyClosedReplacement(t, consumerSchema, "        event: work.ready\n", "        event: work.ready\n        address:\n          by: work_id\n          source: payload.work_id\n          target: entity.work_id\n          cardinality: one\n")
	SetOverlayFile(t, root, "flows/consumer/entities.yaml", "work:\n  work_id:\n    type: text\n")
	return root
}

func CopyLegacyAddressedTemplateSelect(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, TemplateSelectExisting)
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"), "  - from: producer.account_setup\n    to: account.account_setup\n", "")
	applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "schema.yaml"),
		"      - name: account_ready\n        event: account.ready\n",
		"      - name: account_ready\n        event: account.ready\n        key: account_id\n        carries: [account_id, customer_id]\n")
	for _, eventsFile := range []string{"flows/producer/events.yaml", "flows/account/events.yaml"} {
		applyClosedReplacement(t, filepath.Join(root, eventsFile), "account.ready:\n  account_id: text\n", "account.ready:\n  account_id: text\n  customer_id: text\n")
	}
	applyClosedReplacement(t, filepath.Join(root, "flows", "account", "schema.yaml"),
		"      - name: account_ready\n        event: account.ready\n        resolution:\n          mode: select\n          instance_key: account_id\n        carries:\n          account_id:\n            from: payload.account_id\n            type: text\n",
		"      - name: account_ready\n        event: account.ready\n        address:\n          by: customer_id\n          source: payload.customer_id\n          target: entity.customer_id\n          cardinality: one\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "account", "entities.yaml"),
		"    _unused_reason: receiver instance identity\n",
		"    _unused_reason: receiver instance identity\n  customer_id:\n    type: text\n    indexed: true\n")
	return root
}
