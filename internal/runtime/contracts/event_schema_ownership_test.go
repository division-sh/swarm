package contracts

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
)

func TestIntraPackageEventConsumerProjectionCensus(t *testing.T) {
	type manifestation struct {
		bundle       string
		from         string
		to           string
		event        string
		derivedField string
	}
	manifestations := []manifestation{
		{bundle: "fan-in/barrier", from: "ingress", to: "operating", event: "operating.report.requested", derivedField: "operating_id"},
		{bundle: "fan-in/barrier", from: "operating", to: "portfolio", event: "operating.reported"},
		{bundle: "fan-in/stream", from: "ingress", to: "operating", event: "operating.report.requested", derivedField: "operating_id"},
		{bundle: "fan-in/stream", from: "operating", to: "portfolio", event: "operating.reported"},
		{bundle: "template-create-minted-key", from: "producer", to: "validator", event: "validation.requested", derivedField: "validation_case_id"},
		{bundle: "template-create-minted-key", from: "validator", to: "producer", event: "validation.started"},
		{bundle: "notify-all-children", from: "portfolio", to: "account", event: "account.registered"},
		{bundle: "notify-all-children", from: "portfolio", to: "account", event: "account.notify.requested"},
		{bundle: "parent-connect", from: "producer", to: "consumer", event: "work.ready"},
		{bundle: "template-select-existing", from: "producer", to: "account", event: "account.setup"},
		{bundle: "template-select-existing", from: "producer", to: "account", event: "account.ready"},
		{bundle: "template-reply", from: "initiator", to: "requester", event: "requester.setup"},
		{bundle: "template-reply", from: "initiator", to: "requester", event: "requester.requested"},
		{bundle: "template-reply", from: "requester", to: "provider", event: "provider.requested"},
		{bundle: "template-reply", from: "provider", to: "requester", event: "provider.replied"},
		{bundle: "template-select-or-create", from: "producer", to: "account", event: "account.ready"},
	}
	if len(manifestations) != 16 {
		t.Fatalf("manifestation census = %d, want 16", len(manifestations))
	}

	repo := repoRootForContractsTest(t)
	exact, divergent := 0, 0
	for _, item := range manifestations {
		item := item
		t.Run(item.bundle+"/"+item.event, func(t *testing.T) {
			root := filepath.Join(repo, "examples", "routing", filepath.FromSlash(item.bundle))
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatalf("load %s: %v", item.bundle, err)
			}
			producer, _, ok := EventSchemaForFlowEvent(bundle, item.from, item.event)
			if !ok {
				t.Fatalf("producer %s schema %s missing", item.from, item.event)
			}
			consumer, _, ok := EventSchemaForFlowEvent(bundle, item.to, item.event)
			if !ok {
				t.Fatalf("consumer %s projection %s missing", item.to, item.event)
			}
			producerBytes := canonicalEventSchemaBytes(t, producer)
			consumerBytes := canonicalEventSchemaBytes(t, consumer)
			if item.derivedField == "" {
				exact++
				if !bytes.Equal(producerBytes, consumerBytes) {
					t.Fatalf("exact consumer projection differs from producer: producer=%s consumer=%s", producerBytes, consumerBytes)
				}
				return
			}
			divergent++
			if bytes.Equal(producerBytes, consumerBytes) {
				t.Fatalf("derived delivery field %s did not produce the named plan delta", item.derivedField)
			}
			properties, _ := consumer.Schema["properties"].(map[string]any)
			field, _ := properties[item.derivedField].(map[string]any)
			if field["type"] != "string" || field["format"] != "uuid" {
				t.Fatalf("derived field %s = %#v, want intrinsic uuid schema", item.derivedField, field)
			}
			if !stringSliceContains(eventSchemaRequired(consumer.Schema), item.derivedField) {
				t.Fatalf("derived field %s is not required in consumer projection %#v", item.derivedField, consumer.Schema)
			}
		})
	}
	if exact != 13 || divergent != 3 {
		t.Fatalf("census classes exact=%d divergent=%d, want 13/3", exact, divergent)
	}
}

func TestDeclaredLocalEventReferenceFastPathMatchesCanonicalScopeResolution(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := filepath.Join(repo, "examples", "routing", "template-create-minted-key")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range []struct {
		name      string
		flowID    string
		eventType string
	}{
		{name: "authored output", flowID: "producer", eventType: "validation.requested"},
		{name: "connected receiver", flowID: "producer", eventType: "validation.started"},
	} {
		t.Run(item.name, func(t *testing.T) {
			got, ok := bundle.resolveDeclaredLocalFlowEventReference(item.flowID, item.eventType)
			if !ok {
				t.Fatalf("fast path did not recognize %s.%s", item.flowID, item.eventType)
			}
			scope := bundle.flowEventScope(item.flowID)
			want := scope.ResolveEvent(item.eventType, bundle.flowEventDescendants(item.flowID))
			if got != want {
				t.Fatalf("fast path = %q, canonical scope = %q", got, want)
			}
		})
	}
}

func TestEffectiveEventResolutionPrefersConnectedProducerOverUnrelatedSameNameRoot(t *testing.T) {
	repo := repoRootForContractsTest(t)
	source := filepath.Join(repo, "examples", "routing", "template-create-minted-key")
	root := filepath.Join(t.TempDir(), "template-create-minted-key")
	copyTree(t, source, root)
	if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte("validation.requested:\n  wrong: text\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	producerEvents := filepath.Join(root, "flows", "producer", "events.yaml")
	if err := os.WriteFile(producerEvents, []byte(`validation.triggered:
  candidate: text?
validation.requested:
  swarm:
    note: Producer-owned request schema
  candidate:
    type: ProducerCandidate?
    description: Candidate under validation
    pattern: ^[a-z]+$
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "flows", "producer", "types.yaml"), []byte("scalars:\n  ProducerCandidate: text\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}

	entry, key, ok := bundle.ResolveFlowEventCatalogEntry("validator", "validation.requested")
	if !ok || key != "validation.requested" {
		t.Fatalf("effective resolution = key:%q ok:%t", key, ok)
	}
	for _, field := range []string{"candidate", "validation_case_id"} {
		if _, found := entry.Payload.Properties[field]; !found {
			t.Fatalf("effective producer projection omitted %s: %#v", field, entry.Payload.Properties)
		}
	}
	if _, found := entry.Payload.Properties["wrong"]; found {
		t.Fatalf("unrelated root schema won effective ownership: %#v", entry.Payload.Properties)
	}
	if !slices.Contains(entry.Payload.Required, "validation_case_id") {
		t.Fatalf("receiver carry missing from required fields: %v", entry.Payload.Required)
	}
	schema, _, ok := EventSchemaForFlowEvent(bundle, "validator", "validation.requested")
	properties, _ := schema.Schema["properties"].(map[string]any)
	candidate, _ := properties["candidate"].(map[string]any)
	if !ok || candidate["type"] != "string" {
		t.Fatalf("receiver schema did not consume producer type catalog: schema=%#v ok=%t", schema.Schema, ok)
	}
	projectionPrefix := effectiveEventProjectionProvenancePrefix(".", "validator", "validation.requested")
	for _, suffix := range []string{"metadata.swarm.note", "fields.candidate.description", "fields.candidate.refinements.pattern"} {
		provenance, found := bundle.EffectiveProvenance().Lookup(projectionPrefix + "." + suffix)
		if !found || provenance.Origin != EffectiveValueOriginDerived || provenance.RuleID != eventConsumerProjectionRule || len(provenance.InputPaths) != 1 || !strings.Contains(provenance.InputPaths[0], suffix) {
			t.Fatalf("complete projection provenance %s = %#v, found=%t", suffix, provenance, found)
		}
	}

	node := identitytest.ExecutableNode(t, ".", "validator", "validator-node")
	nodeEntry, _, ok := bundle.ResolveExecutableNodeEventCatalogEntry(node, "validation.requested")
	if !ok || nodeEntry.Payload.Properties["validation_case_id"].Type != "uuid" {
		t.Fatalf("executable-node proof missed effective carry: entry=%#v ok=%t", nodeEntry, ok)
	}
	candidateType, ok := ResolveExecutableNodeEventFieldType(bundle, node, "validation.requested", "candidate")
	if !ok {
		t.Fatal("executable-node field typing omitted producer-owned candidate")
	}
	resolvedCandidate, err := candidateType.Resolve()
	if err != nil || resolvedCandidate.Kind != CatalogTypeText {
		t.Fatalf("executable-node field typing lost producer catalog: resolved=%#v err=%v", resolvedCandidate, err)
	}

	lowered, err := bundle.LowerEmitSpecFields(EmitFieldLoweringContext{
		Node:             node,
		TriggerEventType: "validation.requested",
		Site:             "validator-node.validation.requested.emit",
	}, EmitSpec{
		Event: "validation.started",
		Fields: map[string]ExpressionValue{
			"candidate":          CELExpression("payload"),
			"validation_case_id": CELExpression("payload"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := lowered.Fields["validation_case_id"]; !found {
		t.Fatalf("emit lowering omitted effective receiver carry: %#v", lowered.Fields)
	}
}

func TestEffectiveEventResolutionPreservesConnectedRenameOwnership(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := filepath.Join(repo, "tests", "tier11-flow-composition", "test-dynamic-flow-instance")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	entry, key, ok := bundle.ResolveFlowEventCatalogEntry("worker", "work.assign")
	if !ok || key != "worker/work.assign" {
		t.Fatalf("renamed effective resolution = key:%q ok:%t entry:%#v", key, ok, entry)
	}
	for _, field := range []string{"worker_id", "task_label"} {
		if entry.Payload.Properties[field].Type != "string" {
			t.Fatalf("renamed producer field %s = %#v", field, entry.Payload.Properties[field])
		}
	}
	provenance, ok := bundle.EffectiveProvenance().Lookup(effectiveEventProjectionProvenancePrefix(".", "worker", "work.assign") + ".declaration")
	if !ok || provenance.Origin != EffectiveValueOriginDerived || provenance.RuleID != eventConsumerProjectionRule {
		t.Fatalf("renamed projection provenance = %#v, found=%t", provenance, ok)
	}
}

func TestIntraPackageEventConsumerRestatementFailsClosed(t *testing.T) {
	repo := repoRootForContractsTest(t)
	source := filepath.Join(repo, "examples", "routing", "parent-connect")
	root := filepath.Join(t.TempDir(), "parent-connect")
	copyTree(t, source, root)
	consumerEvents := filepath.Join(root, "flows", "consumer", "events.yaml")
	if err := os.WriteFile(consumerEvents, []byte("work.ready:\n  work_id: text?\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err == nil || !contractErrorContains(err, "restates producer-owned schema") || !contractErrorContains(err, "remove the consumer declaration") {
		t.Fatalf("load error = %v, want intra-package restatement teaching error", err)
	}
}

func TestIntraPackageEventConsumerProjectionProvenance(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := filepath.Join(repo, "examples", "routing", "template-create-minted-key")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	prefix := effectiveEventProjectionProvenancePrefix(".", "validator", "validation.requested")
	declaration, ok := bundle.EffectiveProvenance().Lookup(prefix + ".declaration")
	if !ok || declaration.Origin != EffectiveValueOriginDerived || declaration.RuleID != eventConsumerProjectionRule || len(declaration.InputPaths) != 1 {
		t.Fatalf("projection declaration provenance = %#v, found=%t", declaration, ok)
	}
	generated, ok := bundle.EffectiveProvenance().Lookup(prefix + ".fields.validation_case_id.type")
	if !ok || generated.Origin != EffectiveValueOriginDerived || generated.RuleID != eventDeliveryCarryRule || len(generated.InputPaths) != 1 || !strings.Contains(generated.InputPaths[0], "carries") {
		t.Fatalf("generated carry provenance = %#v, found=%t", generated, ok)
	}
}

func TestIntraPackageEventConsumerProjectionCarriesAllOwnerAndAliasProvenance(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := filepath.Join(repo, "examples", "routing", "template-select-existing")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	prefix := effectiveEventProjectionProvenancePrefix(".", "account", "account.setup")
	for _, suffix := range []string{"declaration", "payload.required", "fields.account_id.type", "fields.account_id.is_optional"} {
		provenance, ok := bundle.EffectiveProvenance().Lookup(prefix + "." + suffix)
		if !ok || provenance.Origin != EffectiveValueOriginDerived {
			t.Fatalf("projection provenance %s = %#v, found=%t", suffix, provenance, ok)
		}
	}
	alias, ok := bundle.EffectiveProvenance().Lookup(prefix + ".fields.account_id.type")
	if !ok || alias.RuleID != eventDeliveryCarryRule || len(alias.InputPaths) != 2 || !strings.Contains(strings.Join(alias.InputPaths, " "), "fields.account_id.type") || !strings.Contains(strings.Join(alias.InputPaths, " "), "carries") {
		t.Fatalf("payload alias provenance = %#v, found=%t", alias, ok)
	}
}

func TestCrossPackageBoundarySnapshotIsNotClassifiedAsConsumerRestatement(t *testing.T) {
	bundle := &WorkflowContractBundle{
		projectContracts: map[string]ProjectContractView{
			".":       {Events: map[string]EventCatalogEntry{"work.ready": {}}},
			"package": {Events: map[string]EventCatalogEntry{"work.ready": {}}},
		},
		Semantics: WorkflowSemanticView{CompositionConnects: []FlowPackageConnect{{
			PackageKey: ".", Event: "work.ready", From: ".", To: "worker",
		}}},
		FlowTree: FlowTree{ByID: map[string]*FlowContractView{
			"worker": {Paths: FlowContractPaths{ID: "worker", PackageKey: "package"}, Events: map[string]EventCatalogEntry{"work.ready": {}}},
		}},
	}
	if errs := validateIntraPackageEventSchemaOwnership(bundle); len(errs) != 0 {
		t.Fatalf("cross-package snapshot was rejected as a same-package restatement: %v", errs)
	}
}

func TestCrossPackageBoundarySnapshotProvenanceCarriesPackageIdentity(t *testing.T) {
	snapshot, err := admitEventCatalogEntryForTest(t, "work_id: uuid")
	if err != nil {
		t.Fatal(err)
	}
	internal, err := admitEventCatalogEntryForTest(t, "detail: text")
	if err != nil {
		t.Fatal(err)
	}
	bundle := &WorkflowContractBundle{projectContracts: map[string]ProjectContractView{
		"packages/worker": {
			Paths: ProjectPackagePaths{Key: "packages/worker", ProjectEventsFile: "packages/worker/events.yaml"},
			Manifest: ProjectPackageDocument{
				Name: "worker", Version: "1.0.0",
				Requires: FlowPackageRequires{Inputs: []string{"work.ready"}, Outputs: []string{"work.completed"}},
			},
			Events: map[string]EventCatalogEntry{
				"work.ready":      snapshot,
				"work.completed":  internal,
				"worker.internal": internal,
			},
		},
	}}
	populateEffectiveEventProvenance(bundle)

	prefix := effectiveEventProvenancePrefix("packages/worker", "work.ready")
	for _, suffix := range []string{"declaration", "fields.work_id.type", "fields.work_id.is_optional", "payload.required"} {
		provenance, ok := bundle.EffectiveProvenance().Lookup(prefix + "." + suffix)
		if !ok || provenance.Origin != EffectiveValueOriginBoundarySnapshot || provenance.PackIdentity != "packages/worker" {
			t.Fatalf("boundary provenance %s = %#v, found=%t", suffix, provenance, ok)
		}
	}
	internalPrefix := effectiveEventProvenancePrefix("packages/worker", "worker.internal")
	provenance, ok := bundle.EffectiveProvenance().Lookup(internalPrefix + ".declaration")
	if !ok || provenance.Origin != EffectiveValueOriginAuthored || provenance.PackIdentity != "" {
		t.Fatalf("package-local event misclassified as boundary snapshot: %#v, found=%t", provenance, ok)
	}
	outputPrefix := effectiveEventProvenancePrefix("packages/worker", "work.completed")
	provenance, ok = bundle.EffectiveProvenance().Lookup(outputPrefix + ".declaration")
	if !ok || provenance.Origin != EffectiveValueOriginAuthored || provenance.PackIdentity != "" {
		t.Fatalf("producer-owned package output misclassified as boundary snapshot: %#v, found=%t", provenance, ok)
	}
}

func canonicalEventSchemaBytes(t testing.TB, schema EventSchema) []byte {
	t.Helper()
	raw, err := canonicaljson.Bytes(runtimeeventschema.CanonicalAcceptanceSchema(schema.Schema))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func eventSchemaRequired(schema map[string]any) []string {
	switch values := schema["required"].(type) {
	case []string:
		return values
	case []any:
		out := make([]string, 0, len(values))
		for _, value := range values {
			if text, ok := value.(string); ok {
				out = append(out, strings.TrimSpace(text))
			}
		}
		return out
	default:
		return nil
	}
}
