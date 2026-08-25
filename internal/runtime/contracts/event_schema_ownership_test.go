package contracts

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
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
