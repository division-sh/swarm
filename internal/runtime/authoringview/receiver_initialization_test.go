package authoringview

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestReceiverInitializationAdmissionProjectionParity(t *testing.T) {
	for _, fixture := range []struct {
		name  string
		root  func(testing.TB) string
		flows map[string]map[string]string
	}{
		{"nested", canonicalrouting.CopyReceiverInitializationGeometry, map[string]map[string]string{
			"worker":      {"label": "payload.label", "count": "payload.count"},
			"worker/leaf": {"label": "payload.label", "count": "payload.count"},
		}},
		{"public", canonicalrouting.CopyServedReceiverInitialization, map[string]map[string]string{
			"account": {"label": "payload.values.label", "count": "payload.values.count", "ratio": "payload.values.ratio", "active": "payload.values.active", "attributes": "payload.values.attributes"},
		}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, fixture.root(t), contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			report := bootverify.Run(context.Background(), source, bootverify.Options{})
			if report.HasErrors() {
				t.Fatalf("admitted creation failed verify: %+v", report.Errors())
			}
			view := mustBuild(t, source, &report)
			for flowID, bindings := range fixture.flows {
				flow := flowByID(t, view, flowID)
				pins := source.FlowInputEventPins(flowID)
				if len(flow.InputPins) != 1 || len(pins) != 1 {
					t.Fatalf("%s pins: view=%+v compiled=%+v", flowID, flow.InputPins, pins)
				}
				pin := flow.InputPins[0]
				producer, ok := pins[0].ProducerEventSchema()
				if !ok {
					t.Fatal("creating input has no admitted producer schema")
				}
				receiver, ok := pins[0].ReceiverEventSchema()
				if !ok || !reflect.DeepEqual(pin.Initialize, bindings) || pin.PinDigest != pins[0].Digest() || pin.ProducerSchemaDigest != producer.AcceptanceSchemaDigest() || pin.ReceiverSchemaDigest != receiver.AcceptanceSchemaDigest() {
					t.Fatalf("%s authoring view differs from compiled initialization: %+v", flowID, pin)
				}
				pin.Initialize["label"] = "payload.borrowed"
				if !reflect.DeepEqual(pins[0].Initialization().Bindings(), bindings) {
					t.Fatal("authoring readback mutated canonical initialization")
				}
			}
		})
	}
}
