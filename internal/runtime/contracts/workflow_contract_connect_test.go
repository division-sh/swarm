package contracts

import (
	"reflect"
	"testing"
)

func TestW2CanonicalPinAndPermissionEvidenceIgnoresSetAuthorOrderAndIsImmutable(t *testing.T) {
	context := FlowPinCompilationContext{FlowID: "collector", FlowPath: "collector", SourceFile: "flows/collector/schema.yaml"}
	firstDedup := []string{"payload.worker_id", "event.id"}
	first, err := CompileFlowInputPin(context, FlowInputEventPin{
		Event: "work.reported",
		Resolution: FlowInputPinResolution{
			Mode: FlowInputResolutionModeFanIn, Aggregation: "barrier", Window: "payload.batch_id",
			DedupBy: firstDedup, Singleton: "collector",
		},
	})
	if err != nil {
		t.Fatalf("compile first input pin: %v", err)
	}
	second, err := CompileFlowInputPin(context, FlowInputEventPin{
		Event: "work.reported",
		Resolution: FlowInputPinResolution{
			Mode: FlowInputResolutionModeFanIn, Aggregation: "barrier", Window: "payload.batch_id",
			DedupBy: []string{"event.id", "payload.worker_id"}, Singleton: "collector",
		},
	})
	if err != nil {
		t.Fatalf("compile second input pin: %v", err)
	}
	if first.Digest() == "" || first.Digest() != second.Digest() {
		t.Fatalf("equivalent input pin digests = %q/%q, want one canonical identity", first.Digest(), second.Digest())
	}

	firstDedup[0] = "payload.changed"
	readback := first.Resolution()
	if got, want := readback.DedupBy, []string{"event.id", "payload.worker_id"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("compiled dedup evidence = %#v, want %#v", got, want)
	}
	readback.DedupBy[0] = "payload.changed_again"
	if got := first.Resolution().DedupBy; !reflect.DeepEqual(got, []string{"event.id", "payload.worker_id"}) {
		t.Fatalf("resolution readback mutation escaped into compiled owner: %#v", got)
	}

	firstFields := []string{"entity.updated_at", "entity.status"}
	permissionsA, err := CompileFlowEntityPermissions(firstFields)
	if err != nil {
		t.Fatalf("compile first permissions: %v", err)
	}
	permissionsB, err := CompileFlowEntityPermissions([]string{"entity.status", "entity.updated_at"})
	if err != nil {
		t.Fatalf("compile second permissions: %v", err)
	}
	if !reflect.DeepEqual(permissionsA.Fields(), permissionsB.Fields()) {
		t.Fatalf("equivalent permission sets differ: %#v/%#v", permissionsA.Fields(), permissionsB.Fields())
	}
	firstFields[0] = "entity.changed"
	fields := permissionsA.Fields()
	fields[0] = "entity.changed_again"
	if got := permissionsA.Fields(); !reflect.DeepEqual(got, []string{"entity.status", "entity.updated_at"}) {
		t.Fatalf("permission readback mutation escaped into compiled owner: %#v", got)
	}
}
