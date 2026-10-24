package pinrouting

import (
	"reflect"
	"testing"
)

func TestDescriptorAddressFieldsConsumesNormalizedConstructorState(t *testing.T) {
	fields := map[string]any{"key": int64(42), "label": "receiver", "enabled": false, "record": map[string]any{"key": "not a scalar"}, "absent": nil}
	got, err := DescriptorAddressFields(fields)
	want := map[string]string{"state.key": "42", "state.label": "receiver", "state.enabled": "false"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("constructor selection projection=%#v err=%v", got, err)
	}
	got["state.key"] = "changed"
	if fields["key"] != int64(42) {
		t.Fatal("selection projection mutated constructor state")
	}
}
