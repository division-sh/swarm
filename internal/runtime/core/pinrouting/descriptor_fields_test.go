package pinrouting

import (
	"reflect"
	"testing"
)

func TestDescriptorAddressFieldsConsumesNormalizedConstructorState(t *testing.T) {
	fields := map[string]any{"key": int64(42), "label": "receiver", "enabled": false, "record": map[string]any{"key": "not a scalar"}, "absent": nil}
	got, err := DescriptorAddressFields(fields)
	want := map[string]string{"entity.key": "42", "entity.label": "receiver", "entity.enabled": "false"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("constructor selection projection=%#v err=%v", got, err)
	}
	got["entity.key"] = "changed"
	if fields["key"] != int64(42) {
		t.Fatal("selection projection mutated constructor state")
	}
}
