package durabledata

import (
	"reflect"
	"testing"
)

func TestResourceReceiptCapabilitiesExposeOnlyOpaqueCapture(t *testing.T) {
	opaque := reflect.TypeFor[*CapturedResourceReceipt]()
	if opaque.NumMethod() != 0 {
		t.Fatal("opaque receipt exports executable or transaction methods")
	}
	for _, typ := range []reflect.Type{
		reflect.TypeFor[*ReceiptCorruption](),
		reflect.TypeFor[*receiptAtomicCorruption](),
		reflect.TypeFor[*ImportProvenanceRemoval](),
		reflect.TypeFor[*ProvenanceReadArmAddition](),
		reflect.TypeFor[*NonMutatingHeadHistoryAddition](),
		reflect.TypeFor[*FailedFusedContextCorruption](),
		reflect.TypeFor[*SourceContextCorruption](),
		reflect.TypeFor[*SourceExactInputCorruption](),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			if typ.NumMethod() != 1 {
				t.Fatalf("receipt exposes %d methods, want only opaque capture", typ.NumMethod())
			}
			method := typ.Method(0)
			if method.Name != "CapturedReceipt" || method.Type.NumIn() != 1 || method.Type.NumOut() != 1 || method.Type.Out(0) != opaque {
				t.Fatalf("receipt exposed an open capability: %v", method)
			}
		})
	}
	for i := range opaque.Elem().NumField() {
		if field := opaque.Elem().Field(i); field.IsExported() {
			t.Fatalf("opaque receipt exports a carrier: %v", field)
		}
	}
}
