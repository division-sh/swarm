package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestForkReceiverFaultSnapshotConsumerPreservesTypedCellsAndExactKeys(t *testing.T) {
	for _, key := range []any{"claim-id", []byte("claim-id")} {
		columns := []string{"delivery_id", "revision", "nullable", "payload"}
		row, err := encodeSelectedForkSnapshotValues(columns, []any{key, int64(9007199254740993), nil, []byte("payload")})
		if err != nil {
			t.Fatal(err)
		}
		cells, err := decodeForkReceiverFaultRow(row, columns)
		if err != nil {
			t.Fatal(err)
		}
		if matched, err := forkReceiverFaultKeyMatches(cells[0], "claim-id"); err != nil || !matched {
			t.Fatalf("exact typed key lost: %t,%v", matched, err)
		}
		if matched, err := forkReceiverFaultKeyMatches(cells[0], "foreign"); err != nil || matched {
			t.Fatalf("foreign typed key borrowed: %t,%v", matched, err)
		}
		if encoded, err := json.Marshal(cells); err != nil || string(encoded) != row {
			t.Fatalf("physical cell/type/null/large-integer evidence changed: %s,%v", encoded, err)
		}
		if got, err := decodeForkReceiverFaultRow(row, []string{"foreign", "revision", "nullable", "payload"}); err == nil || got != nil {
			t.Fatalf("foreign column identity accepted: %+v,%v", got, err)
		}
	}
	for _, raw := range []string{`["claim-id"]`, `[{"Column":"delivery_id","Value":"claim-id"}]`, `[{"Column":"delivery_id","Type":"string"}]`, `[]`, `{`} {
		if cells, err := decodeForkReceiverFaultRow(raw, []string{"delivery_id"}); err == nil || cells != nil {
			t.Fatalf("incomplete typed witness accepted: %+v,%v", cells, err)
		}
	}
	for _, cell := range []forkReceiverFaultCell{
		{Column: "delivery_id", Type: "int64", Value: json.RawMessage(`1`)},
		{Column: "delivery_id", Type: "[]uint8", Value: json.RawMessage(`"%%%"`)},
		{Column: "delivery_id", Type: "string", Value: json.RawMessage(`null`)},
	} {
		if matched, err := forkReceiverFaultKeyMatches(cell, "claim-id"); err == nil || matched {
			t.Fatalf("invalid typed key accepted: %+v,%t,%v", cell, matched, err)
		}
	}
	if reflect.DeepEqual(forkReceiverFaultCell{Column: "one", Type: "string", Value: json.RawMessage(`"YQ=="`)}, forkReceiverFaultCell{Column: "one", Type: "[]uint8", Value: json.RawMessage(`"YQ=="`)}) {
		t.Fatal("fault equality erased driver types")
	}
}
