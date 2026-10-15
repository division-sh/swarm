package durabledata

import (
	"context"
	"fmt"
	"testing"
)

func TestCapturedResourceReceiptRetainsEveryPayloadPointer(t *testing.T) {
	single, atomic := &ReceiptCorruption{}, &receiptAtomicCorruption{}
	removal, provenance, head := &ImportProvenanceRemoval{}, &ProvenanceReadArmAddition{}, &NonMutatingHeadHistoryAddition{}
	fused, source, input := &FailedFusedContextCorruption{}, &SourceContextCorruption{}, &SourceExactInputCorruption{}
	for _, proof := range []struct {
		name string
		got  *CapturedResourceReceipt
		want CapturedResourceReceipt
	}{
		{"single", single.CapturedReceipt(), CapturedResourceReceipt{corruption: single}},
		{"atomic", atomic.CapturedReceipt(), CapturedResourceReceipt{atomic: atomic}},
		{"removal", removal.CapturedReceipt(), CapturedResourceReceipt{removal: removal}},
		{"provenance", provenance.CapturedReceipt(), CapturedResourceReceipt{provenance: provenance}},
		{"head", head.CapturedReceipt(), CapturedResourceReceipt{head: head}},
		{"promoted fused", fused.CapturedReceipt(), CapturedResourceReceipt{atomic: &fused.receiptAtomicCorruption}},
		{"promoted source", source.CapturedReceipt(), CapturedResourceReceipt{atomic: &source.receiptAtomicCorruption}},
		{"promoted input", input.CapturedReceipt(), CapturedResourceReceipt{atomic: &input.receiptAtomicCorruption}},
	} {
		t.Run(proof.name, func(t *testing.T) {
			if proof.got == nil || *proof.got != proof.want || !proof.got.hasExactlyOnePayload() {
				t.Fatal("opaque capture lost its sole original payload pointer")
			}
		})
	}
	single.after = []byte("installed")
	if string(single.CapturedReceipt().corruption.after) != "installed" {
		t.Fatal("capture copied the pre-install state")
	}
	single.CapturedReceipt().corruption.after = []byte("restored")
	if string(single.after) != "restored" {
		t.Fatal("restore changed a copy instead of the original payload")
	}
	atomic.fields = []*ReceiptCorruption{single}
	removal.original.Actor, provenance.row.Actor, head.completed = "removed", "added", "head"
	if atomic.CapturedReceipt().atomic.fields[0] != single || removal.CapturedReceipt().removal.original.Actor != "removed" || provenance.CapturedReceipt().provenance.row.Actor != "added" || head.CapturedReceipt().head.completed != "head" {
		t.Fatal("a concrete variant retained stale captured state")
	}
}

func TestCapturedResourceReceiptRejectsEveryAmbiguousPayload(t *testing.T) {
	for mask := 0; mask < 1<<5; mask++ {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			var token CapturedResourceReceipt
			count := 0
			if mask&1 != 0 {
				token.corruption, count = &ReceiptCorruption{}, count+1
			}
			if mask&2 != 0 {
				token.atomic, count = &receiptAtomicCorruption{}, count+1
			}
			if mask&4 != 0 {
				token.removal, count = &ImportProvenanceRemoval{}, count+1
			}
			if mask&8 != 0 {
				token.provenance, count = &ProvenanceReadArmAddition{}, count+1
			}
			if mask&16 != 0 {
				token.head, count = &NonMutatingHeadHistoryAddition{}, count+1
			}
			if token.hasExactlyOnePayload() != (count == 1) {
				t.Fatal("opaque payload validation used precedence instead of exactly-one")
			}
			if InstallCapturedResourceReceiptTx(context.Background(), nil, &token, false) == nil || RestoreCapturedResourceReceiptTx(context.Background(), nil, &token, false) == nil {
				t.Fatal("token bypassed selected transaction admission")
			}
		})
	}
	var nilToken *CapturedResourceReceipt
	if nilToken.hasExactlyOnePayload() || InstallCapturedResourceReceiptTx(context.Background(), nil, nilToken, false) == nil || RestoreCapturedResourceReceiptTx(context.Background(), nil, nilToken, false) == nil {
		t.Fatal("nil capture admitted")
	}
	for _, token := range []*CapturedResourceReceipt{
		(*ReceiptCorruption)(nil).CapturedReceipt(),
		(*receiptAtomicCorruption)(nil).CapturedReceipt(),
		(*ImportProvenanceRemoval)(nil).CapturedReceipt(),
		(*ProvenanceReadArmAddition)(nil).CapturedReceipt(),
		(*NonMutatingHeadHistoryAddition)(nil).CapturedReceipt(),
	} {
		if token.hasExactlyOnePayload() {
			t.Fatal("typed nil payload became executable authority")
		}
	}
}
