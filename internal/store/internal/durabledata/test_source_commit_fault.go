package durabledata

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/google/uuid"
)

func CaptureSourceHeadCommitFaultTx(ctx context.Context, tx *sql.Tx, id, kind string) (*ReceiptCorruption, error) {
	if uuid.Validate(id) != nil {
		return nil, fmt.Errorf("source head fault requires its exact invocation")
	}
	change := receiptCorruptionChange{owner: headHistoryReceiptCorruption}
	switch kind {
	case "missing head history":
		change.remove = true
	case "wrong head history":
		change.column, change.scalar = "operation_kind", func() any { return "fused_import" }
	default:
		return nil, fmt.Errorf("unknown source head integrity case %q", kind)
	}
	return captureReceiptCorruptionTx(ctx, tx, id, "", change)
}

func CapturePrunePinEvidenceRemovalTx(ctx context.Context, tx *sql.Tx, id string, ordinal int) (*ReceiptCorruption, error) {
	if uuid.Validate(id) != nil || ordinal <= 0 {
		return nil, fmt.Errorf("prune evidence removal requires its exact invocation and positive ordinal")
	}
	return captureReceiptCorruptionTx(ctx, tx, id, strconv.Itoa(ordinal), receiptCorruptionChange{owner: prunePinEvidenceCorruption, remove: true})
}
