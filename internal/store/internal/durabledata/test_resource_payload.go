package durabledata

import (
	"context"
	"database/sql"
	"fmt"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

// CaptureResourcePayloadCorruptionTx provides only the finite payload negatives
// exercised by the resource integrity and public-read journeys.
func CaptureResourcePayloadCorruptionTx(ctx context.Context, tx *sql.Tx, version string, kind string) (*ReceiptCorruption, error) {
	if err := runtimedata.VersionID(version).Validate(); err != nil {
		return nil, err
	}
	var payload []byte
	switch kind {
	case "poison sibling":
		payload = []byte("{\"slug\":\"poison\",\"score\":99}\n")
	case "corrupt sibling":
		payload = []byte("{\"slug\":\"corrupt\",\"score\":99}\n")
	case "not JSONL":
		payload = []byte("not JSONL")
	case "truncated JSON":
		payload = []byte("{\"slug\":")
	default:
		return nil, fmt.Errorf("unrecognized resource payload integrity case %q", kind)
	}
	return captureReceiptCorruptionTx(ctx, tx, version, "", receiptCorruptionChange{
		owner: versionReceiptCorruption, column: "canonical_jsonl", scalar: func() any { return payload },
	})
}
