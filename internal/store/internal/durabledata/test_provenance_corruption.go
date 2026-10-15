package durabledata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

func CaptureProvenanceCorruptionTx(ctx context.Context, tx *sql.Tx, version, kind string) (*ReceiptCorruption, error) {
	if err := runtimedata.VersionID(version).Validate(); err != nil {
		return nil, err
	}
	raw, err := ReadFirstResourceProvenanceWireTx(ctx, tx, version)
	if err != nil {
		return nil, err
	}
	var p runtimedata.Provenance
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	change := receiptCorruptionChange{owner: firstProvenanceCorruption}
	switch kind {
	case "json/version":
		change.column, change.json = "provenance_json", func(v map[string]any) {
			v["version_id"] = "resource-version-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
		}
	case "json/producer ref":
		change.column, change.json = "provenance_json", func(v map[string]any) {
			v["producer_ref"] = map[string]any{"kind": "normal_run", "run_id": uuid.NewString()}
		}
	case "json/actor":
		change.column, change.json = "provenance_json", func(v map[string]any) { v["actor"] = "hostile" }
	case "json/committed at":
		change.column, change.json = "provenance_json", func(v map[string]any) { v["committed_at"] = p.CommittedAt.Add(time.Minute).Format(time.RFC3339Nano) }
	case "typed/producer kind":
		change.column, change.scalar = "producer_kind", func() any { return "normal_run" }
	case "typed/producer id":
		change.column, change.scalar = "producer_id", func() any { return uuid.NewString() }
	case "typed/actor":
		change.column, change.scalar = "actor", func() any { return "hostile" }
	case "typed/committed at":
		change.column, change.scalar = "committed_at", func() any { return p.CommittedAt.Add(time.Minute) }
	default:
		return nil, fmt.Errorf("unknown resource provenance integrity case %q", kind)
	}
	return captureReceiptCorruptionTx(ctx, tx, version, "", change)
}
