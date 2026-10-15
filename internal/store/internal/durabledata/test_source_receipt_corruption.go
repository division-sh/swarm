package durabledata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

func CaptureSourceCandidateCorruptionTx(ctx context.Context, tx *sql.Tx, id string, candidate runtimedata.CompiledVersion) (*ReceiptCorruption, error) {
	if uuid.Validate(id) != nil {
		return nil, errors.New("candidate corruption requires its source receipt identity")
	}
	if err := candidate.Manifest.Validate(); err != nil {
		return nil, err
	}
	if err := candidate.VersionID.Validate(); err != nil {
		return nil, err
	}
	return captureReceiptCorruptionTx(ctx, tx, id, "", receiptCorruptionChange{
		owner: sourceReceiptCorruption, column: "result_json", json: func(value map[string]any) {
			stored := value["candidate"].(map[string]any)
			stored["version_id"] = candidate.VersionID
			ref := candidate.Manifest.Declaration
			stored["manifest"] = map[string]any{
				"manifest_format": candidate.Manifest.ManifestFormat, "declaration": map[string]any{"flow_path": ref.FlowPath, "event": ref.EventName},
				"schema_digest": candidate.Manifest.SchemaDigest, "row_codec": candidate.Manifest.RowCodec,
				"content_digest": candidate.Manifest.ContentDigest, "row_count": candidate.Manifest.RowCount,
			}
		},
	})
}

type SourceExactInputCorruption struct {
	receiptAtomicCorruption
	Command runtimedata.SourceCommand
}

func CaptureSourceExactInputCorruptionTx(ctx context.Context, tx *sql.Tx, id string) (*SourceExactInputCorruption, error) {
	if uuid.Validate(id) != nil {
		return nil, errors.New("exact input corruption requires its source receipt identity")
	}
	raw, err := ReadSourceRequestWireTx(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	var command runtimedata.SourceCommand
	if err := json.Unmarshal(raw, &command); err != nil {
		return nil, err
	}
	command.Input = []byte("{\"slug\":\"beta\",\"score\":2}\n")
	hash, raw, err := command.RequestHash()
	if err != nil {
		return nil, err
	}
	f := &SourceExactInputCorruption{Command: command}
	for _, change := range []struct {
		column string
		value  any
	}{{"request_hash", hash}, {"request_json", raw}} {
		field, err := captureReceiptCorruptionTx(ctx, tx, id, "", receiptCorruptionChange{
			owner: sourceReceiptCorruption, column: change.column, scalar: func() any { return change.value },
		})
		if err != nil {
			return nil, err
		}
		f.fields = append(f.fields, field)
	}
	return f, nil
}
