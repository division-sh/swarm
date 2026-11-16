package delivery

import (
	"context"
	"database/sql"
	"time"
)

func (s *DeliveryPostgresOwner) CountDeadLettersForOriginalEventForTest(ctx context.Context, originalEventID string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE original_event_id=$1`, originalEventID).Scan(&out)
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) CountDeadLettersForOriginalEventForTest(ctx context.Context, originalEventID string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE original_event_id=$1`, originalEventID).Scan(&out)
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadTargetFailureDeadLetterStorageForTest(ctx context.Context, eventID string) (TargetFailureDeadLetterStorage, error) {
	var out TargetFailureDeadLetterStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT failure->'detail'->>'code', COALESCE((failure->'detail'->'attributes'->'target')::text, '')
FROM dead_letters
WHERE original_event_id = $1::uuid
  AND failure->>'class' = 'platform.target_unreachable'
  AND handler_node = 'pin_routing'`, eventID).Scan(&out.Reason, &out.TargetContext)
	})
	if err != nil {
		return TargetFailureDeadLetterStorage{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadTargetFailureDeadLetterStorageForTest(ctx context.Context, eventID string) (TargetFailureDeadLetterStorage, error) {
	var out TargetFailureDeadLetterStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COALESCE(json_extract(failure, '$.detail.code'), ''), COALESCE(json_extract(failure, '$.detail.attributes.target'), '')
FROM dead_letters
WHERE original_event_id = ?
  AND json_extract(failure, '$.class') = 'platform.target_unreachable'
  AND handler_node = 'pin_routing'`, eventID).Scan(&out.Reason, &out.TargetContext)
	})
	if err != nil {
		return TargetFailureDeadLetterStorage{}, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadChainDepthDeadLetterStorageForTest(ctx context.Context, runID, entityID string) (ChainDepthDeadLetterStorage, error) {
	var out ChainDepthDeadLetterStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(dl.chain_depth), 0), COALESCE(MAX(dl.handler_node), ''),
COALESCE(MAX(dl.failure->>'class'), ''), COALESCE(MAX(dl.original_event), '')
FROM dead_letters dl JOIN events source ON source.event_id=dl.original_event_id
WHERE source.run_id=$1::uuid
AND COALESCE(NULLIF(dl.original_payload->>'entity_id',''),COALESCE(dl.entity_id::text,''))=$2
AND dl.failure->>'class'='platform.chain_depth_exceeded'`, runID, entityID).Scan(&out.Count, &out.Depth, &out.HandlerNode, &out.FailureClass, &out.OriginalEvent)
	})
	if err != nil {
		return ChainDepthDeadLetterStorage{}, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadChainDepthDeadLetterStorageForTest(ctx context.Context, runID, entityID string) (ChainDepthDeadLetterStorage, error) {
	var out ChainDepthDeadLetterStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(dl.chain_depth), 0), COALESCE(MAX(dl.handler_node), ''),
COALESCE(MAX(json_extract(dl.failure,'$.class')), ''), COALESCE(MAX(dl.original_event), '')
FROM dead_letters dl JOIN events source ON source.event_id=dl.original_event_id
WHERE source.run_id=?
AND COALESCE(NULLIF(json_extract(dl.original_payload,'$.entity_id'),''),COALESCE(dl.entity_id,''))=?
AND json_extract(dl.failure,'$.class')='platform.chain_depth_exceeded'`, runID, entityID).Scan(&out.Count, &out.Depth, &out.HandlerNode, &out.FailureClass, &out.OriginalEvent)
	})
	if err != nil {
		return ChainDepthDeadLetterStorage{}, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) CountDeadLetterEntityRelationsSinceForTest(ctx context.Context, since time.Time, entityID string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters dl
WHERE COALESCE(NULLIF(dl.original_payload->>'entity_id',''),COALESCE(dl.entity_id::text,''))=$1 AND dl.created_at >= $2`, entityID, since).Scan(&out)
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) CountDeadLetterEntityRelationsSinceForTest(ctx context.Context, since time.Time, entityID string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters dl
WHERE COALESCE(NULLIF(json_extract(dl.original_payload,'$.entity_id'),''),COALESCE(dl.entity_id,''))=? AND dl.created_at >= ?`, entityID, since).Scan(&out)
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *DeliveryPostgresOwner) ReadDeadLetterObservationRowsForTest(ctx context.Context) ([]DeadLetterObservationRow, error) {
	var out []DeadLetterObservationRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT dl.original_event_id::text,COALESCE(dl.entity_id::text,''),
COALESCE(dl.original_payload->>'entity_id',''),COALESCE(dl.handler_node,'')
FROM dead_letters dl ORDER BY dl.created_at,dl.dead_letter_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		out = []DeadLetterObservationRow{}
		for rows.Next() {
			var row DeadLetterObservationRow
			if err := rows.Scan(&row.OriginalEventID, &row.StoredEntityID, &row.PayloadEntityID, &row.HandlerNode); err != nil {
				return err
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DeliverySQLiteOwner) ReadDeadLetterObservationRowsForTest(ctx context.Context) ([]DeadLetterObservationRow, error) {
	var out []DeadLetterObservationRow
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT dl.original_event_id,COALESCE(dl.entity_id,''),
COALESCE(json_extract(dl.original_payload,'$.entity_id'),''),COALESCE(dl.handler_node,'')
FROM dead_letters dl ORDER BY dl.created_at,dl.dead_letter_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		out = []DeadLetterObservationRow{}
		for rows.Next() {
			var row DeadLetterObservationRow
			if err := rows.Scan(&row.OriginalEventID, &row.StoredEntityID, &row.PayloadEntityID, &row.HandlerNode); err != nil {
				return err
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
