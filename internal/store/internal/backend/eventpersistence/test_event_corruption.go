package eventpersistence

import "context"

func (s *EventPostgresOwner) CorruptTargetRouteScalarForTest(ctx context.Context, eventID string) error {
	_, err := s.backend.ExecContext(ctx, `UPDATE events SET target_route = $1::jsonb WHERE event_id = $2::uuid`, `"bad"`, eventID)
	return err
}

func (s *EventSQLiteOwner) CorruptTargetRouteScalarForTest(ctx context.Context, eventID string) error {
	_, err := s.backend.ExecContext(ctx, `UPDATE events SET target_route = ? WHERE event_id = ?`, `"bad"`, eventID)
	return err
}

func (s *EventPostgresOwner) CorruptInboundDuplicatePayloadForTest(ctx context.Context, eventID string) error {
	_, err := s.backend.ExecContext(ctx, `UPDATE events SET payload = '{"corrupt":true}'::jsonb, payload_bytes = '{"corrupt":true}'::bytea WHERE event_id = $1::uuid`, eventID)
	return err
}

func (s *EventSQLiteOwner) CorruptInboundDuplicatePayloadForTest(ctx context.Context, eventID string) error {
	_, err := s.backend.ExecContext(ctx, `UPDATE events SET payload = '{"corrupt":true}', payload_bytes = CAST('{"corrupt":true}' AS BLOB) WHERE event_id = ?`, eventID)
	return err
}

func (s *EventPostgresOwner) CountExactEventForTest(ctx context.Context, eventID string) (int, error) {
	var count int
	err := s.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&count)
	return count, err
}

func (s *EventSQLiteOwner) CountExactEventForTest(ctx context.Context, eventID string) (int, error) {
	var count int
	err := s.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = ?`, eventID).Scan(&count)
	return count, err
}
