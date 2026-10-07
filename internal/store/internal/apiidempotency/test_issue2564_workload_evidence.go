package apiidempotency

import "context"

type H2WorkloadActorEvidence struct{ Kind, ID string }

func (s *PostgresOwner) ObserveH2WorkloadActorForTest(ctx context.Context) (H2WorkloadActorEvidence, error) {
	var out H2WorkloadActorEvidence
	err := s.backend.QueryRowContext(ctx, `SELECT actor_kind,actor_id FROM api_idempotency WHERE method='event.publish' AND idempotency_key='h2-start-1'`).Scan(&out.Kind, &out.ID)
	if err != nil {
		return H2WorkloadActorEvidence{}, err
	}
	return out, nil
}
