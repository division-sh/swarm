package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lib/pq"
)

type H2SessionEvidence struct {
	Database, Application, State, Owner string
	Sessions, Keys, AcknowledgedKeys    int
	Age                                 float64
}

// The original native fixture's observation connection stays private and is
// retained before pressure. No lock or execution authority is acquired.
type H2SessionObservation struct {
	conn    *sql.Conn
	run     string
	apiKeys []string
}

func (b *Backend) ObserveH2ServerCapacityForTest(ctx context.Context) (int, error) {
	var capacity int
	err := b.db.QueryRowContext(ctx, `SHOW max_connections`).Scan(&capacity)
	if err != nil {
		return 0, err
	}
	return capacity, nil
}

func (b *Backend) BeginH2SessionObservationForTest(ctx context.Context, run string, count int, actorKind, actorID string) (*H2SessionObservation, error) {
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	out := &H2SessionObservation{conn: conn, run: run, apiKeys: make([]string, count)}
	for ordinal := range out.apiKeys {
		encoded, err := json.Marshal([]string{"event.publish", actorKind, actorID, fmt.Sprintf("h2-bump-%04d", ordinal)})
		if err != nil {
			return nil, errors.Join(err, conn.Close())
		}
		out.apiKeys[ordinal] = "swarm:api-idempotency:" + string(encoded)
	}
	return out, nil
}

func (s *H2SessionObservation) SampleForTest(ctx context.Context, acknowledged []string) ([]H2SessionEvidence, error) {
	const query = `
		WITH pipeline_keys AS MATERIALIZED (
			SELECT event_id,hashtext('swarm:pipeline-replay:' || event_id::text)::bigint AS key,
			       event_id::text = ANY($3::text[]) AS acknowledged
			FROM events WHERE run_id=$1::uuid
		), api_keys AS MATERIALIZED (
			SELECT hashtext(unnest($2::text[]))::bigint AS key
		), held AS MATERIALIZED (
			SELECT pid,classid::bigint AS classid,objid::bigint AS objid
			FROM pg_locks WHERE locktype='advisory' AND granted AND objsubid=1
			  AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
		), pipeline AS MATERIALIZED (
			SELECT DISTINCT h.pid,k.event_id,k.acknowledged FROM held h JOIN pipeline_keys k
			  ON h.classid=CASE WHEN k.key<0 THEN 4294967295::bigint ELSE 0::bigint END
			 AND h.objid=(k.key & 4294967295::bigint)
		), api AS MATERIALIZED (
			SELECT DISTINCT h.pid FROM held h JOIN api_keys k
			  ON h.classid=CASE WHEN k.key<0 THEN 4294967295::bigint ELSE 0::bigint END
			 AND h.objid=(k.key & 4294967295::bigint)
		), sessions AS (
			SELECT a.pid,COALESCE(a.datname,'') AS database,COALESCE(a.application_name,'') AS application,
			       COALESCE(a.state,'') AS state,
			       CASE WHEN EXISTS(SELECT 1 FROM pipeline p WHERE p.pid=a.pid) THEN
			         CASE WHEN EXISTS(SELECT 1 FROM api k WHERE k.pid=a.pid) THEN 'pipeline+api' ELSE 'pipeline_only' END
			         ELSE CASE WHEN EXISTS(SELECT 1 FROM api k WHERE k.pid=a.pid) THEN 'api_only' ELSE 'unmapped' END END AS owner,
			       (SELECT COUNT(*) FROM pipeline p WHERE p.pid=a.pid) AS pipeline_keys,
			       (SELECT COUNT(*) FROM pipeline p WHERE p.pid=a.pid AND p.acknowledged) AS acknowledged_keys,
			       EXTRACT(EPOCH FROM (clock_timestamp()-a.state_change)) AS state_age
			FROM pg_stat_activity a WHERE a.backend_type='client backend' AND a.pid<>pg_backend_pid()
		)
		SELECT database,application,state,owner,COUNT(*),SUM(pipeline_keys),SUM(acknowledged_keys),COALESCE(MAX(state_age),0)
		FROM sessions GROUP BY database,application,state,owner ORDER BY database,application,state,owner`
	rows, err := s.conn.QueryContext(ctx, query, s.run, pq.Array(s.apiKeys), pq.Array(acknowledged))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H2SessionEvidence
	for rows.Next() {
		var row H2SessionEvidence
		if err := rows.Scan(&row.Database, &row.Application, &row.State, &row.Owner, &row.Sessions, &row.Keys, &row.AcknowledgedKeys, &row.Age); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *H2SessionObservation) CloseForTest() error { return s.conn.Close() }
