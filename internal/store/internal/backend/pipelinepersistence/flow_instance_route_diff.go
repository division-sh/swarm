package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

type routeRecordKey struct {
	eventPattern   string
	subscriberType string
	subscriberID   string
}

type routeSourceKey struct {
	routeRecordKey
	sourceFlow string
}

type storedMaterializedRoute struct {
	key              routeRecordKey
	sourceFlow       string
	materializedFrom string
	status           string
	wildcard         bool
}

type routeSourceWinner struct {
	ruleID    string
	createdAt string
	ambiguous bool
}

type routeTopologySnapshot struct {
	byInstance map[string][]storedMaterializedRoute
	sources    map[routeSourceKey]routeSourceWinner
}

const sqliteRouteTopologySnapshotSQL = `SELECT flow_instance, event_pattern, subscriber_type, subscriber_id,
		COALESCE(source_flow, ''), COALESCE(CAST(materialized_from AS TEXT), ''), status, is_wildcard
		FROM routing_rules WHERE run_id = ? AND is_materialized = TRUE`

const postgresRouteTopologySnapshotSQL = `SELECT flow_instance, event_pattern, subscriber_type, subscriber_id,
			COALESCE(source_flow, ''), COALESCE(CAST(materialized_from AS TEXT), ''), status, is_wildcard
			FROM routing_rules WHERE run_id = $1::uuid AND is_materialized = TRUE`

const routeTopologySourcesSQL = `SELECT event_pattern, subscriber_type, subscriber_id,
		COALESCE(source_flow, ''), CAST(rule_id AS TEXT), CAST(created_at AS TEXT)
		FROM routing_rules
		WHERE run_id IS NULL AND is_wildcard = TRUE AND is_materialized = FALSE AND status = 'active'
		ORDER BY event_pattern, subscriber_type, subscriber_id, COALESCE(source_flow, ''), created_at ASC`

func loadRouteTopologySnapshot(ctx context.Context, tx *sql.Tx, postgres bool, runID string) (routeTopologySnapshot, error) {
	snapshot := routeTopologySnapshot{
		byInstance: make(map[string][]storedMaterializedRoute),
		sources:    make(map[routeSourceKey]routeSourceWinner),
	}
	routeQuery := sqliteRouteTopologySnapshotSQL
	if postgres {
		routeQuery = postgresRouteTopologySnapshotSQL
	}
	rows, err := tx.QueryContext(ctx, routeQuery, runID)
	if err != nil {
		return routeTopologySnapshot{}, fmt.Errorf("read selected flow-instance route truth: %w", err)
	}
	for rows.Next() {
		var instance sql.NullString
		var row storedMaterializedRoute
		if err := rows.Scan(&instance, &row.key.eventPattern, &row.key.subscriberType, &row.key.subscriberID,
			&row.sourceFlow, &row.materializedFrom, &row.status, &row.wildcard); err != nil {
			_ = rows.Close()
			return routeTopologySnapshot{}, fmt.Errorf("scan selected flow-instance route truth: %w", err)
		}
		if !instance.Valid {
			_ = rows.Close()
			return routeTopologySnapshot{}, fmt.Errorf("materialized route has no exact flow instance")
		}
		snapshot.byInstance[instance.String] = append(snapshot.byInstance[instance.String], row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return routeTopologySnapshot{}, fmt.Errorf("iterate selected flow-instance route truth: %w", err)
	}
	if err := rows.Close(); err != nil {
		return routeTopologySnapshot{}, fmt.Errorf("close selected flow-instance route truth: %w", err)
	}

	sourceRows, err := tx.QueryContext(ctx, routeTopologySourcesSQL)
	if err != nil {
		return routeTopologySnapshot{}, fmt.Errorf("read current wildcard route sources: %w", err)
	}
	for sourceRows.Next() {
		var key routeSourceKey
		var ruleID string
		var createdAt string
		if err := sourceRows.Scan(&key.eventPattern, &key.subscriberType, &key.subscriberID,
			&key.sourceFlow, &ruleID, &createdAt); err != nil {
			_ = sourceRows.Close()
			return routeTopologySnapshot{}, fmt.Errorf("scan current wildcard route source: %w", err)
		}
		if previous, exists := snapshot.sources[key]; exists {
			if previous.createdAt == createdAt {
				previous.ambiguous = true
				snapshot.sources[key] = previous
			}
			continue
		}
		snapshot.sources[key] = routeSourceWinner{ruleID: ruleID, createdAt: createdAt}
	}
	if err := sourceRows.Err(); err != nil {
		_ = sourceRows.Close()
		return routeTopologySnapshot{}, fmt.Errorf("iterate current wildcard route sources: %w", err)
	}
	if err := sourceRows.Close(); err != nil {
		return routeTopologySnapshot{}, fmt.Errorf("close current wildcard route sources: %w", err)
	}
	return snapshot, nil
}

func (s routeTopologySnapshot) exactOwner(set runtimebus.FlowInstanceRouteRecordSet) bool {
	current := s.byInstance[set.Identity.Route.InstancePath]
	if len(current) == 0 && len(set.Routes) == 0 {
		return true
	}
	want := make(map[routeRecordKey]runtimebus.FlowInstanceRouteRecord, len(set.Routes))
	for _, route := range set.Routes {
		key := routeRecordKey{route.EventPattern, route.SubscriberType, route.SubscriberID}
		if _, duplicate := want[key]; duplicate {
			return false
		}
		want[key] = route
	}
	active := make(map[routeRecordKey]struct{}, len(current))
	for _, row := range current {
		wanted, exists := want[row.key]
		if row.status != "active" {
			if exists { // The original upsert would reactivate this matching row.
				return false
			}
			continue
		}
		if !exists || row.wildcard || row.sourceFlow != wanted.SourceFlow {
			return false
		}
		if _, duplicate := active[row.key]; duplicate {
			return false
		}
		active[row.key] = struct{}{}
		winner := s.sources[routeSourceKey{row.key, wanted.SourceFlow}]
		if winner.ambiguous || row.materializedFrom != winner.ruleID {
			return false
		}
	}
	return len(active) == len(want)
}
