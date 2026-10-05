package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/servedparity"
)

// Failure evidence is bounded and allowlisted: never render inputs, provider
// references, credentials, authority blobs, or effect response/failure bodies.
func logUnsettledChannelDeliveryPlans(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	plans, err := readChannelDeliveryDiagnosticRows(ctx, db, `SELECT
		CAST(delivery_id AS TEXT) AS delivery_id FROM channel_delivery_plans
		WHERE state IN ('planned','rendered') ORDER BY delivery_id LIMIT 21`)
	if err != nil {
		t.Logf("unsettled channel delivery diagnostic: %v", err)
		return
	}
	if len(plans) > 20 {
		t.Log("unsettled channel delivery diagnostic truncated after 20 plans")
		plans = plans[:20]
	}
	for _, plan := range plans {
		evidence, err := readChannelDeliverySettlementDiagnostic(ctx, db, plan["delivery_id"])
		if err != nil {
			t.Logf("unsettled channel delivery diagnostic %s: %v", plan["delivery_id"], err)
			continue
		}
		raw, err := json.Marshal(evidence)
		if err != nil {
			t.Logf("unsettled channel delivery diagnostic encoding: %v", err)
			continue
		}
		t.Logf("unsettled channel delivery: %s", raw)
	}
}

func readChannelDeliverySettlementDiagnostic(ctx context.Context, db *sql.DB, deliveryID string) (map[string]any, error) {
	evidence := map[string]any{"delivery_id": deliveryID}
	queries := []struct{ name, query string }{
		{"plan_source_default", `SELECT
			CAST(p.delivery_id AS TEXT) AS delivery_id, p.source_kind,
			CAST(p.source_id AS TEXT) AS source_id, p.state AS plan_state,
			CAST(p.principal_id AS TEXT) AS principal_id, p.interface_key,
			CAST(p.binding_revision AS TEXT) AS plan_binding_revision,
			CAST(p.delivery_epoch AS TEXT) AS plan_delivery_epoch,
			CAST(p.request_activation_id AS TEXT) AS request_activation_id,
			CAST(p.current_render_id AS TEXT) AS current_render_id,
			CAST(p.current_receipt_operation_id AS TEXT) AS current_receipt_operation_id,
			c.status AS card_status, CAST(c.run_id AS TEXT) AS card_run_id,
			n.status AS notice_status, selected.state AS default_state,
			CAST(selected.principal_id AS TEXT) AS default_principal_id,
			selected.interface_key AS default_interface_key,
			CAST(selected.binding_revision AS TEXT) AS default_binding_revision,
			CAST(selected.delivery_epoch AS TEXT) AS default_delivery_epoch,
			binding.status AS binding_status,
			CAST(binding.binding_revision AS TEXT) AS binding_revision,
			CAST(binding.operation_id AS TEXT) AS binding_operation_id,
			CAST(p.external_account_reference=selected.external_account_reference AS TEXT) AS default_account_matches,
			CAST(p.conversation_reference=selected.conversation_reference AS TEXT) AS default_conversation_matches,
			CAST(p.conversation_scope=selected.conversation_scope AS TEXT) AS default_scope_matches,
			CAST(binding.external_account_reference=selected.external_account_reference AS TEXT) AS binding_account_matches,
			CAST(binding.conversation_reference=selected.conversation_reference AS TEXT) AS binding_conversation_matches,
			CAST(binding.conversation_scope=selected.conversation_scope AS TEXT) AS binding_scope_matches
			FROM channel_delivery_plans p
			LEFT JOIN decision_cards c ON p.source_kind='card' AND c.card_id=p.source_id
			LEFT JOIN mailbox n ON p.source_kind='notice' AND n.item_id=p.source_id
			LEFT JOIN channel_delivery_defaults selected ON selected.singleton_id=1
			LEFT JOIN operator_channel_bindings binding ON binding.interface_key=selected.interface_key
			WHERE CAST(p.delivery_id AS TEXT)=$1`},
		{"activations", `SELECT CAST(a.activation_id AS TEXT) AS activation_id,
			a.status, CAST(a.activation_revision AS TEXT) AS activation_revision,
			CAST(a.principal_id AS TEXT) AS principal_id, a.interface_key,
			CAST(a.binding_revision AS TEXT) AS binding_revision,
			a.bundle_hash, a.bundle_identity, a.pack_inventory_generation,
			CAST(a.runtime_instance_id AS TEXT) AS runtime_instance_id,
			CAST(a.context_publication_generation AS TEXT) AS context_publication_generation,
			a.plan_generation, CAST(a.target_generation AS TEXT) AS target_generation,
			CAST(a.conversation_reference=p.conversation_reference AS TEXT) AS conversation_matches,
			CAST(o.operation_id AS TEXT) AS onboarding_operation_id, o.phase AS onboarding_phase,
			CAST(o.identity_operation_id AS TEXT) AS identity_operation_id
			FROM channel_delivery_plans p JOIN connected_channel_activations a
			ON a.principal_id=p.principal_id AND a.interface_key=p.interface_key
			JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
			WHERE CAST(p.delivery_id AS TEXT)=$1 ORDER BY a.created_at,a.activation_id LIMIT 21`},
		{"renders", `SELECT CAST(r.render_id AS TEXT) AS render_id,
			CAST(r.source_revision AS TEXT) AS source_revision, r.render_hash,
			CAST(r.created_at AS TEXT) AS created_at,
			CAST(r.render_id=p.current_render_id AS TEXT) AS is_current
			FROM channel_delivery_renders r JOIN channel_delivery_plans p ON p.delivery_id=r.delivery_id
			WHERE CAST(p.delivery_id AS TEXT)=$1 ORDER BY r.created_at DESC,r.render_id LIMIT 21`},
		{"receipts", `SELECT CAST(effect_operation_id AS TEXT) AS operation_id,
			CAST(attempt_id AS TEXT) AS attempt_id, CAST(render_id AS TEXT) AS render_id,
			state, CAST(settled_at AS TEXT) AS settled_at FROM channel_delivery_receipts
			WHERE CAST(delivery_id AS TEXT)=$1 ORDER BY settled_at DESC,effect_operation_id LIMIT 21`},
	}
	for _, query := range queries {
		rows, err := readChannelDeliveryDiagnosticRows(ctx, db, query.query, deliveryID)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", query.name, err)
		}
		evidence[query.name] = rows
	}
	// Decode structured authority for exact identity matching; do not search raw
	// JSON by substring or print private, unrelated authority evidence.
	operations, err := readChannelDeliveryDiagnosticRows(ctx, db, `SELECT
		CAST(operation_id AS TEXT) AS operation_id, effect_kind, state,
		CAST(authority_evidence AS TEXT) AS authority_evidence
		FROM runtime_external_effect_operations WHERE authority_kind='channel_delivery'
		ORDER BY created_at DESC,operation_id LIMIT 1025`)
	if err != nil {
		return nil, err
	}
	evidence["operation_scan_truncated"] = len(operations) > 1024
	if len(operations) > 1024 {
		operations = operations[:1024]
	}
	var matching []map[string]any
	for _, operation := range operations {
		var authority map[string]json.RawMessage
		if err := json.Unmarshal([]byte(operation["authority_evidence"]), &authority); err != nil {
			return nil, fmt.Errorf("decode channel operation %s: %w", operation["operation_id"], err)
		}
		var id string
		if err := json.Unmarshal(authority["delivery_id"], &id); err != nil {
			return nil, fmt.Errorf("decode channel operation delivery: %w", err)
		}
		if id != deliveryID {
			continue
		}
		coordinate := map[string]json.RawMessage{}
		for _, key := range []string{"delivery_id", "effect_operation_id", "render_id", "render_hash",
			"previous_receipt_operation_id", "principal_id", "interface_key", "delivery_epoch",
			"binding_revision", "activation_id", "activation_revision", "bundle_hash", "bundle_identity",
			"pack_inventory_generation", "runtime_instance_id", "context_publication_generation",
			"plan_generation", "target_generation"} {
			if value, found := authority[key]; found {
				coordinate[key] = value
			}
		}
		attempts, err := readChannelDeliveryDiagnosticRows(ctx, db, `SELECT
			CAST(attempt_id AS TEXT) AS attempt_id, CAST(attempt_ordinal AS TEXT) AS ordinal,
			state, adapter, transport, CAST(generation AS TEXT) AS generation,
			CAST(fence_generation AS TEXT) AS fence_generation,
			CAST(authorized_at AS TEXT) AS authorized_at, CAST(launched_at AS TEXT) AS launched_at,
			CAST(response_observed_at AS TEXT) AS response_observed_at,
			CAST(completed_at AS TEXT) AS completed_at
			FROM runtime_external_effect_attempts WHERE CAST(operation_id AS TEXT)=$1
			ORDER BY attempt_ordinal LIMIT 21`, operation["operation_id"])
		if err != nil {
			return nil, fmt.Errorf("attempts: %w", err)
		}
		matching = append(matching, map[string]any{"operation_id": operation["operation_id"],
			"kind": operation["effect_kind"], "state": operation["state"],
			"authority": coordinate, "attempts": attempts})
		if len(matching) == 20 {
			evidence["matching_operations_truncated"] = true
			break
		}
	}
	evidence["operations"] = matching
	return evidence, nil
}

func readChannelDeliveryDiagnosticRows(ctx context.Context, db *sql.DB, query string, args ...any) ([]map[string]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var result []map[string]string
	for rows.Next() {
		values, targets := make([]sql.NullString, len(columns)), make([]any, len(columns))
		for index := range values {
			targets[index] = &values[index]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		row := map[string]string{}
		for index, column := range columns {
			if values[index].Valid {
				row[column] = values[index].String
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func TestChannelDeliverySettlementDiagnosticBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			_, db, _ := startChannelAnchorJourney(t, backend, "settlement-diagnostic-token", false)
			var runID string
			if err := db.QueryRow(`SELECT current_run_id FROM standing_services WHERE current_run_id IS NOT NULL`).Scan(&runID); err != nil {
				t.Fatal(err)
			}
			card := waitChannelAnchorCard(t, db, runID, "stage_gate", "telegram-ingress")
			waitChannelAnchorReceipt(t, db, card)
			waitChannelDeliverySendsSettled(t, db, backend)
			var deliveryID string
			if err := db.QueryRow(`SELECT CAST(delivery_id AS TEXT) FROM channel_delivery_plans
				WHERE current_receipt_operation_id IS NOT NULL ORDER BY created_at LIMIT 1`).Scan(&deliveryID); err != nil {
				t.Fatal(err)
			}
			evidence, err := readChannelDeliverySettlementDiagnostic(context.Background(), db, deliveryID)
			if err != nil {
				t.Fatal(err)
			}
			if len(evidence["plan_source_default"].([]map[string]string)) != 1 ||
				len(evidence["activations"].([]map[string]string)) == 0 ||
				len(evidence["receipts"].([]map[string]string)) == 0 ||
				len(evidence["operations"].([]map[string]any)) == 0 {
				t.Fatal("diagnostic omitted actual plan/default/activation/receipt/operation evidence")
			}
		})
	}
}
