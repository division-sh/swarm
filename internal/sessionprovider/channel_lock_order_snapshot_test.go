//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type lockOrderSnapshot map[string]map[string]map[string]storetest.SelectedForkSnapshotCell

// Only decoding/comparison lives here; the existing selected owner supplies
// the complete native snapshot, with no SQL or database handle in this fixture.
func readLockOrderSnapshot(ctx context.Context, selected any, ack bool) (lockOrderSnapshot, error) {
	storage, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		return nil, err
	}
	tables := map[string][]string{
		"operator_channel_bindings": {"interface_key"}, "operator_channel_operations": {"operation_id"},
		"channel_onboarding_operations": {"operation_id"},
	}
	if ack {
		for table, key := range map[string]string{
			"connected_channel_activations": "activation_id", "mailbox": "item_id",
			"operator_channel_action_intents": "publication_id", "operator_channel_text_intents": "publication_id",
			"channel_delivery_plans": "delivery_id", "channel_delivery_receipts": "effect_operation_id",
		} {
			tables[table] = []string{key}
		}
		tables["api_idempotency"] = []string{"method", "actor_kind", "actor_id", "idempotency_key"}
	}
	out := lockOrderSnapshot{}
	for table, keys := range tables {
		rows, err := storetest.DecodeSelectedForkSnapshotRows(storage[table])
		if err != nil {
			return nil, err
		}
		out[table] = map[string]map[string]storetest.SelectedForkSnapshotCell{}
		for _, row := range rows {
			var values []string
			for _, key := range keys {
				value, err := row[key].Text()
				if err != nil {
					return nil, err
				}
				values = append(values, value)
			}
			encoded, _ := json.Marshal(values)
			if _, duplicate := out[table][string(encoded)]; duplicate {
				return nil, fmt.Errorf("duplicate lock-order snapshot row")
			}
			out[table][string(encoded)] = row
		}
	}
	return out, nil
}

func (before lockOrderSnapshot) UnchangedExceptNewIdentity(after lockOrderSnapshot, identity string, committed bool) bool {
	if committed {
		key, _ := json.Marshal([]string{identity})
		if _, found := after["operator_channel_operations"][string(key)]; !found {
			return false
		}
		delete(after["operator_channel_operations"], string(key))
	}
	return reflect.DeepEqual(before, after)
}

func (before lockOrderSnapshot) ExactExceptACK(after lockOrderSnapshot, noticeID, publicationID string, committed bool) bool {
	for table, rows := range before {
		got := after[table]
		count := len(rows)
		if table == "api_idempotency" && committed {
			count++
		}
		if count != len(got) {
			return false
		}
		for key, old := range rows {
			current, found := got[key]
			if !found {
				return false
			}
			id := old["item_id"]
			want := noticeID
			if table == "operator_channel_action_intents" {
				id, want = old["publication_id"], publicationID
			}
			value, _ := id.Text()
			if committed && (table == "mailbox" || table == "operator_channel_action_intents") && value == want {
				if !exactACKMutation(table, old, current) {
					return false
				}
			} else if !reflect.DeepEqual(old, current) {
				return false
			}
		}
	}
	return true
}

func exactACKMutation(table string, before, current map[string]storetest.SelectedForkSnapshotCell) bool {
	old := maps.Clone(before)
	if table == "mailbox" {
		if string(old["notified"].Value) != "false" || string(current["notified"].Value) != "true" {
			return false
		}
		old["notified"] = current["notified"]
	} else {
		state, _ := old["state"].Text()
		next, _ := current["state"].Text()
		disposition, _ := current["disposition"].Text()
		var at time.Time
		if state != "pending" || next != "settled" || disposition != "applied" || string(old["settled_at"].Value) != "null" ||
			json.Unmarshal(current["settled_at"].Value, &at) != nil || at.IsZero() {
			return false
		}
		for _, column := range []string{"state", "disposition", "settled_at"} {
			old[column] = current[column]
		}
	}
	return reflect.DeepEqual(old, current)
}

func (after lockOrderSnapshot) ACKCompletionExact(req apiidempotency.Request, committed bool) (bool, error) {
	key, _ := json.Marshal([]string{req.Method, string(req.Actor.Kind), req.Actor.ID, req.IdempotencyKey})
	row, found := after["api_idempotency"][string(key)]
	if !committed {
		return !found, nil
	}
	if !found {
		return false, nil
	}
	resource, err := row["resource_id"].Text()
	if err != nil {
		return false, err
	}
	hash, err := row["request_hash"].Text()
	if err != nil {
		return false, err
	}
	response, err := row["response"].Text()
	if err != nil {
		return false, err
	}
	var actual map[string]any
	if err := json.Unmarshal([]byte(response), &actual); err != nil {
		return false, err
	}
	return resource == req.ResourceID && hash == req.RequestHash && reflect.DeepEqual(actual,
		map[string]any{"ok": true, "mailbox_id": req.ResourceID, "kind": "notice"}), nil
}
