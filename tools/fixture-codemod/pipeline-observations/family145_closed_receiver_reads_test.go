package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func family145ClosedReceiverOwnedQueries(t *testing.T) map[string]bool {
	t.Helper()
	owners := map[string][]string{
		"internal/backend/effectpersistence/closed_receiver_observation.go":   {"ReadClosedReceiverAuthorityStorageTx"},
		"internal/backend/agentpersistence/closed_receiver_observation.go":    {"ReadClosedReceiverAgentStorageTx"},
		"internal/backend/eventrecord/closed_receiver_observation.go":         {"ReadClosedReceiverEmissionStorageTx", "ReadClosedReceiverRuntimeLogStorageTx"},
		"internal/backend/pipelinepersistence/closed_receiver_observation.go": {"CountClosedReceiverPipelineSuccessStorageTx", "ReadClosedReceiverReceiptStorageTx"},
		"internal/backend/delivery/read_projections.go":                       {"ReadClosedReceiverReadinessStorageTx", "ReadClosedReceiverDeliveryStorageTx", "ReadClosedReceiverAgentRouteStorageTx"},
	}
	owned := map[string]bool{}
	for path, functions := range owners {
		for _, function := range functions {
			source := family145ConversationForkOwnerSource(t, path, function)
			for _, query := range family145A2ClaimLiteralQueries(t, source) {
				if owned[query] {
					t.Fatalf("duplicate receiver SQL owner: %s", query)
				}
				owned[query] = true
			}
			if function == "ReadClosedReceiverDeliveryStorageTx" || function == "ReadClosedReceiverReceiptStorageTx" {
				if !strings.Contains(source, "strings.Replace(query, \"e.run_id = ?\", \"e.run_id = $1::uuid\", 1)") {
					t.Fatal("receiver dialect changed the exact run placeholder substitution")
				}
			}
		}
	}
	return owned
}

func TestFamily145ClosedReceiverEntireDialectQueryInventoryHasExactClosedOwners(t *testing.T) {
	owned := family145ClosedReceiverOwnedQueries(t)
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	consumed := map[string]bool{}
	for _, row := range rows {
		if row.File != "internal/runtime/eventbus_receiver_authority_e2e_test.go" {
			continue
		}
		for _, query := range family145A2ClaimLiteralQueries(t, row.Before) {
			// The original SQLite emission query was assembled from these
			// fixed fragments; the new owner spells the same statement whole.
			if strings.HasSuffix(query, "COALESCE(source_event_id, '') FROM events WHERE run_id =") {
				query += " ? AND event_name = ?"
			}
			if !owned[query] {
				t.Fatalf("receiver physical query lost scope/columns/NULL/order: %s", query)
			}
			consumed[query] = true
		}
	}
	if len(consumed) != len(owned) || len(owned) != 14 {
		t.Fatalf("receiver dialect-query inventory consumed=%d owned=%d/14", len(consumed), len(owned))
	}
}
