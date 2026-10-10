package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeBusPausedExpiryRetainsRealClaimAndEveryParkingContinuationAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-paused-claim-expiry" {
			continue
		}
		matched++
		switch row.Function {
		case "TestPausedHandedAgentParksUntilContinueBothStores":
			if strings.Replace(row.Before, "expirePausedDeliveryClaim(t, f, id)", "expirePausedDeliveryClaim(t, f, claim.Claim)", 1) != row.After {
				t.Fatal("paused expiry changed pending/retry/stale cases, parking, manager work or continuation assertions")
			}
		case "expirePausedDeliveryClaim":
			if !strings.Contains(row.After, "claim runtimedelivery.Claim") || !strings.Contains(row.After, "storetest.ExpireExactDeliveryClaimFault(f.ctx, f.store, claim)") || strings.Contains(row.After, "f.db") {
				t.Fatal("expiry lost live claim or original selected owner")
			}
			owner := selectedCausalObservationBody(t, "internal/store/internal/backend/delivery/adapter.go", "ExpireExactDeliveryClaimFaultTx")
			for _, cut := range []string{"now.Add(-2*time.Hour)", "now.Add(-time.Hour)", "SET created_at=$1,started_at=$1,updated_at=$2", "SET started_at=$1,lease_expires_at=$2", "status='in_progress'", "open_marker=TRUE", "claim.DeliveryID()", "claim.RunID()", "claim.Version()", "claim.PersistenceToken()"} {
				if !strings.Contains(owner, cut) {
					t.Fatalf("exact expiry owner lost temporal/identity cut %s", cut)
				}
			}
			if strings.Count(owner, "count != 1") != 2 {
				t.Fatal("exact expiry lost dual-row rollback cardinality")
			}
		case "assertForkReceiverFaultOnlyColumns":
			for _, cut := range []string{"decodeForkReceiverFaultRow(raw, old.Columns)", "decodeForkReceiverFaultRow(newRaw, new.Columns)", "forkReceiverFaultKeyMatches(values[key], edit.value)", "forkReceiverFaultKeyMatches(newValues[key], edit.value)", "values[index].Type != newValues[index].Type", "!reflect.DeepEqual(old.Columns, new.Columns)", "reflect.DeepEqual(values[index], newValues[index])", "if changed != 1", "if matches != 1", "if !reflect.DeepEqual(before, after)"} {
				if !strings.Contains(row.After, cut) {
					t.Fatalf("typed fault witness lost exact conservation cut %s", cut)
				}
			}
		default:
			t.Fatal("unknown paused expiry consumer")
		}
		if selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("paused expiry diverged from finite migration")
		}
	}
	if matched != 3 {
		t.Fatalf("paused expiry recipes=%d,want3", matched)
	}
}
