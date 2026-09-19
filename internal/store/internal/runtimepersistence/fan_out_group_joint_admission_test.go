package runtimepersistence

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestFanOutPublicationGroupJointAdmissionFreshnessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, corruption := range []string{"unknown_settlement_field", "changed_admitted_facts", "inherited_owner_mismatch"} {
			t.Run(backend+"/"+corruption, func(t *testing.T) {
				f := newB10GroupFaultFixture(t, backend)
				f.requireObservation(t, pipelineobligation.PublicationSettlementPending)
				id := f.members[1].Claim.EventID()
				var original eventrecord.Record
				var found bool
				var err error
				if backend == "postgres" {
					original, found, err = eventrecordpostgres.Load(f.ctx, f.db, id)
				} else {
					original, found, err = eventrecordsqlite.Load(f.ctx, f.db, id)
				}
				if err != nil || !found {
					t.Fatalf("load exact committed member: found=%v err=%v", found, err)
				}
				hostile := original.Clone()
				switch corruption {
				case "unknown_settlement_field":
					var wire map[string]json.RawMessage
					if err := json.Unmarshal(hostile.RouteSettlement, &wire); err != nil {
						t.Fatal(err)
					}
					wire["unowned_evidence"] = json.RawMessage(`true`)
					hostile.RouteSettlement, err = json.Marshal(wire)
					if err != nil {
						t.Fatal(err)
					}
				case "changed_admitted_facts":
					hostile.ChainDepth++
					if _, _, err := hostile.DecodeWithSettlement(); err != nil {
						t.Fatalf("changed facts must remain codec-valid: %v", err)
					}
				case "inherited_owner_mismatch":
					declaration, err := identity.AdmitDeclarationIdentity(f.seed.flowPath, "fan_out", f.seed.semanticPath)
					if err != nil {
						t.Fatal(err)
					}
					origin, err := events.NewInheritedFanOutOrigin(f.seed.runID, uuid.NewString(), f.seed.eventID, uuid.NewString(), declaration, f.seed.bundleHash, "sha256:"+strings.Repeat("3", 64), 0)
					if err != nil {
						t.Fatal(err)
					}
					hostile.Class, hostile.SourceEventID = events.EventAdmissionInheritedFanOut, ""
					hostile.InheritedFanOutOrigin, err = json.Marshal(origin)
					if err != nil {
						t.Fatal(err)
					}
					if _, _, err := hostile.DecodeWithSettlement(); err != nil {
						t.Fatalf("owner mismatch must reach SQL ownership validation: %v", err)
					}
				}
				writeRecord := func(record eventrecord.Record) {
					writeJointSourceReadRecord(t, f.ctx, f.db, backend == "postgres", record)
					if _, err := f.db.ExecContext(f.ctx, `UPDATE events SET chain_depth=$1 WHERE event_id=$2`, record.ChainDepth, id); err != nil {
						t.Fatal(err)
					}
				}
				writeRecord(hostile)
				defer writeRecord(original)
				before := f.snapshot(t)
				checkRejection := func(err error) {
					t.Helper()
					if corruption == "changed_admitted_facts" {
						if err == nil || !strings.Contains(err.Error(), "publication group committed event changed") {
							t.Fatalf("lost sealed-member integrity comparison: %v", err)
						}
					} else {
						assertJointSourceReadCorrupt(t, id, err)
					}
				}
				// Earlier healthy observation must not cache admission, and failure
				// of the later member must discard the already observed prefix.
				observed, err := f.group.ReadPublicationSettlement(f.ctx, f.members)
				checkRejection(err)
				if len(observed.Rows) != 0 || !observed.ObservedAt.IsZero() {
					t.Fatalf("corrupt later member leaked observation: %+v", observed)
				}
				collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				out, err := f.group.Settle(f.ctx, f.members)
				checkRejection(err)
				if len(out.Results) != 0 {
					t.Fatalf("corrupt member minted settlement: %+v", out)
				}
				receipt := collector.Snapshot()
				settlement := receipt.ByOperation[transactiontest.PipelineSettlement]
				if settlement.Begun != 1 || settlement.RollbackAttempts != 1 || settlement.CommitAttempts != 0 || receipt.Total.WriteCommits != 0 || receipt.Total.Revision.Finalizations != 0 || receipt.Active != 0 {
					t.Fatalf("admission failure crossed mutation boundary: %+v", receipt)
				}
				f.requireSnapshot(t, before)
				if f.sink.reserves != 0 || f.sink.submits != 0 || f.sink.cancels != 0 {
					t.Fatalf("rejected admission reached candidate handoff: %+v", f.sink)
				}
				writeRecord(original)
				f.requireObservation(t, pipelineobligation.PublicationSettlementPending)
				revisions := countP16RunRevisions(t, f.db, f.seed.runID)
				out, err = f.group.Settle(f.ctx, f.members)
				requireB10Acknowledged(t, out, err, nil)
				if countP16RunRevisions(t, f.db, f.seed.runID) != revisions+1 || f.sink.submits != 1 {
					t.Fatal("restored exact claims did not settle in one revision/handoff")
				}
				f.requireObservation(t, pipelineobligation.PublicationSettlementSatisfied)
			})
		}
	}
}
