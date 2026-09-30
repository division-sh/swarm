package pipeline_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type a2CorruptMapSource struct {
	p        *a2MapFanOutExecution
	handoff  a2MapFanOutHandoff
	intent   fanoutobligation.Intent
	claim    fanoutobligation.Claim
	original map[string]any
	keys     []string
	raw      []byte
}

func newA2CorruptMapSource(t *testing.T, selected gateRecoveryStoreCase) *a2CorruptMapSource {
	t.Helper()
	f := &a2CorruptMapSource{p: newA2MapFanOutExecution(t, selected, true), original: map[string]any{}}
	f.keys = []string{"\t", " ", " a ", "a", "a "}
	for index, key := range f.keys {
		f.original[key] = []int64{int64(index), int64(index), int64(index + 100)}
	}
	sort.Strings(f.keys)
	f.p.publish(t, "batch.ready", f.original)
	f.p.publish(t, "batch.replace", map[string]any{
		"live-0": []int64{900}, "live-1": []int64{901}, "live-2": []int64{902},
		"live-3": []int64{903}, "live-4": []int64{904},
	})
	f.p.restart(t)
	f.handoff = f.p.handoff(t)
	var found bool
	var err error
	f.intent, f.claim, found, err = f.handoff.owner.ClaimFanOutIntent(f.p.ctx, pipeline.FanOutClaimRequest{
		Owner: "a2-raw-map-source", BundleHash: f.p.fact.BundleHash(), Candidate: &f.handoff.key,
		Now: time.Now().UTC(), Lease: 5 * time.Minute,
	})
	if err != nil || !found || f.intent.Source.Kind != fanoutobligation.SourceEntityField ||
		f.intent.Source.MutationID == "" || f.intent.Request.Cardinality != len(f.keys) {
		t.Fatalf("actual compiled immutable map source: intent=%#v found=%v err=%v", f.intent, found, err)
	}
	f.raw = f.readRaw(t)
	want, err := json.Marshal(f.original)
	if err != nil || a2AccumulatorJSONHash(t, f.raw) != a2AccumulatorJSONHash(t, want) {
		t.Fatalf("compiled writer did not create exact retained business map: raw=%s err=%v", f.raw, err)
	}
	input, err := f.handoff.owner.LoadFanOutEvaluation(f.p.ctx, f.claim)
	if err != nil || input.StartOrdinal != 0 || len(input.Items) != len(f.keys) {
		t.Fatalf("lawful source baseline: input=%#v err=%v", input, err)
	}
	for ordinal, key := range f.keys {
		if input.Items[ordinal] != key {
			t.Fatalf("baseline changed exact text key: ordinal=%d got=%#v want=%q", ordinal, input.Items[ordinal], key)
		}
	}
	t.Cleanup(func() {
		if err := f.writeRaw(f.raw); err != nil {
			t.Errorf("restore exact writer-owned source: %v", err)
		}
	})
	return f
}

func (f *a2CorruptMapSource) readRaw(t *testing.T) []byte {
	t.Helper()
	var raw []byte
	source := f.intent.Source
	if err := f.p.selected.db.QueryRowContext(f.p.ctx, `SELECT new_value FROM entity_mutations
		WHERE mutation_id=$1 AND run_id=$2 AND entity_id=$3 AND domain='authored_field' AND path=$4`,
		source.MutationID, source.RunID, source.EntityID, source.Field).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}

func (f *a2CorruptMapSource) writeRaw(raw []byte) error {
	// Only hostile setup/restoration changes this real, handler-written revision.
	// Intent identity, capsule, cardinality, outcomes and live entity state stay untouched.
	source := f.intent.Source
	result, err := f.p.selected.db.ExecContext(f.p.ctx, `UPDATE entity_mutations SET new_value=$1
		WHERE mutation_id=$2 AND run_id=$3 AND entity_id=$4 AND domain='authored_field' AND path=$5`,
		string(raw), source.MutationID, source.RunID, source.EntityID, source.Field)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return fmt.Errorf("retained business-source setup affected %d rows: %v", count, err)
	}
	return nil
}

type a2CorruptMapSourceFootprint struct {
	Mutations, Facts, Revisions, Publications, Deliveries, Outcomes int
	Cursor, Cardinality                                             int
	LatestRevision                                                  int64
	Status, State                                                   string
	Source, Diagnostic                                              [sha256.Size]byte
}

func (f *a2CorruptMapSource) footprint(t *testing.T) a2CorruptMapSourceFootprint {
	t.Helper()
	var result a2CorruptMapSourceFootprint
	var diagnostic []byte
	if err := f.p.selected.db.QueryRowContext(f.p.ctx, `SELECT
		(SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1),
		(SELECT COALESCE(MAX(revision),0) FROM run_fork_revisions WHERE run_id=$1),
		(SELECT COUNT(*) FROM events WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_deliveries WHERE event_id IN (SELECT event_id FROM events WHERE run_id=$1)),
		(SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1),
		cursor,cardinality,status,blocked_reason FROM fan_out_intents WHERE run_id=$1`, f.p.runID).Scan(
		&result.Mutations, &result.Facts, &result.Revisions, &result.LatestRevision,
		&result.Publications, &result.Deliveries, &result.Outcomes,
		&result.Cursor, &result.Cardinality, &result.Status, &diagnostic); err != nil {
		t.Fatal(err)
	}
	state, err := json.Marshal(f.p.instance(t))
	if err != nil {
		t.Fatal(err)
	}
	result.State = a2AccumulatorJSONHash(t, state)
	result.Source = sha256.Sum256(f.readRaw(t))
	result.Diagnostic = sha256.Sum256(diagnostic)
	return result
}

func TestA2MapFanOutRawBusinessSourceReaderRefusesOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			f := newA2CorruptMapSource(t, backend.open(t))
			wrongItem := make(map[string]any, len(f.original))
			wrongValue := make(map[string]any, len(f.original))
			for key, value := range f.original {
				wrongItem[key], wrongValue[key] = value, value
			}
			wrongItem["a"] = []string{"not-an-integer"}
			wrongValue["a"] = int64(7)
			wrongItemRaw, _ := json.Marshal(wrongItem)
			wrongValueRaw, _ := json.Marshal(wrongValue)
			duplicate := append(append([]byte{}, f.raw[:len(f.raw)-1]...), []byte(`,"a":[3,3,103]}`)...)
			escapedDuplicate := append(append([]byte{}, f.raw[:len(f.raw)-1]...), []byte(`,"\u0061":[3,3,103]}`)...)
			for _, corruption := range []struct {
				name      string
				raw       []byte
				want      string
				duplicate bool
				malformed bool
			}{
				{"non_map", []byte(`[[],[],[],[],[]]`), "map-text-keys source", false, false},
				{"null", []byte(`null`), "collection source is null or absent", false, false},
				{"wrong_list_item_type", wrongItemRaw, "", false, false},
				{"wrong_map_value_shape", wrongValueRaw, "", false, false},
				{"duplicate_text_key", duplicate, "duplicate JSON object key", true, false},
				{"escaped_alias_duplicate_text_key", escapedDuplicate, "duplicate JSON object key", true, false},
				{"malformed_json", f.raw[:len(f.raw)-1], "decode fan-out source collection", false, true},
			} {
				t.Run(corruption.name, func(t *testing.T) {
					if corruption.duplicate {
						if _, err := canonicaljson.Decode(corruption.raw); err == nil || !canonicaljson.IsAdmissionError(err) || !strings.Contains(err.Error(), "duplicate JSON object key") {
							t.Fatalf("duplicate control is not rejected by existing canonical owner: %v", err)
						}
					}
					beforeSetup := f.footprint(t)
					if err := f.writeRaw(corruption.raw); err != nil {
						if !f.p.selected.postgres || !corruption.malformed || !strings.Contains(err.Error(), "invalid input syntax for type json") {
							t.Fatalf("unexpected hostile source setup refusal: %v", err)
						}
						if f.footprint(t) != beforeSetup || !bytes.Equal(f.readRaw(t), f.raw) {
							t.Fatal("Postgres JSONB syntax refusal changed persisted source or execution")
						}
						t.Log("Postgres JSONB refused malformed bytes at storage; no source-reader refusal credit")
						return
					}
					t.Cleanup(func() {
						if err := f.writeRaw(f.raw); err != nil {
							t.Errorf("restore exact source after control: %v", err)
						}
					})
					before := f.footprint(t)
					input, err := f.handoff.owner.LoadFanOutEvaluation(f.p.ctx, f.claim)
					if corruption.duplicate && f.p.selected.postgres {
						retained := f.readRaw(t)
						if _, err := canonicaljson.Decode(retained); err != nil || a2AccumulatorJSONHash(t, retained) != a2AccumulatorJSONHash(t, f.raw) {
							t.Fatalf("JSONB duplicate-evidence boundary changed logical source: retained=%s err=%v", retained, err)
						}
						if err != nil || len(input.Items) != len(f.keys) {
							t.Fatalf("canonical source after JSONB duplicate erasure: input=%#v err=%v", input, err)
						}
						t.Log("Postgres JSONB erased duplicate/escaped-alias raw evidence; no source-reader duplicate rejection credit")
					} else if err == nil || (corruption.want != "" && !strings.Contains(err.Error(), corruption.want)) || len(input.Items) != 0 {
						t.Errorf("selected source reader admitted raw business corruption: items=%#v err=%v; want refusal %q", input.Items, err, corruption.want)
					}
					if after := f.footprint(t); after != before {
						t.Errorf("source read changed execution or history:\nbefore=%+v\nafter=%+v", before, after)
					}
					f.p.assertProgress(t, 0, len(f.keys), "open", f.intent.Source.MutationID)
				})
			}
			t.Run("exact_restoration_pump_gather_once", func(t *testing.T) {
				if !bytes.Equal(f.readRaw(t), f.raw) {
					t.Fatal("controls did not restore exact original writer-owned bytes")
				}
				if settlement, err := f.handoff.owner.ReleaseFanOutClaim(f.p.ctx, f.claim); err != nil || !settlement.Acknowledged {
					t.Fatalf("release original read claim: %#v err=%v", settlement, err)
				}
				arm := exactJoinPersistedArm(t, f.p.instance(t))
				if _, err := f.p.pc.ServeFanOutCandidate(f.p.ctx, f.handoff.owner, f.handoff.key); err != nil {
					t.Fatalf("actual pump after exact source restoration: %v", err)
				}
				waitForGateRecoveryQuiescence(t, f.p.bus, f.p.ctx)
				outputs := f.p.outputs(t)
				if len(outputs) != len(f.keys) {
					t.Fatalf("restored source did not produce one complete range: %#v", outputs)
				}
				collector := externalPipelineSourceNode(t, f.p.source, ".", "collector")
				for ordinal, output := range outputs {
					if output.Ordinal != ordinal || output.Payload.Key != f.keys[ordinal] || output.Payload.Index != ordinal ||
						output.Payload.Count != len(f.keys) || !reflect.DeepEqual(output.Payload.Result, f.original[f.keys[ordinal]]) {
						t.Fatalf("restored ordinal changed exact source key/value: %#v", output)
					}
					assertExactJoinDeliveryStatus(t, f.p.selected, f.p.ctx, output.EventID, collector.Key(), "delivered")
					prepared, found, err := f.p.selected.events.LoadPreparedPublishEvent(f.p.ctx, output.EventID)
					if err != nil || !found {
						t.Fatalf("restored ordinal receipt: found=%v err=%v", found, err)
					}
					bound := 0
					for _, route := range prepared.DeliveryRoutes {
						if route.Recipient.ID() == collector.Key() && len(route.Context.Joins) == 1 &&
							route.Context.Joins[0].Disposition == events.JoinAdmissionBound && route.Context.Joins[0].Ref.Equal(arm.JoinRef()) {
							bound++
						}
					}
					if bound != 1 {
						t.Fatalf("restored ordinal lacks its exact captured join receipt: %#v", prepared.DeliveryRoutes)
					}
					if err := f.p.bus.PublishAcknowledged(f.p.ctx, prepared.Event.Event()); err != nil {
						t.Fatalf("duplicate restored ordinal publication: %v", err)
					}
				}
				waitForGateRecoveryQuiescence(t, f.p.bus, f.p.ctx)
				completed := exactJoinPersistedArm(t, f.p.instance(t))
				if !completed.JoinRef().Equal(arm.JoinRef()) || completed.Status != joinruntime.StatusClosed ||
					completed.Completed() != len(f.keys) || !reflect.DeepEqual(completed.Members, f.keys) {
					t.Fatalf("restored map did not gather exactly once against captured membership: %#v", completed)
				}
				results, err := completed.Results()
				wantResults := make([]any, len(f.keys))
				for ordinal, key := range f.keys {
					wantResults[ordinal] = f.original[key]
				}
				gotResultsRaw, marshalErr := json.Marshal(results)
				wantResultsRaw, _ := json.Marshal(wantResults)
				if err != nil || marshalErr != nil || a2AccumulatorJSONHash(t, gotResultsRaw) != a2AccumulatorJSONHash(t, wantResultsRaw) {
					t.Fatalf("restored gather lost exact ordered business values: results=%#v err=%v marshal=%v", results, err, marshalErr)
				}
				beforeRetry := f.footprint(t)
				if _, err := f.p.pc.ServeFanOutCandidate(f.p.ctx, f.handoff.owner, f.handoff.key); err != nil {
					t.Fatalf("completed restored intent retry: %v", err)
				}
				if f.footprint(t) != beforeRetry {
					t.Fatal("completed restored intent retry changed outcomes, state or history")
				}
				f.p.assertProgress(t, len(f.keys), len(f.keys), "closed", f.intent.Source.MutationID)
			})
		})
	}
}

func TestA2MapFanOutRawBusinessSourcePumpRefusesWithoutProgressOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, corruption := range []string{"non_map", "null", "wrong_list_item_type"} {
			t.Run(backend.name+"/"+corruption, func(t *testing.T) {
				f := newA2CorruptMapSource(t, backend.open(t))
				raw := []byte(`[[],[],[],[],[]]`)
				if corruption == "null" {
					raw = []byte(`null`)
				} else if corruption == "wrong_list_item_type" {
					value := make(map[string]any, len(f.original))
					for key, item := range f.original {
						value[key] = item
					}
					value["a"] = []string{"not-an-integer"}
					raw, _ = json.Marshal(value)
				}
				if err := f.writeRaw(raw); err != nil {
					t.Fatal(err)
				}
				if settlement, err := f.handoff.owner.ReleaseFanOutClaim(f.p.ctx, f.claim); err != nil || !settlement.Acknowledged {
					t.Fatalf("release read claim before actual corrupt-source pump: %#v err=%v", settlement, err)
				}
				before := f.footprint(t)
				var originalFactKey string
				var originalFact []byte
				if err := f.p.selected.db.QueryRowContext(f.p.ctx, `SELECT fact_key,fact
					FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations'
					ORDER BY revision DESC LIMIT 1`, f.p.runID).Scan(&originalFactKey, &originalFact); err != nil {
					t.Fatal(err)
				}
				_, err := f.p.pc.ServeFanOutCandidate(f.p.ctx, f.handoff.owner, f.handoff.key)
				waitForGateRecoveryQuiescence(t, f.p.bus, f.p.ctx)
				if err == nil {
					t.Error("actual pump accepted corrupt retained business source")
					if after := f.footprint(t); after != before {
						t.Errorf("accepted corrupt source changed execution:\nbefore=%+v\nafter=%+v", before, after)
					}
					return
				}
				returned, typed := runtimefailures.EnvelopeFromError(err)
				returnedRaw, marshalErr := runtimefailures.MarshalEnvelope(returned)
				if !typed || marshalErr != nil {
					t.Fatalf("source refusal did not return a valid typed diagnostic: err=%v marshal=%v", err, marshalErr)
				}
				var diagnostic []byte
				var released bool
				if err := f.p.selected.db.QueryRowContext(f.p.ctx, `SELECT blocked_reason,
					claim_owner IS NULL AND lease_expires_at IS NULL FROM fan_out_intents WHERE run_id=$1`,
					f.p.runID).Scan(&diagnostic, &released); err != nil {
					t.Fatal(err)
				}
				persisted, decodeErr := runtimefailures.UnmarshalEnvelope(diagnostic)
				if decodeErr != nil || !released || a2AccumulatorJSONHash(t, diagnostic) != a2AccumulatorJSONHash(t, returnedRaw) {
					t.Fatalf("blocked diagnostic is not the exact returned typed failure: released=%v diagnostic=%s err=%v", released, diagnostic, decodeErr)
				}
				want := before
				want.Facts++
				want.Revisions++
				want.LatestRevision++
				want.Status = "blocked"
				want.Diagnostic = sha256.Sum256(diagnostic)
				if after := f.footprint(t); after != want {
					t.Errorf("source refusal changed more than its one canonical diagnostic fact/revision:\nbefore=%+v\nwant=%+v\nafter=%+v\npump error=%v", before, want, after, err)
				}
				var family, factKey string
				var fact []byte
				var present bool
				if err := f.p.selected.db.QueryRowContext(f.p.ctx, `SELECT family,fact_key,fact,present
					FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2`,
					f.p.runID, want.LatestRevision).Scan(&family, &factKey, &fact, &present); err != nil {
					t.Fatal(err)
				}
				var expectedFact map[string]any
				if err := json.Unmarshal(originalFact, &expectedFact); err != nil {
					t.Fatal(err)
				}
				if expectedFact["fact_kind"] != "intent" || expectedFact["status"] != "open" {
					t.Fatalf("baseline is not the real open intent history owner: %s", originalFact)
				}
				expectedFact["status"], expectedFact["blocked_reason"] = "blocked", string(diagnostic)
				expectedFactRaw, marshalErr := json.Marshal(expectedFact)
				if marshalErr != nil || family != "fan_out_obligations" || factKey != originalFactKey || !present ||
					a2AccumulatorJSONHash(t, fact) != a2AccumulatorJSONHash(t, expectedFactRaw) {
					t.Fatalf("source refusal history is not the exact existing intent diagnostic: family=%s key=%s present=%v fact=%s err=%v", family, factKey, present, fact, marshalErr)
				}
				beforeRetry := f.footprint(t)
				if _, err := f.p.pc.ServeFanOutCandidate(f.p.ctx, f.handoff.owner, f.handoff.key); err != nil {
					t.Fatalf("blocked candidate retry: %v", err)
				}
				if f.footprint(t) != beforeRetry {
					t.Fatal("blocked candidate retry changed business state, publication, progress or diagnostic history")
				}
				f.p.assertProgress(t, 0, len(f.keys), "blocked", f.intent.Source.MutationID)
				t.Logf("exact once-only blocked diagnostic: class=%s code=%s revision=%d; business/publication/cursor/outcomes unchanged; returned=%v", persisted.Class, persisted.Detail.Code, want.LatestRevision, err)
			})
		}
	}
}
