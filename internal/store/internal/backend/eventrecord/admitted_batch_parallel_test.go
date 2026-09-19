package eventrecord

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// Frozen pre-parallel physical decoding loop; no production scheduling helper.
func serialDecodeLoadedRecordsBefore(records []Record) []DecodedRecord {
	results := make([]DecodedRecord, len(records))
	for i := range records {
		results[i].Event, results[i].Settlement, results[i].Err = records[i].DecodeWithSettlement()
	}
	return results
}

func batchDecodeProbeRecords(tb testing.TB, count int) []Record {
	tb.Helper()
	admitted, settlement, _ := admittedSerializationFixture(tb, 18, "consumer/parallel-admission")
	base, err := FromAdmitted(admitted, settlement)
	if err != nil {
		tb.Fatal(err)
	}
	base.RouteSettlement = equalityFormattedJSON(tb, base.RouteSettlement)
	records := make([]Record, count)
	for i := range records {
		records[i] = base.Clone()
		records[i].EventID = fmt.Sprintf("11111111-1111-4111-8111-%012d", i+1)
	}
	return records
}

func TestBatchDecodePhysicalCandidateParity(t *testing.T) {
	for name, mutate := range map[string]func(*Record){
		"valid":        func(*Record) {},
		"payload_null": func(r *Record) { r.Payload = []byte(`{"bad":null}`) },
		"timestamp":    func(r *Record) { r.CreatedAt = r.CreatedAt.Add(time.Nanosecond) },
		"identity":     func(r *Record) { r.EventID += " " },
		"unknown_settlement": func(r *Record) {
			r.RouteSettlement = []byte(`{"write_class":"normal_publication","arm":"delivery","evaluation":{"plans":[]},"unknown":true}`)
		},
		"trailing_settlement": func(r *Record) { r.RouteSettlement = append(r.RouteSettlement, []byte(` {}`)...) },
		"invalid_source":      func(r *Record) { r.SourceRoute = []byte(`{"flow_id":"foreign"}`) },
		"zero_record":         func(r *Record) { *r = Record{} },
	} {
		t.Run(name, func(t *testing.T) {
			records := batchDecodeProbeRecords(t, 18)
			mutate(&records[1])
			mutate(&records[16])
			before := make([]Record, len(records))
			for i := range records {
				before[i] = records[i].Clone()
			}
			sequential, parallel := serialDecodeLoadedRecordsBefore(records), DecodeLoadedRecords(records)
			for i := range sequential {
				a, b := sequential[i], parallel[i]
				if (a.Err == nil) != (b.Err == nil) || errors.Is(a.Err, ErrCorrupt) != errors.Is(b.Err, ErrCorrupt) {
					t.Fatalf("index%d acceptance changed: scalar=%v parallel=%v", i, a.Err, b.Err)
				}
				if a.Err != nil {
					if a.Err.Error() != b.Err.Error() || b.Event.ID() != "" || b.Settlement.WriteClass().Code() != "" {
						t.Fatalf("index%d error/evidence changed: scalar=%v parallel=%v", i, a.Err, b.Err)
					}
					continue
				}
				left, err := FromAdmitted(a.Event, a.Settlement)
				if err != nil {
					t.Fatal(err)
				}
				right, err := FromAdmitted(b.Event, b.Settlement)
				if err != nil || !left.Equal(right) || b.Event.ID() != records[i].EventID {
					t.Fatalf("index%d order or admitted facts changed: %v", i, err)
				}
			}
			if !reflect.DeepEqual(before, records) {
				t.Fatal("parallel decoding mutated input records")
			}
		})
	}
}

func TestBatchDecodeLoadedRecordsJoinedAndFresh(t *testing.T) {
	for _, count := range []int{0, 1, 2, 3, 4, 18, 128} {
		t.Run(fmt.Sprintf("records%d", count), func(t *testing.T) {
			records := batchDecodeProbeRecords(t, count)
			results := DecodeLoadedRecords(records)
			if len(results) != count {
				t.Fatalf("batch cardinality=%d, want%d", len(results), count)
			}
			for i, result := range results {
				if result.Err != nil || result.Event.ID() != records[i].EventID {
					t.Fatalf("worker result%d incomplete on return: %v", i, result.Err)
				}
				payload := bytes.Clone(records[i].Payload)
				// The completed call no longer owns or reads any input bytes.
				// Race qualification detects a worker escaping the join here.
				records[i].Payload[0] = '!'
				records[i].RouteSettlement[0] = '!'
				if !bytes.Equal(result.Event.Event().Payload(), payload) {
					t.Fatalf("worker result%d aliases input bytes", i)
				}
			}
			for i, result := range DecodeLoadedRecords(records) {
				if !errors.Is(result.Err, ErrCorrupt) || result.Event.ID() != "" {
					t.Fatalf("later decode%d reused admission or exposed partial result: %v", i, result.Err)
				}
			}
		})
	}
}

func BenchmarkBatchDecodePhysicalCandidate(b *testing.B) {
	for _, count := range []int{1, 18} {
		records := batchDecodeProbeRecords(b, count)
		for _, workers := range []int{1, 4} {
			b.Run(fmt.Sprintf("records%d/workers%d", count, workers), func(b *testing.B) {
				decode := serialDecodeLoadedRecordsBefore
				if workers == 4 {
					decode = DecodeLoadedRecords
				}
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					results := decode(records)
					for _, result := range results {
						if result.Err != nil {
							b.Fatal(result.Err)
						}
					}
				}
			})
		}
	}
}
