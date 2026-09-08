package runforkrevision

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

type factKeyGolden struct {
	name   string
	family Family
	raw    string
	want   string
}

// Expected bytes are literal ledger keys, independent of the implementation.
var factKeyGoldens = []factKeyGolden{
	{"events", FamilyEvents, `{"event_id":"11111111-1111-4111-8111-111111111111"}`, "11111111-1111-4111-8111-111111111111"},
	{"mutations", FamilyEntityMutations, `{"mutation_id":"22222222-2222-4222-8222-222222222222"}`, "22222222-2222-4222-8222-222222222222"},
	{"metadata", FamilyEntityMetadata, `{"entity_id":"33333333-3333-4333-8333-333333333333"}`, "33333333-3333-4333-8333-333333333333"},
	{"deliveries", FamilyEventDeliveries, `{"delivery_id":"44444444-4444-4444-8444-444444444444"}`, "44444444-4444-4444-8444-444444444444"},
	{"replay_scopes", FamilyCommittedReplayScopes, `{"event_id":"55555555-5555-4555-8555-555555555555"}`, "55555555-5555-4555-8555-555555555555"},
	{"receipts", FamilyEventReceipts, `{"receipt_id":"66666666-6666-4666-8666-666666666666"}`, "66666666-6666-4666-8666-666666666666"},
	{"dead_letters", FamilyDeadLetters, `{"dead_letter_id":"77777777-7777-4777-8777-777777777777"}`, "77777777-7777-4777-8777-777777777777"},
	{"timers", FamilyTimers, `{"timer_id":"88888888-8888-4888-8888-888888888888"}`, "88888888-8888-4888-8888-888888888888"},
	{"sessions", FamilyAgentSessions, `{"session_id":"99999999-9999-4999-8999-999999999999"}`, "99999999-9999-4999-8999-999999999999"},
	{"turns", FamilyAgentTurns, `{"turn_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}`, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"},
	{"audits", FamilyAgentConversationAudits, `{"session_id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}`, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"},
	{"reply_text", FamilyReplyContexts, `{"reply_context_id":"request/reply|opaque:42"}`, "request/reply|opaque:42"},
	{"intent_root", FamilyFanOutObligations, `{"fact_kind":"intent","triggering_delivery_id":"11111111-1111-4111-8111-111111111111","flow_path":".","declaration_family":"fan_out","semantic_path":"nodes.work.handlers.ready.fan_out","ordinal":null}`, "intent|11111111-1111-4111-8111-111111111111|.|fan_out|nodes.work.handlers.ready.fan_out"},
	{"outcome_zero", FamilyFanOutObligations, `{"fact_kind":"outcome","triggering_delivery_id":"11111111-1111-4111-8111-111111111111","flow_path":".","declaration_family":"fan_out","semantic_path":"nodes.work.handlers.ready.fan_out","ordinal":0}`, "outcome|11111111-1111-4111-8111-111111111111|.|fan_out|nodes.work.handlers.ready.fan_out|0"},
	{"barrier_root", FamilyFanOutObligations, `{"fact_kind":"barrier","triggering_delivery_id":"11111111-1111-4111-8111-111111111111","flow_path":".","declaration_family":"fan_out","semantic_path":"nodes.work.handlers.ready.fan_out"}`, "barrier|11111111-1111-4111-8111-111111111111|.|fan_out|nodes.work.handlers.ready.fan_out"},
	{"trigger_coordinate", FamilyFanOutObligations, `{"fact_kind":"intent","triggering_delivery_id":"22222222-2222-4222-8222-222222222222","flow_path":".","declaration_family":"fan_out","semantic_path":"nodes.work.handlers.ready.fan_out"}`, "intent|22222222-2222-4222-8222-222222222222|.|fan_out|nodes.work.handlers.ready.fan_out"},
	{"flow_coordinate", FamilyFanOutObligations, `{"fact_kind":"intent","triggering_delivery_id":"11111111-1111-4111-8111-111111111111","flow_path":"parent/worker-2","declaration_family":"fan_out","semantic_path":"nodes.work.handlers.ready.fan_out"}`, "intent|11111111-1111-4111-8111-111111111111|parent/worker-2|fan_out|nodes.work.handlers.ready.fan_out"},
	{"family_coordinate", FamilyFanOutObligations, `{"fact_kind":"intent","triggering_delivery_id":"11111111-1111-4111-8111-111111111111","flow_path":".","declaration_family":"rule","semantic_path":"nodes.work.handlers.ready.fan_out"}`, "intent|11111111-1111-4111-8111-111111111111|.|rule|nodes.work.handlers.ready.fan_out"},
	{"semantic_coordinate", FamilyFanOutObligations, `{"fact_kind":"intent","triggering_delivery_id":"11111111-1111-4111-8111-111111111111","flow_path":".","declaration_family":"fan_out","semantic_path":"nodes.work.handlers.other.fan_out"}`, "intent|11111111-1111-4111-8111-111111111111|.|fan_out|nodes.work.handlers.other.fan_out"},
	{"ordinal_decimal", FamilyFanOutObligations, `{"fact_kind":"outcome","triggering_delivery_id":"11111111-1111-4111-8111-111111111111","flow_path":".","declaration_family":"fan_out","semantic_path":"nodes.work.handlers.ready.fan_out","ordinal":2147483647}`, "outcome|11111111-1111-4111-8111-111111111111|.|fan_out|nodes.work.handlers.ready.fan_out|2147483647"},
	{"intent_delimiter", FamilyFanOutObligations, `{"fact_kind":"intent","triggering_delivery_id":"11111111-1111-4111-8111-111111111111","flow_path":".","declaration_family":"fan_out","semantic_path":"nodes.work|0|"}`, "intent|11111111-1111-4111-8111-111111111111|.|fan_out|nodes.work|0|"},
	{"outcome_delimiter", FamilyFanOutObligations, `{"fact_kind":"outcome","triggering_delivery_id":"11111111-1111-4111-8111-111111111111","flow_path":".","declaration_family":"fan_out","semantic_path":"nodes.work|0|","ordinal":12}`, "outcome|11111111-1111-4111-8111-111111111111|.|fan_out|nodes.work|0||12"},
	{"barrier_delimiter", FamilyFanOutObligations, `{"fact_kind":"barrier","triggering_delivery_id":"11111111-1111-4111-8111-111111111111","flow_path":".","declaration_family":"fan_out","semantic_path":"nodes.work|0|"}`, "barrier|11111111-1111-4111-8111-111111111111|.|fan_out|nodes.work|0|"},
}

func TestFactKeyGoldenBytes(t *testing.T) {
	covered := map[Family]bool{}
	for _, tc := range factKeyGoldens {
		t.Run(tc.name, func(t *testing.T) {
			got, err := FactKey(tc.family, []byte(tc.raw))
			if err != nil || got != tc.want {
				t.Fatalf("FactKey = %q, %v; want literal %q", got, err, tc.want)
			}
		})
		covered[tc.family] = true
	}
	for _, family := range AllFamilies() {
		if !covered[family] {
			t.Errorf("missing independent golden for %s", family)
		}
	}
}

func TestFactKeyScalarIdentityLaws(t *testing.T) {
	for _, tc := range factKeyGoldens[:12] {
		t.Run(tc.name, func(t *testing.T) {
			var fields map[string]string
			if err := json.Unmarshal([]byte(tc.raw), &fields); err != nil {
				t.Fatal(err)
			}
			var field string
			for name := range fields {
				field = name
			}
			for _, bad := range []string{`{}`, `null`, `[]`, `"scalar"`, `{`, tc.raw + `{}`, fmt.Sprintf(`{"%s":null}`, field), fmt.Sprintf(`{"%s":"  "}`, field), fmt.Sprintf(`{"%s":42}`, field)} {
				if got, err := FactKey(tc.family, []byte(bad)); err == nil {
					t.Errorf("invalid primary %s accepted as %q", bad, got)
				}
			}
			for _, spelling := range []string{field, strings.ToUpper(field), `\u` + fmt.Sprintf("%04x", field[0]) + field[1:]} {
				bad := strings.TrimSuffix(tc.raw, "}") + fmt.Sprintf(`,"%s":%q}`, spelling, tc.want)
				if _, err := FactKey(tc.family, []byte(bad)); err == nil {
					t.Errorf("duplicate or alternate-spelling primary accepted: %s", bad)
				}
			}
			for _, id := range []string{"not-a-uuid", "00000000-0000-0000-0000-000000000000", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", " aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa "} {
				raw := fmt.Sprintf(`{"%s":%q}`, field, id)
				got, err := FactKey(tc.family, []byte(raw))
				if tc.family != FamilyReplyContexts && id == "not-a-uuid" {
					if err == nil {
						t.Error("malformed UUID primary accepted")
					}
				} else if err != nil || got != id {
					t.Errorf("primary spelling changed: %q => %q, %v", id, got, err)
				}
			}
		})
	}
	if _, err := FactKey(Family("unknown"), []byte(`{}`)); err == nil {
		t.Fatal("unknown family accepted")
	}
}

func TestFactKeyFanOutInvalidCoordinates(t *testing.T) {
	base := factKeyGoldens[13].raw
	for field, values := range map[string][]any{
		"fact_kind":              {nil, "", "future", "Intent", 1},
		"triggering_delivery_id": {nil, "", "bad", 1},
		"flow_path":              {nil, "", "../other", "a|b", "a//b", " ."},
		"declaration_family":     {nil, "", "fan|out", " fan_out", "FanOut"},
		"semantic_path":          {nil, "", "x\ny", " x", "x\x00y"},
		"ordinal":                {nil, -1, 0.5, "0", true},
	} {
		t.Run(field, func(t *testing.T) {
			for _, value := range values {
				var fields map[string]any
				if err := json.Unmarshal([]byte(base), &fields); err != nil {
					t.Fatal(err)
				}
				fields[field] = value
				raw, err := json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				if key, err := FactKey(FamilyFanOutObligations, raw); err == nil {
					t.Errorf("invalid %s=%v accepted as %q", field, value, key)
				}
				delete(fields, field)
				raw, _ = json.Marshal(fields)
				if _, err := FactKey(FamilyFanOutObligations, raw); err == nil {
					t.Errorf("missing %s accepted", field)
				}
			}
			for _, spelling := range []string{field, strings.ToUpper(field), `\u` + fmt.Sprintf("%04x", field[0]) + field[1:]} {
				raw := strings.TrimSuffix(base, "}") + fmt.Sprintf(`,"%s":null}`, spelling)
				if _, err := FactKey(FamilyFanOutObligations, []byte(raw)); err == nil {
					t.Errorf("duplicate/alternate coordinate accepted: %s", raw)
				}
			}
		})
	}
}

func TestFactKeyDoesNotAdmitNonKeySemantics(t *testing.T) {
	// Key derivation neither invents required cross-family references nor
	// claims to validate their shape, duplicate fields, or owning run.
	for _, tc := range factKeyGoldens {
		raw := strings.TrimSuffix(tc.raw, "}") + `,"run_id":"foreign","source_run_id":"ancestor","source_event_id":null,"payload":{"n":9007199254740993,"x":1,"x":2}}`
		got, err := FactKey(tc.family, []byte(raw))
		if err != nil || got != tc.want {
			t.Errorf("%s non-key fields changed key: %q, %v", tc.name, got, err)
		}
	}
}

func TestCanonicalProjectionConsumesFactKey(t *testing.T) {
	for _, tc := range factKeyGoldens {
		t.Run(tc.name, func(t *testing.T) {
			spec, ok := canonicalProjectionSpec(tc.family)
			if !ok {
				t.Fatal("missing canonical projection")
			}
			if strings.Contains(spec.query, "||") || strings.Contains(spec.query, "SELECT '',") {
				t.Fatal("SQL retains a key expression or placeholder")
			}
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var body map[string]any
			if err := json.Unmarshal([]byte(tc.raw), &body); err != nil {
				t.Fatal(err)
			}
			columns := make([]string, 0, len(spec.columns))
			values := make([]driver.Value, 0, len(spec.columns))
			for _, column := range spec.columns {
				columns = append(columns, column.name)
				values = append(values, body[column.name])
			}
			mock.ExpectQuery(spec.query).WithArgs("selected-run").WillReturnRows(sqlmock.NewRows(columns).AddRow(values...))
			facts, err := loadCanonicalProjection(context.Background(), db, "selected-run", tc.family)
			if err != nil || len(facts) != 1 {
				t.Fatalf("loadCanonicalProjection = %v, %v", facts, err)
			}
			if facts[0].key != tc.want {
				t.Fatalf("writer key = %q, want literal %q", facts[0].key, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCanonicalProjectionRejectsMissingFactKey(t *testing.T) {
	for _, family := range AllFamilies() {
		t.Run(string(family), func(t *testing.T) {
			spec, ok := canonicalProjectionSpec(family)
			if !ok {
				t.Fatal("missing canonical projection")
			}
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			columns := make([]string, 0, len(spec.columns))
			values := make([]driver.Value, 0, len(spec.columns))
			for _, column := range spec.columns {
				columns = append(columns, column.name)
				values = append(values, nil)
			}
			mock.ExpectQuery(spec.query).WithArgs("selected-run").WillReturnRows(sqlmock.NewRows(columns).AddRow(values...))
			facts, err := loadCanonicalProjection(context.Background(), db, "selected-run", family)
			if err == nil || len(facts) != 0 {
				t.Fatalf("missing body identity accepted: %v, %v", facts, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
