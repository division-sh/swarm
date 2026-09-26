package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Frozen pre-batch validator. Do not delegate its checks to the production
// validator: error order and canonical admission are part of this oracle.
func publicationGroupMemberBefore(ctx context.Context, tx pipelineQueryer, g *publicationGroup, member *publicationGroupMember) error {
	if !g.sealed || member.ordinal >= g.end {
		return errors.New("publication group lacks exact sealed attempt evidence")
	}
	key := g.claim.Key
	var kind, eventID string
	query := `SELECT outcome_kind,COALESCE(event_id,'') FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND ordinal=$6`
	if g.postgres != nil {
		query = `SELECT outcome_kind,COALESCE(event_id::text,'') FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND ordinal=$6`
	}
	err := tx.QueryRowContext(ctx, query, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath, member.ordinal).Scan(&kind, &eventID)
	if err != nil {
		return err
	}
	if kind != string(fanoutobligation.OutcomeCommitted) || eventID != member.event.ID() {
		return errors.New("publication group durable ordinal identity conflicts")
	}
	var admitted events.AdmittedEvent
	var found bool
	if g.postgres != nil {
		admitted, _, found, err = eventrecordpostgres.LoadAdmitted(ctx, tx, eventID)
	} else {
		admitted, _, found, err = eventrecordsqlite.LoadAdmitted(ctx, tx, eventID)
	}
	if err != nil {
		return err
	}
	if !found || admitted.Event().RunID() == "" {
		return errors.New("publication group committed event is absent or corrupt")
	}
	want, err := events.IntegrityProjection(member.event)
	if err != nil {
		return err
	}
	actual, err := events.IntegrityProjection(admitted.Event())
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, actual) {
		return errors.New("publication group committed event changed")
	}
	return nil
}

type publicationBatchQueryCount struct {
	pipelineQueryer
	outcomes, events, rows, lists, maxArgs int
	beforeRow                              func()
}

func (q *publicationBatchQueryCount) count(query string, args []any) {
	q.maxArgs = max(q.maxArgs, len(args))
	if strings.Contains(query, "FROM fan_out_outcomes") {
		q.outcomes++
	}
	if strings.Contains(query, "FROM events e") {
		q.events++
	}
}

func (q *publicationBatchQueryCount) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.lists++
	q.count(query, args)
	return q.pipelineQueryer.QueryContext(ctx, query, args...)
}

func (q *publicationBatchQueryCount) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if q.beforeRow != nil {
		before := q.beforeRow
		q.beforeRow = nil
		before()
	}
	q.rows++
	q.count(query, args)
	return q.pipelineQueryer.QueryRowContext(ctx, query, args...)
}

func publicationBatchFixture(t *testing.T, postgres bool) (*sql.DB, *publicationGroup, []*publicationGroupMember) {
	t.Helper()
	var db *sql.DB
	if postgres {
		_, db, _ = testutil.StartEmptyPostgres(t)
	} else {
		var err error
		db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "publication.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	db.SetMaxOpenConns(1)
	columns := []string{}
	for _, name := range strings.Fields("event_class event_name task_id flow_instance scope payload_schema_bundle_hash payload_schema_flow_id payload_schema_event_key payload_schema_digest payload_schema_class execution_mode produced_by produced_by_type routing_source_kind routing_source_authority") {
		columns = append(columns, name+" TEXT")
	}
	idType, jsonType, bytesType, timeType := "TEXT", "TEXT", "BLOB", "TIMESTAMP"
	if postgres {
		idType, jsonType, bytesType, timeType = "UUID", "JSONB", "BYTEA", "TIMESTAMPTZ"
	}
	for _, name := range strings.Fields("event_id run_id entity_id source_event_id operator_reference_event_id") {
		columns = append(columns, name+" "+idType)
	}
	for _, name := range strings.Fields("payload source_route target_route target_set route_settlement inherited_fan_out_origin") {
		columns = append(columns, name+" "+jsonType)
	}
	columns = append(columns, "payload_bytes "+bytesType, "chain_depth INTEGER", "created_at "+timeType, "UNIQUE(event_id)")
	coords := "run_id TEXT, deployment_feed_id TEXT, triggering_delivery_id TEXT, flow_path TEXT, declaration_family TEXT, semantic_path TEXT"
	for _, ddl := range []string{
		"CREATE TABLE events (" + strings.Join(columns, ",") + ")",
		"CREATE TABLE run_fork_selected_contract_executions (fork_event_id " + idType + ", source_run_id " + idType + ", source_event_id " + idType + ", selection_authority TEXT)",
		"CREATE TABLE run_fork_delivery_event_replays (fork_event_id " + idType + ", source_run_id " + idType + ", source_event_id " + idType + ", selection_authority TEXT)",
		"CREATE TABLE fan_out_outcomes (" + coords + ", ordinal INTEGER, outcome_kind TEXT, event_id " + idType + ")",
		"CREATE UNIQUE INDEX handler_outcome_key ON fan_out_outcomes(run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal) WHERE deployment_feed_id IS NULL",
		"CREATE UNIQUE INDEX deployment_outcome_key ON fan_out_outcomes(run_id,deployment_feed_id,ordinal) WHERE deployment_feed_id IS NOT NULL",
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	g := &publicationGroup{sealed: true, end: fanoutobligation.MaxChunkSize}
	g.claim.Key = foldStatementKey()
	if postgres {
		g.postgres = &PipelinePostgresOwner{}
	}
	ctx := context.Background()
	var members []*publicationGroupMember
	for ordinal := range fanoutobligation.MaxChunkSize {
		event := eventtest.RunCreatingRootIngress(uuid.NewString(), "record.valid", "gateway", "task-1", []byte(`{"n":1}`), 0, g.claim.Key.RunID, "", events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond))
		event, err := eventtest.AdmitPayload(event, "", "record.valid")
		if err != nil {
			t.Fatal(err)
		}
		admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
		if err != nil {
			t.Fatal(err)
		}
		ledger, err := events.NewConnectEvaluationLedger(nil)
		if err != nil {
			t.Fatal(err)
		}
		settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
		if err != nil {
			t.Fatal(err)
		}
		record, err := eventrecord.FromAdmitted(admitted, settlement)
		if err != nil {
			t.Fatal(err)
		}
		if err := insertPublicationBatchEventFixture(ctx, db, record); err != nil {
			t.Fatal(err)
		}
		key := g.claim.Key
		if _, err := db.Exec(`INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,event_id) VALUES ($1,$2,$3,$4,$5,$6,'committed',$7)`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath, ordinal, event.ID()); err != nil {
			t.Fatal(err)
		}
		members = append(members, &publicationGroupMember{ordinal: ordinal, event: event})
	}
	return db, g, members
}

func TestPublicationOutcomeSelectionKeepsDeploymentAndHandlerOriginsDistinct(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			db, group, _ := publicationBatchFixture(t, postgres)
			feedID := uuid.NewString()
			if _, err := db.Exec(`INSERT INTO fan_out_outcomes (run_id,deployment_feed_id,ordinal,outcome_kind,event_id) VALUES ($1,$2,0,'committed',$3)`, group.claim.Key.RunID, feedID, uuid.NewString()); err != nil {
				t.Fatal(err)
			}
			for _, key := range []fanoutobligation.IntentKey{
				group.claim.Key,
				{RunID: group.claim.Key.RunID, DeploymentFeedID: feedID},
			} {
				where, args, err := fanOutOutcomeKeySelection(key)
				if err != nil {
					t.Fatal(err)
				}
				args = append(args, 0)
				var count int
				if err := db.QueryRow(`SELECT COUNT(*) FROM fan_out_outcomes WHERE `+where+fmt.Sprintf(` AND ordinal=$%d`, len(args)), args...).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("origin selection returned %d rows for %#v", count, key)
				}
			}
		})
	}
}

func insertPublicationBatchEventFixture(ctx context.Context, db *sql.DB, record eventrecord.Record) error {
	jsonValue := func(raw []byte) any {
		if len(raw) == 0 {
			return nil
		}
		return string(raw)
	}
	_, err := db.ExecContext(ctx, `INSERT INTO events (
		event_class,event_id,run_id,event_name,task_id,entity_id,flow_instance,scope,payload,payload_bytes,
		payload_schema_bundle_hash,payload_schema_flow_id,payload_schema_event_key,payload_schema_digest,payload_schema_class,
		execution_mode,chain_depth,produced_by,produced_by_type,source_event_id,created_at,
		routing_source_kind,routing_source_authority,source_route,target_route,target_set,route_settlement,
		operator_reference_event_id,inherited_fan_out_origin
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)`,
		record.Class, record.EventID, nullableText(record.RunID), record.EventName, nullableText(record.TaskID), nullableText(record.EntityID), nullableText(record.FlowInstance), record.Scope,
		jsonValue(record.Payload), record.Payload, record.PayloadSchemaBundleHash, nullableText(record.PayloadSchemaFlowID), record.PayloadSchemaEventKey, record.PayloadSchemaDigest, record.PayloadSchemaClass,
		record.ExecutionMode, record.ChainDepth, record.ProducedBy, record.ProducedByType, nullableText(record.SourceEventID), record.CreatedAt,
		record.RoutingSourceKind, nullableText(record.RoutingSourceAuthority), jsonValue(record.SourceRoute), jsonValue(record.TargetRoute), jsonValue(record.TargetSet), jsonValue(record.RouteSettlement),
		nullableText(record.OperatorReferencedEventID), jsonValue(record.InheritedFanOutOrigin))
	return err
}

func TestPublicationGroupBatchNativeDifferentialBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, g, members := publicationBatchFixture(t, backend == "postgres")
			ctx := context.Background()
			before := func(q pipelineQueryer, selected []*publicationGroupMember) error {
				for _, member := range selected {
					if err := publicationGroupMemberBefore(ctx, q, g, member); err != nil {
						return err
					}
				}
				return nil
			}
			for _, size := range []int{0, 1, 3, fanoutobligation.MaxChunkSize} {
				t.Run(fmt.Sprintf("count_%d", size), func(t *testing.T) {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					selected := append([]*publicationGroupMember(nil), members[:size]...)
					for i, j := 0, size-1; i < j; i, j = i+1, j-1 {
						selected[i], selected[j] = selected[j], selected[i]
					}
					oldQ := &publicationBatchQueryCount{pipelineQueryer: tx}
					newQ := &publicationBatchQueryCount{pipelineQueryer: tx}
					if err := before(oldQ, selected); err != nil {
						t.Fatal(err)
					}
					if err := g.validateCommittedMembersTx(ctx, newQ, selected); err != nil {
						t.Fatal(err)
					}
					batches := 0
					if size != 0 {
						batches = 1
					}
					rowQueries, listQueries := 0, 2*batches
					if size == 1 {
						rowQueries, listQueries = 2, 0
					}
					if oldQ.rows != 2*size || oldQ.lists != 0 || oldQ.outcomes != size || oldQ.events != size || newQ.rows != rowQueries || newQ.lists != listQueries || newQ.outcomes != batches || newQ.events != batches || newQ.maxArgs > 5+fanoutobligation.MaxChunkSize {
						t.Fatalf("physical query counts: before=%+v after=%+v", oldQ, newQ)
					}
					t.Logf("members=%d frozen_queries=%d batched_queries=%d outcome_queries=%d event_queries=%d max_args=%d", size, oldQ.rows, newQ.rows+newQ.lists, newQ.outcomes, newQ.events, newQ.maxArgs)
				})
			}
			cases := []struct {
				name, query, want string
				args              []any
			}{
				{"missing_outcome", `DELETE FROM fan_out_outcomes WHERE ordinal=2`, sql.ErrNoRows.Error(), nil},
				{"wrong_kind", `UPDATE fan_out_outcomes SET outcome_kind='semantic_rejected' WHERE ordinal=2`, "durable ordinal identity conflicts", nil},
				{"wrong_event", `UPDATE fan_out_outcomes SET event_id=$1 WHERE ordinal=2`, "durable ordinal identity conflicts", []any{members[0].event.ID()}},
				{"null_kind", `UPDATE fan_out_outcomes SET outcome_kind=NULL WHERE ordinal=2`, "converting NULL", nil},
				{"missing_event", `DELETE FROM events WHERE event_id=$1`, "committed event is absent or corrupt", []any{members[2].event.ID()}},
				{"corrupt_settlement", `UPDATE events SET route_settlement='{}' WHERE event_id=$1`, "event record corrupt", []any{members[2].event.ID()}},
				{"corrupt_payload", `UPDATE events SET payload_bytes=$1 WHERE event_id=$2`, "event record corrupt", []any{[]byte(`{"n":null}`), members[2].event.ID()}},
				{"changed_payload", `UPDATE events SET payload_bytes=$1 WHERE event_id=$2`, "committed event changed", []any{[]byte(`{"n":2}`), members[2].event.ID()}},
				{"lineage_owner", `INSERT INTO run_fork_selected_contract_executions VALUES ($1,$2,$3,'fixture')`, "event record corrupt", []any{members[2].event.ID(), uuid.NewString(), uuid.NewString()}},
				{"unselected_corrupt", `UPDATE events SET route_settlement='{}' WHERE event_id=$1`, "", []any{members[3].event.ID()}},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					selected := []*publicationGroupMember{members[2], members[0]}
					if err := g.validateCommittedMembersTx(ctx, tx, selected); err != nil {
						t.Fatal(err)
					}
					if _, err := tx.ExecContext(ctx, tc.query, tc.args...); err != nil {
						t.Fatal(err)
					}
					want, got := before(tx, selected), g.validateCommittedMembersTx(ctx, tx, selected)
					if fmt.Sprint(want) != fmt.Sprint(got) || reflect.TypeOf(want) != reflect.TypeOf(got) || errors.Is(want, sql.ErrNoRows) != errors.Is(got, sql.ErrNoRows) || errors.Is(want, eventrecord.ErrCorrupt) != errors.Is(got, eventrecord.ErrCorrupt) {
						t.Fatalf("frozen=%T %v; batch=%T %v", want, want, got, got)
					}
					if tc.want == "" && got != nil || tc.want != "" && (got == nil || !strings.Contains(got.Error(), tc.want)) {
						t.Fatalf("second read=%v want %q", got, tc.want)
					}
					var oldCorrupt, newCorrupt *eventrecord.CorruptError
					if errors.As(want, &oldCorrupt) && (!errors.As(got, &newCorrupt) || oldCorrupt.EventID != newCorrupt.EventID) {
						t.Fatalf("canonical corrupt identity changed: %v", got)
					}
				})
			}
			t.Run("member_error_precedence", func(t *testing.T) {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.Exec(`UPDATE events SET payload_bytes=$1 WHERE event_id=$2`, []byte(`{"n":2}`), members[0].event.ID()); err != nil {
					t.Fatal(err)
				}
				for _, mutation := range []string{
					`UPDATE events SET route_settlement='{}' WHERE event_id=$1`,
					`DELETE FROM fan_out_outcomes WHERE event_id=$1`,
				} {
					if _, err := tx.Exec(mutation, members[1].event.ID()); err != nil {
						t.Fatal(err)
					}
					want := before(tx, members[:2])
					got := g.validateCommittedMembersTx(ctx, tx, members[:2])
					if want == nil || want.Error() != "publication group committed event changed" || fmt.Sprint(want) != fmt.Sprint(got) || reflect.TypeOf(want) != reflect.TypeOf(got) {
						t.Fatalf("later batch error displaced earlier member: frozen=%v batch=%v", want, got)
					}
				}
			})
			t.Run("fallback_cannot_rescue_authority", func(t *testing.T) {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.Exec(`UPDATE events SET payload_bytes=$1 WHERE event_id=$2`, []byte(`{"n":2}`), members[1].event.ID()); err != nil {
					t.Fatal(err)
				}
				q := &publicationBatchQueryCount{pipelineQueryer: tx, beforeRow: func() {
					if _, err := tx.Exec(`UPDATE events SET payload_bytes=$1 WHERE event_id=$2`, []byte(`{"n":1}`), members[1].event.ID()); err != nil {
						t.Fatal(err)
					}
				}}
				if err := g.validateCommittedMembersTx(ctx, q, members[:2]); err == nil || err.Error() != "publication group committed event changed" || q.rows != 4 {
					t.Fatalf("successful individual rereads rescued failed batch: err=%v rereads=%d", err, q.rows)
				}
				if err := before(tx, members[:2]); err != nil {
					t.Fatalf("repair must actually be valid for the fail-closed proof: %v", err)
				}
			})
			t.Run("exact_coordinates_and_sparse_membership", func(t *testing.T) {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				key := g.claim.Key
				coords := []any{key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath}
				for i := range coords {
					other := append([]any{}, coords...)
					other[i] = uuid.NewString()
					if _, err := tx.Exec(`INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,event_id) VALUES ($1,$2,$3,$4,$5,0,NULL,NULL)`, other...); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := tx.Exec(`UPDATE fan_out_outcomes SET outcome_kind=NULL WHERE ordinal=1`); err != nil {
					t.Fatal(err)
				}
				selected := []*publicationGroupMember{members[2], members[0]}
				q := &publicationBatchQueryCount{pipelineQueryer: tx}
				if err := g.validateCommittedMembersTx(ctx, q, selected); err != nil || q.rows != 0 || q.lists != 2 {
					t.Fatalf("foreign key or unselected ordinal influenced exact batch: %v %+v", err, q)
				}
			})
			t.Run("bound_before_query", func(t *testing.T) {
				q := &publicationBatchQueryCount{pipelineQueryer: db}
				if err := g.validateCommittedMembersTx(ctx, q, append(append([]*publicationGroupMember{}, members...), members[0])); err == nil || q.rows+q.lists != 0 {
					t.Fatalf("oversized input queried or admitted: %v %+v", err, q)
				}
			})
		})
	}
}
