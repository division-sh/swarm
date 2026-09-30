package releasee2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
	"gopkg.in/yaml.v3"
)

type numericFeedExpectations struct {
	Conformance struct {
		Disposition string   `yaml:"disposition"`
		Verify      string   `yaml:"verify"`
		Proves      []string `yaml:"proves"`
	} `yaml:"conformance"`
	Feed struct {
		Dataset             string   `yaml:"dataset"`
		SHA256              string   `yaml:"sha256"`
		Rows                int      `yaml:"rows"`
		Event               string   `yaml:"event"`
		ReceiverFlow        string   `yaml:"receiver_flow"`
		ReceiverNode        string   `yaml:"receiver_node"`
		ReceiverType        string   `yaml:"receiver_type"`
		ReceiverStage       string   `yaml:"receiver_stage"`
		StateFields         []string `yaml:"state_fields"`
		OptionalEventFields []string `yaml:"optional_event_fields"`
		TimerAfter          string   `yaml:"timer_after"`
		DeadLetters         *int     `yaml:"dead_letters"`
	} `yaml:"numeric_feed"`
}

func decodeNumericFeedExpectations(body []byte) (numericFeedExpectations, error) {
	var expected numericFeedExpectations
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	if err := decoder.Decode(&expected); err != nil {
		return expected, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return expected, fmt.Errorf("expected exactly one assertion document: %v", err)
	}
	f := expected.Feed
	if expected.Conformance.Disposition != "verify-only" || expected.Conformance.Verify != "pass" || !reflect.DeepEqual(expected.Conformance.Proves, []string{"catalog.authoring.numeric_feed_fixture"}) {
		return expected, fmt.Errorf("numeric source requires its exact structural claim")
	}
	if f.Dataset != "data/items.jsonl" || len(f.SHA256) != 64 || f.Rows != 100 || f.Event != "item.registered" ||
		f.ReceiverFlow != "registry" || f.ReceiverNode != "registry-intake" || f.ReceiverType != "item" || f.ReceiverStage != "registered" ||
		f.TimerAfter != "2160h" || f.DeadLetters == nil || *f.DeadLetters != 0 ||
		!reflect.DeepEqual(f.StateFields, []string{"batch_id", "item_id", "label", "region", "source_ref", "openings", "tags", "origin", "score", "qualified"}) ||
		!reflect.DeepEqual(f.OptionalEventFields, []string{"note"}) {
		return expected, fmt.Errorf("missing or unsupported numeric-feed assertion")
	}
	return expected, nil
}

func loadNumericFeedCorpus(t *testing.T, source string) (numericFeedExpectations, map[string]map[string]any) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(source, "tests/expected.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := decodeNumericFeedExpectations(body)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(source, expected.Feed.Dataset))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != expected.Feed.SHA256 {
		t.Fatal("numeric corpus bytes differ from the handover checksum")
	}
	rows := map[string]map[string]any{}
	nulls := 0
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var row map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		key, _ := row["item_id"].(string)
		if key == "" || rows[key] != nil {
			t.Fatalf("missing or duplicate item key %q", key)
		}
		openings, integer := row["openings"].(float64)
		_, numeric := row["score"].(float64)
		if !integer || openings != math.Trunc(openings) || !numeric {
			t.Fatalf("source row %s lost real integer/numeric input: %+v", key, row)
		}
		if note, present := row["note"]; present && note == nil {
			delete(row, "note")
			nulls++
		}
		rows[key] = row
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != expected.Feed.Rows || nulls != 48 {
		t.Fatalf("numeric source cardinality=%d optional nulls=%d", len(rows), nulls)
	}
	return expected, rows
}

// Public event and entity readers are independent, exhaustively paginated
// projections. Join their exact identities; counts or loose membership cannot
// prove that the right row was delivered to the right isolated receiver.
func assertNumericFeedPublic(t *testing.T, ctx context.Context, rpc *releaseRPCClient, runID string, expected numericFeedExpectations, rows map[string]map[string]any) map[string]goldenEntitySummary {
	t.Helper()
	entities := listGoldenEntities(t, ctx, rpc, runID)
	if len(entities) != expected.Feed.Rows {
		t.Fatalf("numeric entities=%d, want %d", len(entities), expected.Feed.Rows)
	}
	byID := map[string]goldenEntitySummary{}
	byKey := map[string]goldenEntitySummary{}
	paths := map[string]bool{}
	for _, entity := range entities {
		if entity.EntityType != expected.Feed.ReceiverType || entity.CurrentState != expected.Feed.ReceiverStage || entity.FlowInstance == "" || paths[entity.FlowInstance] {
			t.Fatalf("wrong or shared numeric receiver: %+v", entity)
		}
		var detail struct {
			Fields map[string]any `json:"fields"`
		}
		if err := rpc.call(ctx, "entity.get", map[string]any{"entity_id": entity.EntityID, "run_id": runID}, &detail); err != nil {
			t.Fatal(err)
		}
		key, _ := detail.Fields["item_id"].(string)
		row := rows[key]
		if row == nil || byKey[key].EntityID != "" {
			t.Fatalf("unknown or duplicate numeric receiver key=%q entity=%+v", key, entity)
		}
		for _, field := range expected.Feed.StateFields {
			if !reflect.DeepEqual(detail.Fields[field], row[field]) {
				t.Fatalf("numeric receiver %s field %s=%#v, want %#v", key, field, detail.Fields[field], row[field])
			}
		}
		if _, leaked := detail.Fields["note"]; leaked {
			t.Fatalf("event-only note leaked into state for %s", key)
		}
		byID[entity.EntityID], byKey[key], paths[entity.FlowInstance] = entity, entity, true
	}
	observed := map[string]bool{}
	receiver, err := identity.ParseExecutableNode(expected.Feed.ReceiverFlow, expected.Feed.ReceiverNode)
	if err != nil {
		t.Fatal(err)
	}
	events, err := listGoldenEvents(ctx, rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if len(event.DeadLetters) != *expected.Feed.DeadLetters {
			t.Fatalf("numeric dead letter: %+v", event)
		}
		if event.EventName != expected.Feed.Event {
			if event.EventName == "import.requested" || event.EventName == "items.batch_imported" {
				t.Fatalf("numeric feed used the retired import bridge: %+v", event)
			}
			continue
		}
		key, _ := event.Payload["item_id"].(string)
		if rows[key] == nil || observed[key] || !reflect.DeepEqual(event.Payload, rows[key]) || len(event.Deliveries) != 1 {
			t.Fatalf("numeric event lost exact row %s: %+v want=%+v", key, event, rows[key])
		}
		delivery := event.Deliveries[0]
		entity, exists := byID[delivery.Target.EntityID]
		if !exists || entity != byKey[key] || delivery.SubscriberType != "node" || delivery.SubscriberID != receiver.Key() ||
			delivery.Target.Kind != "materializing_entity" || delivery.Target.FlowID != expected.Feed.ReceiverFlow || delivery.Target.FlowInstance != entity.FlowInstance || delivery.Status != "delivered" || !delivery.Terminal {
			t.Fatalf("numeric row %s delivered to wrong receiver: %+v entity=%+v", key, delivery, entity)
		}
		observed[key] = true
	}
	if len(observed) != len(rows) {
		t.Fatalf("numeric row chain has %d/%d exact members", len(observed), len(rows))
	}
	return byKey
}

func TestNumericFeedAssertionsRejectMissingAndUnknownFields(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(releaseE2ERepoRoot(t), numericScatterSource, "tests/expected.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeNumericFeedExpectations(body); err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]string{
		"unknown":              string(body) + "  ignored_assertion: true\n",
		"missing_rows":         strings.ReplaceAll(string(body), "  rows: 100\n", ""),
		"missing_dead_letters": strings.ReplaceAll(string(body), "  dead_letters: 0\n", ""),
		"wrong_target":         strings.ReplaceAll(string(body), "receiver_flow: registry", "receiver_flow: investigator"),
		"wrong_fields":         strings.ReplaceAll(string(body), ", score", ""),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeNumericFeedExpectations([]byte(bad)); err == nil {
				t.Fatal("incomplete assertion contract was accepted")
			}
		})
	}
}

// Timer rows have no public list API. Inspect through the canonical typed store
// only after the child has stopped and joined, then close this handle before
// starting the successor. This is explicitly supplementary persistence credit.
func inspectNumericFeedTimers(t *testing.T, ctx context.Context, root string, selected goldenStoreSelection, runID string, expected numericFeedExpectations, entities map[string]goldenEntitySummary) map[string]pipeline.WorkflowTimerActivation {
	t.Helper()
	var db *sql.DB
	var reader pipeline.WorkflowTimerActivationPersistence
	if selected.name == "sqlite" {
		var err error
		db, err = sql.Open("sqlite", filepath.Join(root, "runtime.db"))
		if err != nil {
			t.Fatal(err)
		}
		reader = storetest.AdmitSQLiteRuntimeStore(t, db)
	} else {
		if selected.inspectionConnector == nil {
			t.Fatal("exact isolated PostgreSQL inspection connector is missing")
		}
		db = sql.OpenDB(selected.inspectionConnector)
		reader = storetest.AdmitPostgresRuntimeStore(t, db)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	activations, err := reader.ListWorkflowTimerActivations(ctx, runID, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(activations) != len(entities) {
		t.Fatalf("typed numeric timer inventory=%d, want %d", len(activations), len(entities))
	}
	after, err := time.ParseDuration(expected.Feed.TimerAfter)
	if err != nil {
		t.Fatal(err)
	}
	byEntity := map[string]pipeline.WorkflowTimerActivation{}
	for _, activation := range activations {
		if err := activation.Validate(); err != nil {
			t.Fatal(err)
		}
		if activation.RunID != runID || activation.Status != "active" || activation.Recurring || activation.Ref.Cause != timeridentity.WorkflowTimerActivationCauseInitial ||
			activation.FireAt.Sub(activation.CreatedAt) != after || activation.Route.ScopeKey != expected.Feed.ReceiverFlow || byEntity[activation.EntityID].EntityID != "" {
			t.Fatalf("wrong initial numeric timer: %+v", activation)
		}
		byEntity[activation.EntityID] = activation
	}
	for key, entity := range entities {
		activation, exists := byEntity[entity.EntityID]
		if !exists || activation.Route.InstancePath != entity.FlowInstance {
			t.Fatalf("item %s lost exact timer/receiver ownership: entity=%+v timer=%+v", key, entity, activation)
		}
	}
	return byEntity
}

func readNumericCreationReceipt(t *testing.T, ctx context.Context, rpc *releaseRPCClient, runID string) map[string]json.RawMessage {
	t.Helper()
	result := map[string]json.RawMessage{}
	for _, detail := range []string{"summary", "request_binding", "run_binding"} {
		var record json.RawMessage
		params := map[string]any{
			"view": "operation", "detail": detail,
			"operation_ref": map[string]any{"kind": "run_creation", "run_id": runID},
		}
		if detail == "run_binding" {
			params["page"] = map[string]any{"limit": 10}
		}
		if err := rpc.call(ctx, "data.show", params, &record); err != nil {
			t.Fatal(err)
		}
		if len(record) == 0 || bytes.Equal(record, []byte("null")) {
			t.Fatalf("missing permanent %s creation evidence", detail)
		}
		result[detail] = record
	}
	return result
}
