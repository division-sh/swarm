package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestHandleGraph_Holding(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	now := time.Date(2026, 2, 14, 12, 0, 0, 0, time.UTC)
	s := NewServer(db, nil, nil, nil, nil)
	s.now = func() time.Time { return now }

	cfg := `{"system_prompt":"Holding prompt","tools":["sql_execute"],"subscriptions":["system.started","opco.*.steady_state_reached"],"constraints":{"x":1}}`
	rows := sqlmock.NewRows([]string{"id", "role", "mode", "status", "parent_agent_id", "config"}).
		AddRow("empire-coordinator", "empire-coordinator", "holding", "active", "", []byte(cfg)).
		AddRow("operations-analyst", "operations-analyst", "holding", "active", "empire-coordinator", []byte(`{"system_prompt":"OA"}`))

	mock.ExpectQuery(`(?s)SELECT.*FROM agents a.*vertical_id IS NULL.*ORDER BY a\.id ASC`).
		WillReturnRows(rows)

	r := httptest.NewRequest(http.MethodGet, "/api/graph?mode=holding", nil)
	w := httptest.NewRecorder()
	s.handleGraph(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Nodes []map[string]any `json:"nodes"`
		Edges []map[string]any `json:"edges"`
		Mode  string           `json:"mode"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Mode != "holding" {
		t.Fatalf("expected mode holding, got %q", resp.Mode)
	}
	if len(resp.Nodes) < 3 { // 2 agents + at least one evt node from subscriptions
		t.Fatalf("expected nodes >= 3, got %d", len(resp.Nodes))
	}

	// Ensure system_prompt is present for agent nodes and subscription edges exist.
	foundPrompt := false
	for _, n := range resp.Nodes {
		if n["kind"] == "agent" && n["id"] == "empire-coordinator" {
			if sp, _ := n["system_prompt"].(string); strings.TrimSpace(sp) == "Holding prompt" {
				foundPrompt = true
			}
		}
	}
	if !foundPrompt {
		t.Fatalf("expected holding agent system_prompt in response")
	}
	foundProducer := false
	foundMessage := false
	foundMailbox := false
	foundHumanNode := false
	for _, e := range resp.Edges {
		switch e["kind"] {
		case "producer":
			foundProducer = true
		case "message":
			foundMessage = true
		case "mailbox":
			foundMailbox = true
		}
	}
	for _, n := range resp.Nodes {
		if n["id"] == "sys:human-board" {
			foundHumanNode = true
			break
		}
	}
	if !foundProducer {
		t.Fatalf("expected producer edges from communication graph registry")
	}
	if !foundMessage {
		t.Fatalf("expected message authority edges")
	}
	if !foundMailbox {
		t.Fatalf("expected mailbox round-trip edges")
	}
	if !foundHumanNode {
		t.Fatalf("expected human board node")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestHandleGraph_Template(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := NewServer(db, nil, nil, nil, nil)

	agents := `[{"role":"opco-ceo","parent_role":"","type":"sonnet","system_prompt":"CEO {mandate_document}","tools":["agent_hire"],"subscriptions":["opco.*"]}]`
	bootstrap := `[{"event_pattern":"opco.*","subscriber_role":"opco-ceo","subscriber_id":"","reason":"bootstrap"}]`
	seeded := `[]`
	mock.ExpectQuery(`(?s)SELECT version, COALESCE\(agents.*FROM org_templates.*ORDER BY created_at DESC.*LIMIT 1`).
		WillReturnRows(sqlmock.NewRows([]string{"version", "agents", "bootstrap_routes", "seeded_routes"}).
			AddRow("2.0", []byte(agents), []byte(bootstrap), []byte(seeded)))

	r := httptest.NewRequest(http.MethodGet, "/api/graph?mode=template", nil)
	w := httptest.NewRecorder()
	s.handleGraph(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Nodes []map[string]any `json:"nodes"`
		Edges []map[string]any `json:"edges"`
		Mode  string           `json:"mode"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Mode != "template" {
		t.Fatalf("expected mode template, got %q", resp.Mode)
	}
	if len(resp.Nodes) < 2 { // agent + evt node
		t.Fatalf("expected nodes >= 2, got %d", len(resp.Nodes))
	}
	foundCEO := false
	for _, n := range resp.Nodes {
		if n["kind"] == "agent" && n["id"] == "opco-ceo" {
			foundCEO = true
			if sp, _ := n["system_prompt"].(string); !strings.Contains(sp, "CEO") {
				t.Fatalf("unexpected template system_prompt: %q", sp)
			}
		}
	}
	if !foundCEO {
		t.Fatalf("expected template agent node for opco-ceo")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestHandleGraph_OpCo(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := NewServer(db, nil, nil, nil, nil)

	// Resolve vertical
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT id::text, COALESCE(slug,''), COALESCE(name,''), COALESCE(geography,''), COALESCE(template_version,'')
		FROM verticals
		WHERE id::text = $1 OR COALESCE(slug,'') = $1
		LIMIT 1
	`)).
		WithArgs("v-test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "slug", "name", "geography", "template_version"}).
			AddRow("v-test", "test", "TestCo", "US", "2.0"))

	// Agents in vertical
	mock.ExpectQuery(`(?s)SELECT.*FROM agents a.*COALESCE\(a\.vertical_id::text,''\) = \$1.*ORDER BY a\.id ASC`).
		WithArgs("v-test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "role", "mode", "status", "parent_agent_id", "config"}).
			AddRow("opco-ceo-v-test", "opco-ceo", "operating", "active", "", []byte(`{"system_prompt":"Run the company","tools":["configure_routing"],"subscriptions":["opco.*"]}`)).
			AddRow("vp-product-v-test", "vp-product", "operating", "active", "opco-ceo-v-test", []byte(`{"system_prompt":"Build product"}`)))

	// Routing rules
	mock.ExpectQuery(`(?s)SELECT.*FROM routing_rules.*WHERE vertical_id = \$1::uuid.*ORDER BY created_at ASC`).
		WithArgs("v-test").
		WillReturnRows(sqlmock.NewRows([]string{"event_pattern", "subscriber_id", "status", "source", "reason"}).
			AddRow("opco.*", "opco-ceo-v-test", "active", "bootstrap", "bootstrap"))

	r := httptest.NewRequest(http.MethodGet, "/api/graph?mode=opco&vertical=v-test", nil)
	w := httptest.NewRecorder()
	s.handleGraph(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Nodes []map[string]any `json:"nodes"`
		Edges []map[string]any `json:"edges"`
		Mode  string           `json:"mode"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Mode != "opco" {
		t.Fatalf("expected mode opco, got %q", resp.Mode)
	}

	foundRouting := false
	for _, e := range resp.Edges {
		if e["kind"] == "routing" && e["source"] == "bootstrap" {
			foundRouting = true
		}
	}
	if !foundRouting {
		t.Fatalf("expected bootstrap routing edge in response")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
