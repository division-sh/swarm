package store

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestListPendingAgentDeliveryDetailsFailsClosedOnNonCanonicalCapabilities(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	pg := &PostgresStore{DB: db}
	pg.schemaCaps = StoreSchemaCapabilities{
		Events: EventSchemaCapabilities{
			Log:        SchemaFlavorCanonical,
			Deliveries: SchemaFlavorCanonical,
			Receipts:   SchemaFlavorLegacy,
		},
	}
	pg.schemaCapsBound = true

	page, err := pg.ListPendingAgentDeliveryDetails(context.Background(), PendingAgentDeliveryListOptions{AgentID: "agent-1"})
	if err == nil || !strings.Contains(err.Error(), "event_receipts schema is unsupported") {
		t.Fatalf("ListPendingAgentDeliveryDetails err=%v page=%+v, want event_receipts capability failure", err, page)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
