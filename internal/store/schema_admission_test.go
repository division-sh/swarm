package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
)

const unacceptedAdmissionEventID = "11111111-1111-4111-8111-111111111111"

func TestUnacceptedSelectedStoreRuntimeCallsFailBeforeSQL(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			var call func() error
			if backend == "postgres" {
				store := &PostgresStore{DB: db}
				call = func() error {
					_, err := store.ListActiveAgentDescriptors(context.Background())
					return err
				}
			} else {
				store := &SQLiteRuntimeStore{SQLiteSchemaStore: &SQLiteSchemaStore{DB: db}}
				call = func() error {
					_, err := store.ListActiveAgentDescriptors(context.Background())
					return err
				}
			}

			err = call()
			if err == nil || !strings.Contains(err.Error(), "schema is unaccepted") {
				t.Fatalf("runtime call error = %v, want unaccepted admission failure", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unaccepted runtime call issued SQL: %v", err)
			}
		})
	}
}

func TestUnacceptedSelectedStoreEventMutationBoundariesFailBeforeSQL(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			callbackCalled := false
			var calls []struct {
				name string
				call func() error
			}
			if backend == "postgres" {
				store := &PostgresStore{DB: db}
				calls = []struct {
					name string
					call func() error
				}{
					{name: "begin_event_transaction", call: func() error { _, err := store.BeginEventTx(context.Background()); return err }},
					{name: "run_event_transaction", call: func() error {
						return store.RunEventTransaction(context.Background(), func(context.Context, *sql.Tx) error {
							callbackCalled = true
							return nil
						})
					}},
					{name: "insert_delivery_targets", call: func() error {
						return store.InsertEventDeliveriesWithTargets(context.Background(), unacceptedAdmissionEventID, []string{"agent-a"}, nil)
					}},
					{name: "insert_delivery_routes", call: func() error {
						return store.InsertEventDeliveryRoutes(context.Background(), unacceptedAdmissionEventID, unacceptedAdmissionRoutes())
					}},
				}
			} else {
				store := &SQLiteRuntimeStore{SQLiteSchemaStore: &SQLiteSchemaStore{DB: db}}
				calls = []struct {
					name string
					call func() error
				}{
					{name: "begin_event_transaction", call: func() error { _, err := store.BeginEventTx(context.Background()); return err }},
					{name: "run_event_transaction", call: func() error {
						return store.RunEventTransaction(context.Background(), func(context.Context, *sql.Tx) error {
							callbackCalled = true
							return nil
						})
					}},
					{name: "run_runtime_mutation", call: func() error {
						return store.RunRuntimeMutation(context.Background(), func(context.Context, *sql.Tx) error {
							callbackCalled = true
							return nil
						})
					}},
					{name: "insert_delivery_targets", call: func() error {
						return store.InsertEventDeliveriesWithTargets(context.Background(), unacceptedAdmissionEventID, []string{"agent-a"}, nil)
					}},
					{name: "insert_delivery_routes", call: func() error {
						return store.InsertEventDeliveryRoutes(context.Background(), unacceptedAdmissionEventID, unacceptedAdmissionRoutes())
					}},
				}
			}

			for _, tc := range calls {
				t.Run(tc.name, func(t *testing.T) {
					callbackCalled = false
					err := tc.call()
					requireUnacceptedAdmissionFailure(t, err)
					if callbackCalled {
						t.Fatal("unaccepted mutation callback executed")
					}
				})
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unaccepted event mutation issued SQL: %v", err)
			}
		})
	}
}

func TestUnacceptedSelectedStoreCallerSuppliedDeliveryTransactionsFailBeforeSQL(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		for _, operation := range []string{"deliveries", "delivery_targets", "delivery_routes"} {
			t.Run(backend+"/"+operation, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock: %v", err)
				}
				defer db.Close()

				mock.ExpectBegin()
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatalf("begin caller-supplied transaction: %v", err)
				}
				mock.ExpectRollback()
				defer tx.Rollback()

				if backend == "postgres" {
					store := &PostgresStore{DB: db}
					switch operation {
					case "deliveries":
						err = store.InsertEventDeliveriesTx(context.Background(), tx, unacceptedAdmissionEventID, []string{"agent-a"})
					case "delivery_targets":
						err = store.InsertEventDeliveriesWithTargetsTx(context.Background(), tx, unacceptedAdmissionEventID, []string{"agent-a"}, nil)
					case "delivery_routes":
						err = store.InsertEventDeliveryRoutesTx(context.Background(), tx, unacceptedAdmissionEventID, unacceptedAdmissionRoutes())
					}
				} else {
					store := &SQLiteRuntimeStore{SQLiteSchemaStore: &SQLiteSchemaStore{DB: db}}
					switch operation {
					case "deliveries":
						err = store.InsertEventDeliveriesTx(context.Background(), tx, unacceptedAdmissionEventID, []string{"agent-a"})
					case "delivery_targets":
						err = store.InsertEventDeliveriesWithTargetsTx(context.Background(), tx, unacceptedAdmissionEventID, []string{"agent-a"}, nil)
					case "delivery_routes":
						err = store.InsertEventDeliveryRoutesTx(context.Background(), tx, unacceptedAdmissionEventID, unacceptedAdmissionRoutes())
					}
				}
				requireUnacceptedAdmissionFailure(t, err)
				if err := tx.Rollback(); err != nil {
					t.Fatalf("rollback caller-supplied transaction: %v", err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("unaccepted caller-supplied transaction issued SQL: %v", err)
				}
			})
		}
	}
}

func unacceptedAdmissionRoutes() []events.DeliveryRoute {
	return []events.DeliveryRoute{{SubscriberType: "agent", SubscriberID: "agent-a"}}
}

func requireUnacceptedAdmissionFailure(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "schema is unaccepted") {
		t.Fatalf("runtime call error = %v, want unaccepted admission failure", err)
	}
}
