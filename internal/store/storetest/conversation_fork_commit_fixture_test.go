package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestConversationForkCommitPortsRefuseRawCancelledAbsentAndClosedOwnersBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			reads := conversationForkCommitOwnershipReads()
			for i, read := range reads {
				zero, err := read(context.Background(), selected)
				if !zero || (i < 2 && err != nil) || (i == 2 && !errors.Is(err, sql.ErrNoRows)) {
					t.Fatalf("empty physical read %d changed: zero=%t err=%v", i, zero, err)
				}
			}
			actions := append(reads, conversationForkCommitOwnershipFaults()...)
			for _, action := range actions {
				for _, foreign := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}} {
					if zero, err := action(context.Background(), foreign); !zero || err == nil {
						t.Fatalf("raw/foreign %T supplied evidence/fault: %t/%v", foreign, zero, err)
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if zero, err := action(ctx, selected); !zero || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled native owner supplied evidence/fault: %t/%v", zero, err)
				}
			}
			if err := RemoveConversationForkCompletionInsertFailure(context.Background(), selected); err == nil {
				t.Fatal("absent completion fault reported successful removal")
			}
			if err := InstallConversationForkCompletionInsertFailure(context.Background(), selected); err != nil {
				t.Fatal(err)
			}
			if err := RemoveConversationForkCompletionInsertFailure(context.Background(), selected); err != nil {
				t.Fatal(err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, action := range actions {
				if zero, err := action(context.Background(), selected); !zero || err == nil {
					t.Fatalf("closed native owner supplied evidence/fault: %t/%v", zero, err)
				}
			}
		})
	}
}

func conversationForkCommitOwnershipReads() []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, selected any) (bool, error) {
			count, err := CountConversationForkStorage(ctx, selected)
			return count == 0, err
		},
		func(ctx context.Context, selected any) (bool, error) {
			count, err := CountConversationForkCompletionStorage(ctx, selected)
			return count == 0, err
		},
		func(ctx context.Context, selected any) (bool, error) {
			expiry, err := ReadConversationForkCompletionExpiryStorage(ctx, selected)
			return expiry == (ConversationForkCompletionExpiryStorage{}), err
		},
	}
}

func conversationForkCommitOwnershipFaults() []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, selected any) (bool, error) {
			return true, InstallConversationForkCompletionInsertFailure(ctx, selected)
		},
		func(ctx context.Context, selected any) (bool, error) {
			return true, RemoveConversationForkCompletionInsertFailure(ctx, selected)
		},
	}
}
