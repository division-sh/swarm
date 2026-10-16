package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	decisioncard "github.com/division-sh/swarm/internal/store/internal/backend/decisioncard"
)

func CountHumanTaskToolCardStorageForTest(ctx context.Context, selected any, cardID string) (int, error) {
	if cardID == "" || strings.TrimSpace(cardID) != cardID {
		return 0, fmt.Errorf("human-task observation requires exact card identity")
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return decisioncard.CountHumanTaskToolCardStorageTx(ctx, tx, cardID)
	})
}

func CountHumanTaskToolContinuationStorageForTest(ctx context.Context, selected any, cardID string) (int, error) {
	if cardID == "" || strings.TrimSpace(cardID) != cardID {
		return 0, fmt.Errorf("human-task observation requires exact card identity")
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return decisioncard.CountHumanTaskToolContinuationStorageTx(ctx, tx, cardID)
	})
}

func CountAllHumanTaskToolCardsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, decisioncard.CountAllHumanTaskToolCardsStorageTx)
}
