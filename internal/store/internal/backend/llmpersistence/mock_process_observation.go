package llmpersistence

import (
	"context"
	"database/sql"
)

func CountMockConversationTurnsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE execution_mode = 'mock' AND (agent_id LIKE 'phrase-bot%' OR agent_id = 'memory-bot')`).Scan(&count)
	return count, err
}

func CountMockPhraseTurnsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE execution_mode = 'mock' AND agent_id LIKE 'phrase-bot%'`).Scan(&count)
	return count, err
}

type MockConversationTurnDiagnosticStorage struct {
	AgentID, ExecutionMode     string
	ParseOK                    bool
	Request, Response, Failure string
}

type MockConversationTurnDiagnosticsStorage struct {
	Rows      []MockConversationTurnDiagnosticStorage
	ScanError error
}

func ReadMockConversationTurnDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx) (MockConversationTurnDiagnosticsStorage, error) {
	var out MockConversationTurnDiagnosticsStorage
	rows, err := tx.QueryContext(ctx, `SELECT agent_id, execution_mode, parse_ok, CAST(COALESCE(request_payload, '{}') AS TEXT), CAST(COALESCE(response_payload, '{}') AS TEXT), CAST(COALESCE(failure, '{}') AS TEXT) FROM agent_turns ORDER BY created_at`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var row MockConversationTurnDiagnosticStorage
		if out.ScanError = rows.Scan(&row.AgentID, &row.ExecutionMode, &row.ParseOK, &row.Request, &row.Response, &row.Failure); out.ScanError != nil {
			break
		}
		out.Rows = append(out.Rows, row)
	}
	// Preserve the original failure-only diagnostic's scan/prefix contract.
	return out, nil
}

func ReadLatestMockPhraseRequestStorageTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var request string
	err := tx.QueryRowContext(ctx, `SELECT CAST(request_payload AS TEXT) FROM agent_turns WHERE agent_id LIKE 'phrase-bot%' ORDER BY created_at DESC LIMIT 1`).Scan(&request)
	return request, err
}
