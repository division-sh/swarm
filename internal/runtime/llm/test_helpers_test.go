package llm

import "context"

type turnCapture struct {
	records []AgentTurnRecord
}

func (t *turnCapture) AppendAgentTurn(_ context.Context, rec AgentTurnRecord) error {
	t.records = append(t.records, rec)
	return nil
}
