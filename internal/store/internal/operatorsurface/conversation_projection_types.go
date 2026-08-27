package operatorsurface

import (
	"github.com/division-sh/swarm/internal/operatorread"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func operatorLiveTurnFromPublic(turn *operatorread.OperatorPublicConversationTurn) *operatorread.OperatorLiveTurn {
	if turn == nil {
		return nil
	}
	out := &operatorread.OperatorLiveTurn{
		TurnID: turn.TurnID, TaskID: turn.TaskID, ParseOK: turn.ParseOK,
		AssistantVisibleOutput: turn.AssistantVisibleOutput, Outcome: turn.Outcome,
	}
	for i := len(turn.Activity) - 1; i >= 0; i-- {
		activity := turn.Activity[i]
		if activity.Kind != "tool_result" && activity.Kind != "tool" {
			continue
		}
		ok := turn.ParseOK
		if activity.OK != nil {
			ok = *activity.OK
		}
		out.LastTool = &operatorread.OperatorAgentTool{Name: activity.ToolName, ToolUseID: activity.ToolUseID, OK: ok}
		break
	}
	return out
}

func enrichOperatorProjectionWithPublicTurn(projection *operatorAgentProjection, turn *operatorread.OperatorPublicConversationTurn) {
	if projection == nil || turn == nil {
		return
	}
	projection.LiveTurn = operatorLiveTurnFromPublic(turn)
	projection.LastTool = projection.LiveTurn.LastTool
	projection.CurrentTaskID = turn.TaskID
	projection.DiagnosisActive = operatorAgentDiagnosisActiveFromLatestTurn(turn.TurnID, turn.TaskID, turn.EntityID)
	projection.LastTurnRef = &operatorread.OperatorTurnRef{
		TurnID: turn.TurnID, CompletedAt: turn.CompletedAt, ParseOK: turn.ParseOK,
		Failure: runtimefailures.CloneEnvelope(turn.Failure),
	}
}
