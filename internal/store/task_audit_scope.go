package store

import (
	"strings"

	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
)

type taskAuditIdentity struct {
	EntityID     string
	FlowInstance string
	ScopeKey     string
	Scope        string
}

func taskAuditIdentityFromTurn(rec runtimellm.AgentTurnRecord) taskAuditIdentity {
	return newTaskAuditIdentity(rec.EntityID, rec.FlowInstance, rec.ScopeKey)
}

func taskAuditIdentityFromConversation(rec runtimellm.ConversationRecord) taskAuditIdentity {
	return newTaskAuditIdentity("", "", rec.ScopeKey)
}

func newTaskAuditIdentity(entityID, flowInstance, scopeKey string) taskAuditIdentity {
	entityID = strings.TrimSpace(entityID)
	flowInstance = strings.TrimSpace(flowInstance)
	scopeKey = strings.TrimSpace(scopeKey)

	if entityID != "" {
		return taskAuditIdentity{
			EntityID: entityID,
			ScopeKey: entityID,
			Scope:    runtimesessions.SessionScopeEntity.String(),
		}
	}
	if flowInstance != "" {
		return taskAuditIdentity{
			FlowInstance: flowInstance,
			ScopeKey:     flowInstance,
			Scope:        runtimesessions.SessionScopeFlow.String(),
		}
	}
	return taskAuditIdentity{
		ScopeKey: scopeKey,
		Scope:    runtimesessions.SessionScopeGlobal.String(),
	}
}
