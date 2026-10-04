package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

var projectPersistedAgentConfig = storeagent.ProjectAgentConfig
var hydratePersistedAgentConfig = storeagent.HydrateAgentConfig
var decodePersistedAgentRuntimeDescriptor = storeagent.DecodePersistedAgentRuntimeDescriptor
var extractSubscriptions = storeagent.ExtractSubscriptions
var validateOpaqueAgentConfig = storeagent.ValidateOpaqueAgentConfig
var normalizeJSONPayload = storeagent.NormalizeJSONPayload
var nullable = storeagent.Nullable
var sanitizeSchemaIdent = storeagent.SanitizeSchemaIdent
var quoteIdent = storeagent.QuoteIdent
var redactPayloadValue = storeagent.RedactPayloadValue
var redactText = storeagent.RedactText
var redactName = storeagent.RedactName
var isNameKey = storeagent.IsNameKey

func (s *PostgresStore) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.agentPostgresOwner.LoadAgents(ctx)
}

func (s *SQLiteRuntimeStore) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.agentSQLiteOwner.LoadAgents(ctx)
}

func (s *PostgresStore) ObserveOrdinaryRunSource(ctx context.Context, source correlation.SourceArtifactFact, runID string) (bool, error) {
	return s.agentPostgresOwner.ObserveOrdinaryRunSource(ctx, source, runID)
}

func (s *SQLiteRuntimeStore) ObserveOrdinaryRunSource(ctx context.Context, source correlation.SourceArtifactFact, runID string) (bool, error) {
	return s.agentSQLiteOwner.ObserveOrdinaryRunSource(ctx, source, runID)
}

func (s *PostgresStore) loadAgentsSpec(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.agentPostgresOwner.LoadAgentsSpec(ctx)
}
