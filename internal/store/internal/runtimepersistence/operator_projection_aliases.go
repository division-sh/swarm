package runtimepersistence

import (
	"context"
	"strings"
	"time"

	storeoperatorsurface "github.com/division-sh/swarm/internal/store/internal/operatorsurface"
)

type OperatorEventListFilter = storeoperatorsurface.OperatorEventListFilter
type OperatorEventListOptions = storeoperatorsurface.OperatorEventListOptions
type OperatorEventListResult = storeoperatorsurface.OperatorEventListResult
type OperatorEventFull = storeoperatorsurface.OperatorEventFull
type OperatorEventDelivery = storeoperatorsurface.OperatorEventDelivery
type OperatorDeadLetterRecord = storeoperatorsurface.OperatorDeadLetterRecord
type OperatorRuntimeLogListOptions = storeoperatorsurface.OperatorRuntimeLogListOptions
type OperatorRuntimeLogListResult = storeoperatorsurface.OperatorRuntimeLogListResult
type OperatorRuntimeLogEntry = storeoperatorsurface.OperatorRuntimeLogEntry
type OperatorRuntimeIncidentListOptions = storeoperatorsurface.OperatorRuntimeIncidentListOptions
type OperatorRuntimeIncidentListResult = storeoperatorsurface.OperatorRuntimeIncidentListResult
type OperatorRuntimeIncident = storeoperatorsurface.OperatorRuntimeIncident
type OperatorObservabilityReadOwner = storeoperatorsurface.OperatorObservabilityReadOwner
type OperatorObservabilityReadSurface = storeoperatorsurface.OperatorObservabilityReadSurface
type RunDebugQueryOptions = storeoperatorsurface.RunDebugQueryOptions
type RunDebugRunSummary = storeoperatorsurface.RunDebugRunSummary
type RunDebugEventCount = storeoperatorsurface.RunDebugEventCount
type RunDebugDeliveryCount = storeoperatorsurface.RunDebugDeliveryCount
type RunDebugEvent = storeoperatorsurface.RunDebugEvent
type RunDebugMutation = storeoperatorsurface.RunDebugMutation
type RunDebugDeadLetter = storeoperatorsurface.RunDebugDeadLetter
type RunDebugFailureDelivery = storeoperatorsurface.RunDebugFailureDelivery
type RunDebugAgentTurn = storeoperatorsurface.RunDebugAgentTurn
type RunDebugRuntimeLog = storeoperatorsurface.RunDebugRuntimeLog
type RunDebugRuntimeSummary = storeoperatorsurface.RunDebugRuntimeSummary
type RunTestQuiescence = storeoperatorsurface.RunTestQuiescence
type RunDebugReport = storeoperatorsurface.RunDebugReport
type RunDebugTraceQueryOptions = storeoperatorsurface.RunDebugTraceQueryOptions
type RunDebugTraceFilter = storeoperatorsurface.RunDebugTraceFilter
type RunDebugTraceRow = storeoperatorsurface.RunDebugTraceRow
type RunHeader = storeoperatorsurface.RunHeader
type RunHeaderListOptions = storeoperatorsurface.RunHeaderListOptions
type RunOperationalStatus = storeoperatorsurface.RunOperationalStatus

var NewOperatorEventFull = storeoperatorsurface.NewOperatorEventFull
var EnrichOperatorEventFailureEvidence = storeoperatorsurface.EnrichOperatorEventFailureEvidence
var EnrichOperatorDeliveryFailureEvidence = storeoperatorsurface.EnrichOperatorDeliveryFailureEvidence
var ProjectRunOperationalStatus = storeoperatorsurface.ProjectRunOperationalStatus
var NewOperatorObservabilityReadSurface = storeoperatorsurface.NewOperatorObservabilityReadSurface

func storeTimeValue(raw any) (time.Time, bool, error) {
	return storeoperatorsurface.StoreTimeValue(raw)
}

func traceTimePtr(value time.Time) *time.Time {
	return storeoperatorsurface.TraceTimePtr(value)
}

func readStoreString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func runDebugTraceSessionSources() string {
	return storeoperatorsurface.RunDebugTraceSessionSources()
}

func (s *PostgresStore) loadRunTestQuiescence(ctx context.Context, runID string, observedAt time.Time) (RunTestQuiescence, error) {
	return s.operatorPostgres.LoadRunTestQuiescence(ctx, runID, observedAt)
}

func (s *SQLiteRuntimeStore) sqliteRunTestQuiescence(ctx context.Context, runID string, observedAt time.Time) (RunTestQuiescence, error) {
	return s.operatorSQLite.LoadRunTestQuiescence(ctx, runID, observedAt)
}
