package store

import (
	"context"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
)

const destructiveResetPipelineSubscriberID = activeRunQuiescencePipelineSubscriberID

func (s *PostgresStore) ApplyDestructiveResetQuiescence(ctx context.Context, req destructivereset.QuiescenceRequest) (destructivereset.QuiescenceResult, error) {
	requestedAt := req.RequestedAt.UTC()
	if requestedAt.IsZero() {
		requestedAt = time.Now().UTC()
	}
	operationName := strings.TrimSpace(req.Result.OperationName)
	if operationName == "" {
		operationName = destructivereset.DefaultOperationName
	}
	result, err := s.ApplyActiveRunQuiescence(ctx, runtimerunquiescence.Request{
		OperationName: operationName,
		DryRun:        req.Result.DryRun,
		RequestedAt:   requestedAt,
		RunIDs:        activeRunIDsFromResetPlan(req.Result.Plan),
		ReasonCode:    destructivereset.QuiescenceReasonCode,
		ControlledBy:  destructivereset.QuiescenceControlledBy,
		DeliveryNote:  destructivereset.QuiescenceDeliveryNote,
	})
	if err != nil {
		return destructivereset.QuiescenceResult{}, err
	}
	return destructiveResetQuiescenceResult(result), nil
}

func activeRunIDsFromResetPlan(plan destructivereset.Plan) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(plan.ActiveRuns))
	for _, run := range plan.ActiveRuns {
		if !activeRunQuiescenceRunStatusActive(run.Status) {
			continue
		}
		id := nullUUIDString(run.RunID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func destructiveResetQuiescenceResult(result runtimerunquiescence.Result) destructivereset.QuiescenceResult {
	out := destructivereset.QuiescenceResult{
		OperationName:        result.OperationName,
		DryRun:               result.DryRun,
		AppliedAt:            result.AppliedAt,
		ReasonCode:           result.ReasonCode,
		ControlledBy:         result.ControlledBy,
		PipelineReceiptCount: result.PipelineReceiptCount,
	}
	for _, run := range result.Runs {
		out.Runs = append(out.Runs, destructivereset.QuiescedRun{
			RunID:          run.RunID,
			PreviousStatus: run.PreviousStatus,
			Status:         run.Status,
			ReasonCode:     run.ReasonCode,
			Changed:        run.Changed,
		})
	}
	for _, delivery := range result.Deliveries {
		out.Deliveries = append(out.Deliveries, destructivereset.QuiescedDelivery{
			DeliveryID:      delivery.DeliveryID,
			RunID:           delivery.RunID,
			EventID:         delivery.EventID,
			SubscriberType:  delivery.SubscriberType,
			SubscriberID:    delivery.SubscriberID,
			PreviousStatus:  delivery.PreviousStatus,
			Status:          delivery.Status,
			ReasonCode:      delivery.ReasonCode,
			PreviousReason:  delivery.PreviousReason,
			ActiveSessionID: delivery.ActiveSessionID,
			Changed:         delivery.Changed,
		})
	}
	return out
}
