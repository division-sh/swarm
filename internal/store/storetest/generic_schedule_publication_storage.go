package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type GenericSchedulePublicationModes = private.GenericSchedulePublicationModes

func ReadGenericSchedulePublicationModes(ctx context.Context, selected any, taskID string) (GenericSchedulePublicationModes, error) {
	return private.ReadGenericSchedulePublicationModesForTest(ctx, selected, taskID)
}

func ReadGenericScheduleDistinctPublicationModes(ctx context.Context, selected any, taskID string) (GenericSchedulePublicationModes, error) {
	return private.ReadGenericScheduleDistinctPublicationModesForTest(ctx, selected, taskID)
}

func CountGenericSchedulePublications(ctx context.Context, selected any, taskID string) (int, error) {
	return private.CountGenericSchedulePublicationsForTest(ctx, selected, taskID)
}
