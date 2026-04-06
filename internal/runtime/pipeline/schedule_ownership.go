package pipeline

import "context"

func ClaimAndRegisterSchedule(ctx context.Context, store SchedulePersistence, scheduler *Scheduler, sc Schedule) (bool, error) {
	if scheduler == nil {
		return false, nil
	}
	if store != nil {
		claimed, err := store.ClaimSchedule(ctx, sc)
		if err != nil {
			return false, err
		}
		if !claimed {
			return false, nil
		}
	}
	if err := scheduler.Register(sc); err != nil {
		if store != nil {
			_ = store.ReleaseSchedule(ctx, sc)
		}
		return false, err
	}
	return true, nil
}

func ReleaseOwnedSchedule(ctx context.Context, store SchedulePersistence, sc Schedule) error {
	if store == nil {
		return nil
	}
	return store.ReleaseSchedule(ctx, sc)
}

func ReleaseAllOwnedSchedules(ctx context.Context, store SchedulePersistence) error {
	if store == nil {
		return nil
	}
	return store.ReleaseScheduleClaims(ctx)
}
