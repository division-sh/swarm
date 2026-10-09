package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SessionPublicationLock struct {
	owner *private.SessionPublicationLock
}

func HoldSessionPublicationOnboardingLock(ctx context.Context, selected any, operationID string) (*SessionPublicationLock, error) {
	owner, err := private.HoldSessionPublicationOnboardingLockForTest(ctx, selected, operationID)
	if err != nil {
		return nil, err
	}
	return &SessionPublicationLock{owner: owner}, nil
}
func (l *SessionPublicationLock) Waiting(ctx context.Context) (bool, error) {
	return l.owner.Waiting(ctx)
}
func (l *SessionPublicationLock) Release() error { return l.owner.Release() }
func SetSessionPublicationInsertFault(ctx context.Context, selected any, enabled bool) error {
	return private.SetSessionPublicationInsertFaultForTest(ctx, selected, enabled)
}
