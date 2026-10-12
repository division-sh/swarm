package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type PostgresAdministrativeSurroundingRead = private.PostgresAdministrativeSurroundingRead

func LimitPostgresAdministrativeFixturePool(ctx context.Context, selected any) error {
	return private.LimitPostgresAdministrativeFixturePoolForTest(ctx, selected)
}

func ReadPostgresAdministrativeUnrelatedProgress(ctx context.Context, selected any) (int, error) {
	return private.ReadPostgresAdministrativeUnrelatedProgressForTest(ctx, selected)
}

func HoldPostgresAdministrativeSurroundingRead(ctx context.Context, selected any) (*PostgresAdministrativeSurroundingRead, error) {
	return private.HoldPostgresAdministrativeSurroundingReadForTest(ctx, selected)
}
