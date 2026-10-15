package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/store/internal/startupownership"
)

type SourceSetHeadFault = startupownership.SourceSetHeadFault
type SourceSetHeadFaultReceipt = startupownership.SourceSetHeadFaultReceipt

const (
	SourceSetHeadCorruptPlan      = startupownership.SourceSetHeadCorruptPlan
	SourceSetHeadRevisionMismatch = startupownership.SourceSetHeadRevisionMismatch
)

func ApplySourceSetHeadFaultForTest(ctx context.Context, selected any, expected agenttopology.SourceSetPlan, fault SourceSetHeadFault) (*SourceSetHeadFaultReceipt, error) {
	owner, err := sourceSetHeadFaultOwner(selected)
	if err != nil {
		return nil, err
	}
	return startupownership.ApplySourceSetHeadFaultForTest(ctx, owner, expected, fault)
}

func RestoreSourceSetHeadFaultForTest(ctx context.Context, selected any, receipt *SourceSetHeadFaultReceipt) error {
	owner, err := sourceSetHeadFaultOwner(selected)
	if err != nil {
		return err
	}
	return startupownership.RestoreSourceSetHeadFaultForTest(ctx, owner, receipt)
}

func sourceSetHeadFaultOwner(selected any) (any, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		if s != nil && s.startupPostgresOwner != nil {
			return s.startupPostgresOwner, nil
		}
	case *SQLiteRuntimeStore:
		if s != nil && s.startupSQLiteOwner != nil {
			return s.startupSQLiteOwner, nil
		}
	}
	return nil, fmt.Errorf("source-set head fault requires the original native selected store")
}
