//go:build !darwin && !linux

package startupownership

import (
	"context"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

func CaptureSQLiteInspectionIdentity(string) (*SQLiteBackendIdentity, error) {
	_, err := acquireSQLiteFilePossession("")
	return nil, err
}

func probeSQLitePossession(context.Context, string, *SQLiteBackendIdentity) (bool, error) {
	_, err := acquireSQLiteFilePossession("")
	return false, err
}

func acquireSQLiteFilePossession(string) (sqlitePossession, error) {
	return nil, &runtimestartupownership.AcquisitionError{
		Failure: runtimestartupownership.AcquisitionPriorOwnerAmbiguous,
		Detail:  "SQLite selected-store filesystem ownership is unsupported on this platform",
	}
}

func acquireSQLiteConstructionPossession(path string) (sqlitePossession, error) {
	return acquireSQLiteFilePossession(path)
}

func sameSQLitePossessionResource(sqlitePossession, sqlitePossession) bool { return false }
