//go:build !darwin && !linux

package startupownership

import (
	"context"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"os"
)

func captureSQLiteInspectionCoordinate(string) (os.FileInfo, error) {
	_, err := acquireSQLiteFilePossession("")
	return nil, err
}

func CaptureSQLiteInspectionIdentity(string) (*SQLiteBackendIdentity, error) {
	_, err := acquireSQLiteFilePossession("")
	return nil, err
}

func ObserveSQLiteInspectionAbsence(context.Context, string) (bool, error) {
	_, err := acquireSQLiteFilePossession("")
	return false, err
}

func inspectSQLiteCoordinate(ctx context.Context, path string) (string, bool, error) {
	absent, err := ObserveSQLiteInspectionAbsence(ctx, path)
	return path, absent, err
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

func waitSQLiteFilePossession(context.Context, string) (sqlitePossession, error) {
	_, err := acquireSQLiteFilePossession("")
	return nil, err
}

func waitSQLiteConstructionPossession(context.Context, string, ...*SQLiteInspectionInput) (sqlitePossession, error) {
	_, err := acquireSQLiteFilePossession("")
	return nil, err
}

func waitSQLitePrewritePossession(context.Context, string, ...*SQLiteInspectionInput) (sqlitePossession, error) {
	_, err := acquireSQLiteFilePossession("")
	return nil, err
}

func sameSQLitePossessionResource(sqlitePossession, sqlitePossession) bool { return false }
