//go:build !darwin && !linux

package startupownership

import runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"

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
