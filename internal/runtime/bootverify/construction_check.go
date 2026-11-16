package bootverify

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
)

// ConstructionTarget identifies input only; it cannot grant store possession.
type ConstructionTarget struct {
	Backend string
	Seal    string
}

func PostgresConstructionTarget(dsn string) ConstructionTarget {
	return ConstructionTarget{Backend: "postgres", Seal: fmt.Sprintf("%x", sha256.Sum256([]byte(dsn)))}
}

func SQLiteConstructionTarget(path string) (ConstructionTarget, error) {
	path, err := filepath.Abs(filepath.Clean(path))
	return ConstructionTarget{Backend: "sqlite", Seal: path}, err
}

// ConstructionChecks performs the fixed serve check stage under native
// exclusion, before writable construction. It exposes no store or authority.
type ConstructionChecks interface {
	InspectConstruction(context.Context, ConstructionTarget) (Report, error)
}
