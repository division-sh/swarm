package startupownership

import "context"

type Lease interface {
	Release(context.Context) error
}

type Store interface {
	AcquireRuntimeStartupOwnership(context.Context, string) (Lease, error)
}
