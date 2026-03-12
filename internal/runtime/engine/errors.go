package engine

import "errors"

var (
	ErrChainDepthExceeded   = errors.New("engine: chain depth exceeded")
	ErrMissingSemanticSource = errors.New("engine: semantic source is required")
	ErrMissingStateRepo     = errors.New("engine: state repository is required")
	ErrMissingTransaction   = errors.New("engine: transaction runner is required")
	ErrMissingEntityLocker  = errors.New("engine: entity locker is required")
	ErrMissingOutbox        = errors.New("engine: outbox writer is required")
	ErrMissingDispatcher    = errors.New("engine: post-commit dispatcher is required")
	ErrMissingNodeID        = errors.New("engine: node id is required")
	ErrMissingNodeHandler   = errors.New("engine: node handler is required")
	ErrNotImplemented       = errors.New("engine: not implemented")
)
