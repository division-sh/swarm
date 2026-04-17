package engine

import "errors"

var (
	ErrChainDepthExceeded           = errors.New("engine: chain depth exceeded")
	ErrMissingSemanticSource        = errors.New("engine: semantic source is required")
	ErrMissingStateRepo             = errors.New("engine: state repository is required")
	ErrMissingTransaction           = errors.New("engine: transaction runner is required")
	ErrMissingEntityLocker          = errors.New("engine: entity locker is required")
	ErrMissingOutbox                = errors.New("engine: outbox writer is required")
	ErrMissingDispatcher            = errors.New("engine: post-commit dispatcher is required")
	ErrMissingNodeID                = errors.New("engine: node id is required")
	ErrMissingNodeHandler           = errors.New("engine: node handler is required")
	ErrInvalidTransition            = errors.New("engine: invalid transition")
	ErrEmitPersistencePrerequisite  = errors.New("engine: emit persistence prerequisite missing")
	ErrEmitPayloadContractViolation = errors.New("engine: emit payload contract violation")
	ErrInvalidConfig                = errors.New("engine: invalid config")
	ErrNotImplemented               = errors.New("engine: not implemented")
)
