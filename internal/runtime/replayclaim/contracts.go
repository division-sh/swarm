package replayclaim

import (
	"errors"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

var (
	ErrAuthoritativeRecipientManifestUnavailable = errors.New("authoritative delivery recipient manifest is unavailable for non-persistent event stores")
	ErrMissingCommittedReplayScope               = errors.New("store does not support authoritative committed replay scope for persisted replay")
)

type CommittedReplayScope = runtimepipelineobligation.CommittedScope

const (
	CommittedReplayScopeDirect     = runtimepipelineobligation.ScopeDirect
	CommittedReplayScopeSubscribed = runtimepipelineobligation.ScopeSubscribed
)
