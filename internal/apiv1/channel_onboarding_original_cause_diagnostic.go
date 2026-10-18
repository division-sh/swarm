package apiv1

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"reflect"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"github.com/lib/pq"
)

// Disposable original-error observation, reviewer-g 6251306217. Never land this
// overlay or serialize the error, its prose, request parameters or driver data.
const onboardingOriginalCausePrefix = "ONBOARDING_ORIGINAL_CAUSE "

type onboardingOriginalCauseNode struct {
	Index           int                      `json:"index"`
	Parent          int                      `json:"parent"`
	Depth           int                      `json:"depth"`
	Type            string                   `json:"type"`
	Sentinels       []string                 `json:"sentinels,omitempty"`
	Class           string                   `json:"class,omitempty"`
	SQLState        string                   `json:"sqlstate,omitempty"`
	SQLStateInvalid bool                     `json:"sqlstate_invalid,omitempty"`
	DeadlockCycle   *onboardingDeadlockCycle `json:"deadlock_cycle,omitempty"`
}

type onboardingOriginalCauseReport struct {
	Ordinal   uint64                        `json:"request_ordinal"`
	Nodes     []onboardingOriginalCauseNode `json:"nodes"`
	Truncated bool                          `json:"truncated"`
}

func onboardingOriginalCauseMetadata(err error, ordinal uint64) onboardingOriginalCauseReport {
	report := onboardingOriginalCauseReport{Ordinal: ordinal}
	sentinels := []struct {
		name string
		err  error
	}{
		{"context_canceled", context.Canceled},
		{"context_deadline", context.DeadlineExceeded},
		{"onboarding_invalid_request", channelonboarding.ErrInvalidRequest},
		{"onboarding_not_found", channelonboarding.ErrNotFound},
		{"onboarding_conflict", channelonboarding.ErrConflict},
		{"onboarding_revision_conflict", channelonboarding.ErrRevisionConflict},
		{"session_pending", channelonboarding.ErrSessionPending},
		{"session_account", sessionstate.ErrSessionAccount},
		{"identity_conflict", operatorchannel.ErrConflict},
		{"identity_revision_conflict", operatorchannel.ErrRevisionConflict},
		{"credential_unusable", credentials.ErrCredentialValueUnusable},
	}
	var visit func(error, int, int)
	visit = func(cause error, parent, depth int) {
		if cause == nil {
			return
		}
		if len(report.Nodes) >= 32 || depth > 8 {
			report.Truncated = true
			return
		}
		node := onboardingOriginalCauseNode{Index: len(report.Nodes), Parent: parent, Depth: depth, Type: reflect.TypeOf(cause).String()}
		// Direct identity matches only. errors.Is/As would perform an additional
		// unbounded traversal (or invoke a custom Is/As method) behind this bound.
		for _, sentinel := range sentinels {
			if cause == sentinel.err {
				node.Sentinels = append(node.Sentinels, sentinel.name)
			}
		}
		if typed, ok := cause.(*runtimefailures.Error); ok && typed != nil {
			if _, known := runtimefailures.DefinitionFor(typed.Failure.Class); known {
				node.Class = string(typed.Failure.Class)
			} else {
				node.Class = "unlisted"
			}
		}
		if coded, ok := cause.(interface{ SQLState() string }); ok {
			state := coded.SQLState()
			valid := len(state) == 5
			for i := 0; valid && i < len(state); i++ {
				c := state[i]
				if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
					valid = false
				}
			}
			if valid {
				node.SQLState = state
			} else {
				node.SQLStateInvalid = true
			}
		}
		if pg, ok := cause.(*pq.Error); ok && pg != nil && pg.Code == "40P01" {
			cycle := onboardingDeadlockMetadata(pg.Detail)
			node.DeadlockCycle = &cycle
		}
		report.Nodes = append(report.Nodes, node)
		if joined, ok := cause.(interface{ Unwrap() []error }); ok {
			for _, child := range joined.Unwrap() {
				if child == nil {
					continue
				}
				if len(report.Nodes) >= 32 {
					report.Truncated = true
					break
				}
				visit(child, node.Index, depth+1)
			}
		} else if wrapped, ok := cause.(interface{ Unwrap() error }); ok {
			visit(wrapped.Unwrap(), node.Index, depth+1)
		}
	}
	visit(err, -1, 0)
	return report
}

func captureOnboardingRetryOriginalCause(err error, ordinal uint64) {
	if err == nil || os.Getenv("SWARM_TEST_2614_ORIGINAL_CAUSE") != "1" {
		return
	}
	report := onboardingOriginalCauseMetadata(err, ordinal)
	raw, _ := json.Marshal(report)
	log.Print(onboardingOriginalCausePrefix + string(raw))
}
