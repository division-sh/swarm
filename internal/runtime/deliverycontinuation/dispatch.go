package deliverycontinuation

import (
	"errors"
	"fmt"
)

// DispatchDisposition closes the process-local result of re-entering one
// exact persisted delivery route.
type DispatchDisposition uint8

const (
	DispatchTransferred DispatchDisposition = iota + 1
	DispatchTerminal
	DispatchDeferred
	DispatchFatal
)

// DispatchWakeAuthority identifies the already-live owner whose committed
// transition will wake a deferred continuation. It is not retry authority.
type DispatchWakeAuthority uint8

const (
	DispatchWakeAgentRouteLifecycle DispatchWakeAuthority = iota + 1
	DispatchWakeInternalSubscriptionLifecycle
	DispatchWakeCarrierReturn
)

func (a DispatchWakeAuthority) String() string {
	switch a {
	case DispatchWakeAgentRouteLifecycle:
		return "agent_route_lifecycle"
	case DispatchWakeInternalSubscriptionLifecycle:
		return "internal_subscription_lifecycle"
	case DispatchWakeCarrierReturn:
		return "carrier_return"
	default:
		return ""
	}
}

// DispatchResult is the complete result of one EventBus continuation
// dispatch. Only a deferred result names a wake owner; only a fatal result
// carries an error.
type DispatchResult struct {
	disposition DispatchDisposition
	wake        DispatchWakeAuthority
	err         error
}

func Transferred() DispatchResult {
	return DispatchResult{disposition: DispatchTransferred}
}

func TerminallySettled() DispatchResult {
	return DispatchResult{disposition: DispatchTerminal}
}

func Deferred(wake DispatchWakeAuthority) DispatchResult {
	return DispatchResult{disposition: DispatchDeferred, wake: wake}
}

func Fatal(err error) DispatchResult {
	if err == nil {
		err = errors.New("delivery continuation dispatch failed without a cause")
	}
	return DispatchResult{disposition: DispatchFatal, err: err}
}

func (r DispatchResult) Disposition() DispatchDisposition     { return r.disposition }
func (r DispatchResult) WakeAuthority() DispatchWakeAuthority { return r.wake }
func (r DispatchResult) Failure() error                       { return r.err }

func (r DispatchResult) Validate() error {
	switch r.disposition {
	case DispatchTransferred, DispatchTerminal:
		if r.wake != 0 || r.err != nil {
			return errors.New("settled delivery continuation dispatch result carries extra authority")
		}
	case DispatchDeferred:
		if r.wake.String() == "" {
			return errors.New("deferred delivery continuation requires a named live wake authority")
		}
		if r.err != nil {
			return errors.New("deferred delivery continuation cannot carry a failure")
		}
	case DispatchFatal:
		if r.err == nil {
			return errors.New("fatal delivery continuation dispatch requires a failure")
		}
		if r.wake != 0 {
			return errors.New("fatal delivery continuation dispatch cannot carry wake authority")
		}
	default:
		return fmt.Errorf("unknown delivery continuation dispatch disposition %d", r.disposition)
	}
	return nil
}
