package runforkexecution

import (
	"errors"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// WithFanOutExecutorForTest observes the real serving executor without replacing
// the retained session's bound owner or changing another execution owner.
func WithFanOutExecutorForTest(owner SelectedContractExecutionOwner, decorate func(startupownership.FanOutExecutor) startupownership.FanOutExecutor) (SelectedContractExecutionOwner, error) {
	ports, err := owner.require()
	if err != nil {
		return SelectedContractExecutionOwner{}, err
	}
	if decorate == nil || ports.testFanOutExecutor != nil {
		return SelectedContractExecutionOwner{}, errors.New("selected fan-out executor requires one test decorator")
	}
	copy := *ports
	copy.testFanOutExecutor = decorate
	return SelectedContractExecutionOwner{ports: &copy}, nil
}

func (p *selectedContractExecutionPorts) fanOutExecutor(executor startupownership.FanOutExecutor) (startupownership.FanOutExecutor, error) {
	if p.testFanOutExecutor == nil {
		return executor, nil
	}
	decorated := p.testFanOutExecutor(executor)
	if decorated != nil {
		switch value := reflect.ValueOf(decorated); value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				return nil, errors.New("selected fan-out test decorator returned a nil executor")
			}
		}
		return decorated, nil
	}
	return nil, errors.New("selected fan-out test decorator returned a nil executor")
}
