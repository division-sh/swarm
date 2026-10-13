package runforkexecution

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/effects"
)

type selectedCloseAcknowledgmentProbe struct {
	SelectedContractRuntimeExecutionLifecycle
	acknowledged bool
	cause        error
	identity     string
	calls        int
	canceled     bool
}

func (p *selectedCloseAcknowledgmentProbe) CloseRunForkSelectedContractRuntimeExecution(ctx context.Context, id string) (bool, error) {
	p.calls++
	p.identity = id
	p.canceled = ctx.Err() != nil
	return p.acknowledged, p.cause
}

func TestSelectedForkCloseConsumesExactAcknowledgment(t *testing.T) {
	cleanup := errors.New("acknowledged close cleanup")
	for _, acknowledged := range []bool{false, true} {
		for _, withError := range []bool{false, true} {
			name := "unacknowledged"
			if acknowledged {
				name = "acknowledged"
			}
			if withError {
				name += "_error"
			}
			t.Run(name, func(t *testing.T) {
				probe := &selectedCloseAcknowledgmentProbe{acknowledged: acknowledged}
				if withError {
					probe.cause = cleanup
				}
				diagnostics := &selectedForkCommitDiagnostics{}
				container := selectedContractForkLocalRuntimeContainer{
					ports:       &selectedContractExecutionPorts{runtimeExecution: probe},
					authority:   effects.Authority{ID: "exact-selected-executor"},
					diagnostics: diagnostics,
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err := container.Close(ctx)
				if (err == nil) != acknowledged || (!acknowledged && withError && !errors.Is(err, cleanup)) {
					t.Fatalf("close ack=%t err=%v", acknowledged, err)
				}
				if got := diagnostics.err(); acknowledged && withError {
					if !errors.Is(got, cleanup) {
						t.Fatalf("acknowledged close lost cleanup diagnostic: %v", got)
					}
				} else if got != nil {
					t.Fatalf("uncommitted or absent diagnostic: %v", got)
				}
				if probe.calls != 1 || probe.identity != container.authority.ID || probe.canceled {
					t.Fatalf("close lost exact identity or cleanup context: %+v", probe)
				}
			})
		}
	}
}
