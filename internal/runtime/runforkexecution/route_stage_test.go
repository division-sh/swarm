package runforkexecution

import (
	"context"
	"errors"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

type selectedFlowRoutePublisherProbe struct {
	ack                         bool
	stageErr, nextErr           error
	stages, publishes, verifies int
}

func (p *selectedFlowRoutePublisherProbe) StageFlowInstanceRouteContext(context.Context, runtimebus.FlowInstanceRouteMaterializationRequest) (runtimebus.FlowInstanceRouteTopologyResult, error) {
	p.stages++
	return runtimebus.FlowInstanceRouteTopologyResult{Acknowledged: p.ack}, p.stageErr
}

func (p *selectedFlowRoutePublisherProbe) PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest) error {
	p.publishes++
	return p.nextErr
}

func (p *selectedFlowRoutePublisherProbe) VerifyFlowInstanceRoute(context.Context, runtimeflowidentity.RunScopedFlowInstance) error {
	p.verifies++
	return nil
}

func TestSelectedForkRouteStageAcknowledgementControlsPublication(t *testing.T) {
	cleanup := errors.New("route stage postcommit cleanup")
	publishErr := errors.New("independent route publication failure")
	for _, tc := range []struct {
		name                    string
		ack                     bool
		stageErr, nextErr       error
		wantPublish, wantVerify int
	}{
		{name: "acknowledged_cleanup", ack: true, stageErr: cleanup, wantPublish: 1, wantVerify: 1},
		{name: "acknowledged_then_publish_failure", ack: true, stageErr: cleanup, nextErr: publishErr, wantPublish: 1},
		{name: "missing_ack_with_error", stageErr: cleanup},
		{name: "missing_ack_without_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &selectedFlowRoutePublisherProbe{ack: tc.ack, stageErr: tc.stageErr, nextErr: tc.nextErr}
			diagnostics := &selectedForkCommitDiagnostics{}
			var published []runtimeflowidentity.RunScopedFlowInstance
			err := publishSelectedContractFlowRoute(context.Background(), probe, runtimebus.FlowInstanceRouteMaterializationRequest{}, diagnostics, &published)
			if probe.stages != 1 || probe.publishes != tc.wantPublish || probe.verifies != tc.wantVerify || len(published) != tc.wantPublish {
				t.Fatalf("stage/publish/verify/cleanup=%d/%d/%d/%d", probe.stages, probe.publishes, probe.verifies, len(published))
			}
			if tc.ack {
				if !errors.Is(diagnostics.err(), cleanup) || !errors.Is(err, tc.nextErr) {
					t.Fatalf("acknowledged stage err=%v diagnostic=%v", err, diagnostics.err())
				}
			} else if err == nil || diagnostics.err() != nil {
				t.Fatalf("unacknowledged stage err=%v diagnostic=%v", err, diagnostics.err())
			}
		})
	}
}
