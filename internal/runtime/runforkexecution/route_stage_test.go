package runforkexecution

import (
	"context"
	"errors"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
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

func (p *selectedFlowRoutePublisherProbe) PublishPersistedFlowInstanceRouteForAttempt(context.Context, runtimebus.FlowInstanceRouteMaterializationRequest, runtimepipeline.DynamicFlowRuntimeActivationAttempt) (runtimebus.FlowRoutePublicationHandle, error) {
	p.publishes++
	if p.nextErr != nil {
		return nil, p.nextErr
	}
	return selectedFlowRoutePublicationProbe{}, nil
}

type selectedFlowRoutePublicationProbe struct{}

func (selectedFlowRoutePublicationProbe) Retire() error { return nil }

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
			binding := runtimeprocessbinding.Binding{
				ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "selected-route-test", ProcessBootID: uuid.NewString(),
				GenerationGrantID: uuid.NewString(), BundleHash: runForkTestBundleHash,
				RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1,
			}
			identity, err := runtimeflowidentity.NewRunScopedFlowInstance(uuid.NewString(), runtimeflowidentity.RouteForInstancePath("review/inst-1"))
			if err != nil {
				t.Fatal(err)
			}
			attempt, err := runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(uuid.NewString(), identity.RunID, identity.Route.InstancePath, 1, binding)
			if err != nil {
				t.Fatal(err)
			}
			var published []selectedFlowActivation
			err = publishSelectedContractFlowRoute(context.Background(), probe, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}, attempt, diagnostics, &published)
			if probe.stages != 1 || probe.publishes != tc.wantPublish || probe.verifies != tc.wantVerify || len(published) != 1 {
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
