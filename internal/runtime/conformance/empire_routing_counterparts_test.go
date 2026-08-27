package conformance

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// These paths identify the Empire evidence inspected at a528e0b. Empire's
// legacy spellings are evidence only; every executable proof below comes from
// the platform's checked-in canonical artifacts.
const (
	empireValidationNodes        = "contracts/flows/validation/nodes.yaml"
	empireTreasuryNodes          = "contracts/flows/treasury/nodes.yaml"
	empireOperatingNodes         = "contracts/flows/operating/nodes.yaml"
	empireComponentScaffoldNodes = "contracts/flows/component-scaffold/nodes.yaml"
	empireSpecRepoNodes          = "contracts/flows/spec-repo/nodes.yaml"
	empireRepoScaffoldNodes      = "contracts/flows/repo-scaffold/nodes.yaml"
)

func TestEmpireValidationCreateMintedKeyCounterpart(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.TemplateCreateMintedKey)
	_, plans := canonicalCounterpartPlans(t, canonicalrouting.TemplateCreateMintedKey)
	plan := requireCounterpartPlan(t, plans, func(plan runtimepinrouting.ConnectRoutePlan) bool {
		return plan.InstanceKey() != nil && plan.InstanceKey().Mode() == runtimecontracts.FlowInputResolutionModeCreate
	})
	if key := plan.InstanceKey().Readback(); key.SourceKind != string(runtimecontracts.FlowInputInstanceSourceGeneratedUUID) || key.SourcePath != runtimecontracts.FlowInputInstanceSourceGeneratedUUIDPath || key.Field != "validation_case_id" {
		t.Fatalf("%s counterpart = %#v, want UUID-minted validation_case_id", empireValidationNodes, plan.InstanceKey())
	}
}

func TestEmpireTreasuryPortfolioFanInCounterpart(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.FanInStream, canonicalrouting.FanInBarrier)
	_, streamPlans := canonicalCounterpartPlans(t, canonicalrouting.FanInStream)
	stream := requireCounterpartPlan(t, streamPlans, func(plan runtimepinrouting.ConnectRoutePlan) bool {
		return plan.FanIn() != nil && plan.FanIn().Aggregation() == runtimepinrouting.ConnectFanInStream
	})
	_, barrierPlans := canonicalCounterpartPlans(t, canonicalrouting.FanInBarrier)
	barrier := requireCounterpartPlan(t, barrierPlans, func(plan runtimepinrouting.ConnectRoutePlan) bool {
		return plan.FanIn() != nil && plan.FanIn().Aggregation() == runtimepinrouting.ConnectFanInBarrier
	})
	if stream.FanIn().Readback().Singleton != "portfolio" || barrier.FanIn().Readback().Singleton != "portfolio" {
		t.Fatalf("%s/%s counterparts do not resolve to the portfolio singleton: stream=%#v barrier=%#v", empireTreasuryNodes, empireOperatingNodes, stream.FanIn(), barrier.FanIn())
	}
}

func TestEmpireComponentScaffoldReplyCounterpart(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.TemplateReply)
	_, plans := canonicalCounterpartPlans(t, canonicalrouting.TemplateReply)
	request := requireCounterpartPlan(t, plans, func(plan runtimepinrouting.ConnectRoutePlan) bool {
		return plan.ReplyRole() == runtimepinrouting.ConnectReplyRoleRequest
	})
	response := requireCounterpartPlan(t, plans, func(plan runtimepinrouting.ConnectRoutePlan) bool {
		return plan.ReplyRole() == runtimepinrouting.ConnectReplyRoleResponse
	})
	if requestReadback, responseReadback := request.ReplyResolution().Readback(), response.ReplyResolution().Readback(); requestReadback.RequesterFlowID == "" || responseReadback.RequesterFlowID != requestReadback.RequesterFlowID {
		t.Fatalf("%s reply counterpart lacks one requester identity: request=%#v response=%#v", empireComponentScaffoldNodes, request.ReplyResolution(), response.ReplyResolution())
	}
}

func TestEmpireKeyedRoutingCounterparts(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.TemplateSelectExisting, canonicalrouting.TemplateSelectOrCreate)
	_, selectPlans := canonicalCounterpartPlans(t, canonicalrouting.TemplateSelectExisting)
	selected := requireCounterpartPlan(t, selectPlans, func(plan runtimepinrouting.ConnectRoutePlan) bool {
		return plan.InstanceKey() != nil && plan.InstanceKey().Mode() == runtimecontracts.FlowInputResolutionModeSelect
	})
	_, selectOrCreatePlans := canonicalCounterpartPlans(t, canonicalrouting.TemplateSelectOrCreate)
	selectedOrCreated := requireCounterpartPlan(t, selectOrCreatePlans, func(plan runtimepinrouting.ConnectRoutePlan) bool {
		return plan.InstanceKey() != nil && plan.InstanceKey().Mode() == runtimecontracts.FlowInputResolutionModeSelectOrCreate
	})
	if selected.InstanceKey().Field().Empty() || selectedOrCreated.InstanceKey().Field().Empty() {
		t.Fatalf("%s/%s/%s keyed counterparts require one carried key: select=%#v select-or-create=%#v", empireValidationNodes, empireSpecRepoNodes, empireRepoScaffoldNodes, selected.InstanceKey(), selectedOrCreated.InstanceKey())
	}
}

func TestEmpireResolutionCounterpartsRequireNoSeventhModeOrMultiPrimaryInstance(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.TemplateCreateMintedKey,
		canonicalrouting.TemplateSelectExisting,
		canonicalrouting.TemplateSelectOrCreate,
		canonicalrouting.TemplateReply,
		canonicalrouting.FanInStream,
		canonicalrouting.FanInBarrier,
	)
	type counterpart struct {
		path     string
		artifact canonicalrouting.ArtifactID
		mode     runtimecontracts.FlowInputResolutionMode
		key      string
	}
	evidence := []counterpart{
		{empireValidationNodes, canonicalrouting.TemplateCreateMintedKey, runtimecontracts.FlowInputResolutionModeCreate, "validation_case_id"},
		{empireTreasuryNodes, canonicalrouting.TemplateSelectExisting, runtimecontracts.FlowInputResolutionModeSelect, "vertical_id"},
		{empireSpecRepoNodes, canonicalrouting.TemplateSelectOrCreate, runtimecontracts.FlowInputResolutionModeSelectOrCreate, "component_id"},
		{empireComponentScaffoldNodes, canonicalrouting.TemplateReply, runtimecontracts.FlowInputResolutionModeReply, "component_id"},
		{empireOperatingNodes, canonicalrouting.FanInStream, runtimecontracts.FlowInputResolutionModeFanIn, "operating_id"},
		{empireOperatingNodes, canonicalrouting.FanInBarrier, runtimecontracts.FlowInputResolutionModeFanIn, "operating_id"},
	}
	for _, item := range evidence {
		bundle, plans := canonicalCounterpartPlans(t, item.artifact)
		if len(plans) == 0 {
			t.Fatalf("%s counterpart %s has no canonical route plan", item.path, item.artifact)
		}
		matched := false
		for _, plan := range plans {
			switch item.mode {
			case runtimecontracts.FlowInputResolutionModeReply:
				matched = matched || plan.ReplyResolution() != nil
			case runtimecontracts.FlowInputResolutionModeFanIn:
				matched = matched || plan.FanIn() != nil
			default:
				matched = matched || (plan.InstanceKey() != nil && plan.InstanceKey().Mode() == item.mode)
			}
		}
		if !matched {
			t.Fatalf("%s intent does not map to canonical mode %s in %s", item.path, runtimecontracts.FlowInputResolutionModeCode(item.mode), item.artifact)
		}
		// Strict bundle loading resolves one primary entity contract per flow.
		// The named key remains routing identity inside that single-entity model.
		if item.mode != runtimecontracts.FlowInputResolutionModeReply && item.mode != runtimecontracts.FlowInputResolutionModeFanIn {
			for _, plan := range plans {
				if plan.InstanceKey() == nil || plan.InstanceKey().Mode() != item.mode {
					continue
				}
				receiver := plan.ReceiverEndpoint().Readback()
				primary, err := bundle.ResolveFlowPrimaryEntity(receiver.FlowID)
				if err != nil || primary.EntityType == "" {
					t.Fatalf("%s receiver %s lacks one primary entity contract: primary=%#v err=%v", item.path, receiver.FlowID, primary, err)
				}
				if item.key == "" {
					t.Fatalf("%s counterpart key is empty", item.path)
				}
			}
		}
	}
}

func canonicalCounterpartPlans(t *testing.T, artifact canonicalrouting.ArtifactID) (*runtimecontracts.WorkflowContractBundle, []runtimepinrouting.ConnectRoutePlan) {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, artifact),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical counterpart %s: %v", artifact, err)
	}
	plans, issues := compiledConnectPlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("lower canonical counterpart %s issues = %#v", artifact, issues)
	}
	return bundle, plans
}

func requireCounterpartPlan(t *testing.T, plans []runtimepinrouting.ConnectRoutePlan, match func(runtimepinrouting.ConnectRoutePlan) bool) runtimepinrouting.ConnectRoutePlan {
	t.Helper()
	for _, plan := range plans {
		if match(plan) {
			return plan
		}
	}
	t.Fatalf("no matching canonical counterpart route in %#v", plans)
	return runtimepinrouting.ConnectRoutePlan{}
}
