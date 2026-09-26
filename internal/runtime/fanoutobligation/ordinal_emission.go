package fanoutobligation

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/google/uuid"
)

// OrdinalEmission owns the semantic relation between an immutable trigger and
// newly executed work. It is not permission to write: the named chunk commit
// must obtain the intent under its claim and prove the source's fork lineage.
type OrdinalEmission struct {
	lineage       events.EventLineage
	origin        *events.InheritedFanOutOrigin
	deployment    *DeploymentOrigin
	deploymentKey IntentKey
	ordinal       int
	node          string
	source        events.RoutingSource
	depth         int
}

func deploymentOrdinalEventID(key IntentKey, ordinal int) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("swarm.deployment-ordinal.v1\x00%s\x00%d", key.DeploymentFeedID, ordinal))).String()
}

// ValidateCommittedDeploymentOrdinalEvent binds a historical publication to
// the exact feed and ordinal that minted it; an outcome row alone is not enough.
func ValidateCommittedDeploymentOrdinalEvent(intent Intent, ordinal int, eventID string) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	if intent.Request.Deployment == nil || ordinal < 0 || ordinal >= intent.Cursor || eventID != deploymentOrdinalEventID(intent.Request.Key, ordinal) {
		return fmt.Errorf("deployment outcome event differs from its exact feed ordinal")
	}
	return nil
}

// PrepareDeploymentOrdinalEmission preserves the feed/ordinal relation without
// borrowing handler lineage. The row payload is read from the pinned source by
// the serving owner; this projection fixes the event's durable identity.
func PrepareDeploymentOrdinalEmission(intent Intent, ordinal int) (OrdinalEmission, error) {
	if err := intent.Validate(); err != nil {
		return OrdinalEmission{}, err
	}
	if intent.Request.Deployment == nil || ordinal < intent.Cursor || ordinal >= intent.Request.Cardinality {
		return OrdinalEmission{}, fmt.Errorf("deployment ordinal is outside the exact claimed feed suffix")
	}
	source, err := events.NewDeploymentFeedRoutingSource(intent.Request.Deployment.Declaration.FlowPath)
	if err != nil {
		return OrdinalEmission{}, err
	}
	return OrdinalEmission{deployment: intent.Request.Deployment, deploymentKey: intent.Request.Key, ordinal: ordinal, source: source}, nil
}

func PrepareOrdinalEmission(intent Intent, trigger events.Event, ordinal int) (OrdinalEmission, error) {
	if err := intent.Validate(); err != nil {
		return OrdinalEmission{}, err
	}
	capsule := intent.Request.Capsule
	if trigger.ID() != capsule.Lineage.ParentEventID || trigger.RunID() != capsule.Lineage.RunID ||
		trigger.TaskID() != capsule.Lineage.TaskID || trigger.ExecutionMode() != capsule.Lineage.ExecutionMode {
		return OrdinalEmission{}, fmt.Errorf("fan-out trigger disagrees with immutable intent lineage")
	}
	if ordinal < intent.Cursor || ordinal >= intent.Request.Cardinality {
		return OrdinalEmission{}, fmt.Errorf("fan-out ordinal %d is outside the claimed suffix [%d,%d)", ordinal, intent.Cursor, intent.Request.Cardinality)
	}
	return ordinalEmission(intent.Request.Key, intent.Request.PlanRef, intent.Request.Capsule, ordinal)
}

// ValidateCommittedOrdinalEvent checks immutable semantic facts for an already
// committed ordinal. Its callers must prove the exact durable outcome relation;
// unlike preparation, this does not grant a cursor suffix permission to write.
func ValidateCommittedOrdinalEvent(key IntentKey, plan contracts.FanOutPlanRef, capsule Capsule, ordinal int, event events.Event) error {
	projection, err := ordinalEmission(key, plan, capsule, ordinal)
	if err != nil {
		return err
	}
	return projection.ValidateEvent(event)
}

func ordinalEmission(key IntentKey, plan contracts.FanOutPlanRef, capsule Capsule, ordinal int) (OrdinalEmission, error) {
	if err := key.Validate(); err != nil {
		return OrdinalEmission{}, err
	}
	if plan.ElementRef != key.ElementRef || plan.BundleHash == "" || plan.SemanticDigest == "" || ordinal < 0 {
		return OrdinalEmission{}, fmt.Errorf("fan-out ordinal requires exact declaration and immutable plan")
	}
	if err := capsule.Validate(); err != nil {
		return OrdinalEmission{}, err
	}
	projection := OrdinalEmission{
		lineage: capsule.Lineage, node: capsule.NodeKey, source: capsule.ProducerSource, depth: capsule.ChainDepth + 1,
	}
	if key.RunID != capsule.Lineage.RunID {
		declaration, err := key.ElementRef.DeclarationIdentity()
		if err != nil {
			return OrdinalEmission{}, err
		}
		origin, err := events.NewInheritedFanOutOrigin(key.RunID, capsule.Lineage.RunID, capsule.Lineage.ParentEventID,
			key.TriggeringDeliveryID, declaration, plan.BundleHash, plan.SemanticDigest, ordinal)
		if err != nil {
			return OrdinalEmission{}, err
		}
		projection.origin = &origin
	}
	return projection, nil
}

func (p OrdinalEmission) NewEvent(facts events.EventFacts) (events.Event, error) {
	if p.deployment != nil {
		facts.ID = deploymentOrdinalEventID(p.deploymentKey, p.ordinal)
		facts.Type = events.EventType(p.deployment.Declaration.EventName)
		facts.Producer = events.ProducerClaim{Type: events.EventProducerExternal, ID: "deployment-feed"}
		facts.ChainDepth = 0
		facts.RoutingSource = p.source
		return events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{Facts: facts, RunID: p.deploymentKey.RunID})
	}
	if p.node == "" || facts.Producer.Type != events.EventProducerNode || facts.Producer.ID != p.node ||
		facts.ChainDepth != p.depth || facts.RoutingSource != p.source {
		var absent events.Event
		return absent, fmt.Errorf("fan-out emission disagrees with immutable producer, depth or source")
	}
	facts.TaskID, facts.ExecutionMode = p.lineage.TaskID, p.lineage.ExecutionMode
	if p.origin != nil {
		return events.NewInheritedFanOutEvent(events.InheritedFanOutEventInput{Facts: facts, Origin: *p.origin})
	}
	return events.NewChildEvent(events.ChildEventInput{Facts: facts, Lineage: p.lineage})
}

func (p OrdinalEmission) ValidateEvent(event events.Event) error {
	if p.deployment != nil {
		if event.ID() != deploymentOrdinalEventID(p.deploymentKey, p.ordinal) ||
			event.RunID() != p.deploymentKey.RunID || event.Type() != events.EventType(p.deployment.Declaration.EventName) ||
			event.AdmissionClass() != events.EventAdmissionRootIngress || event.ParentEventID() != "" ||
			!events.ProducerIs(event, events.EventProducerExternal, "deployment-feed") || event.ChainDepth() != 0 ||
			event.RoutingSource() != p.source {
			return fmt.Errorf("deployment publication disagrees with exact feed ordinal and root ingress")
		}
		return events.ValidatePersistentEvent(event)
	}
	if p.node == "" || event.Producer().Type() != events.EventProducerNode || event.Producer().ID() != p.node ||
		event.ChainDepth() != p.depth || event.RoutingSource() != p.source ||
		event.TaskID() != p.lineage.TaskID || event.ExecutionMode() != p.lineage.ExecutionMode {
		return fmt.Errorf("fan-out publication disagrees with immutable execution facts")
	}
	origin, inherited := event.InheritedFanOutOrigin()
	if p.origin != nil {
		if !inherited || origin != *p.origin || event.RunID() != origin.RunID() || event.ParentEventID() != "" || event.AdmissionClass() != events.EventAdmissionInheritedFanOut {
			return fmt.Errorf("fan-out publication disagrees with exact inherited origin")
		}
	} else if inherited || event.AdmissionClass() != events.EventAdmissionChild || event.RunID() != p.lineage.RunID || event.ParentEventID() != p.lineage.ParentEventID {
		return fmt.Errorf("ordinary fan-out publication requires exact same-run causal parent")
	}
	return events.ValidatePersistentEvent(event)
}
