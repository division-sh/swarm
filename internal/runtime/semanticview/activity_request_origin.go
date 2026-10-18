package semanticview

import (
	"bytes"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
)

// ActivityRequestOrigin is detached preparation evidence, never execution
// authority. The native reader must reconstruct it again at commit.
type ActivityRequestOrigin struct {
	Request events.Event
	Cuts    []ActivityRequestCut
}

type ActivityRequestCut struct {
	RunID, RequestID                    string
	Revision                            int64
	Source                              correlation.SourceArtifactFact
	ParentPointKind, ParentPointEventID string
	ParentPointInput                    string
	ParentPointRevision                 int64
}

func (o ActivityRequestOrigin) Validate() error {
	if len(o.Cuts) == 0 || o.Request.Type() != "platform.activity_requested" {
		return fmt.Errorf("original activity requires exact request and fixed-cut evidence")
	}
	seen := make(map[string]bool, len(o.Cuts))
	for i, cut := range o.Cuts {
		key := cut.RunID + "/" + cut.RequestID
		if cut.RunID == "" || cut.RequestID == "" || cut.Revision <= 0 || seen[key] {
			return fmt.Errorf("original activity has incomplete or cyclic fixed-cut evidence")
		}
		seen[key] = true
		if err := cut.Source.Validate(); err != nil {
			return err
		}
		if i == len(o.Cuts)-1 {
			if cut.RunID != o.Request.RunID() || cut.RequestID != o.Request.ID() || cut.ParentPointKind != "" || cut.ParentPointEventID != "" || cut.ParentPointInput != "" || cut.ParentPointRevision != 0 {
				return fmt.Errorf("original activity does not end at its producing request")
			}
		} else if cut.ParentPointKind == "" || cut.ParentPointRevision != o.Cuts[i+1].Revision {
			return fmt.Errorf("original activity has crossed parent-cut evidence")
		}
	}
	return nil
}

func (o ActivityRequestOrigin) Source() correlation.SourceArtifactFact {
	if len(o.Cuts) == 0 {
		return correlation.SourceArtifactFact{}
	}
	return o.Cuts[len(o.Cuts)-1].Source
}

func (o ActivityRequestOrigin) Matches(other ActivityRequestOrigin) bool {
	admission, admitted := o.Request.PayloadAdmission()
	otherAdmission, otherAdmitted := other.Request.PayloadAdmission()
	lineage, selected := o.Request.SelectedForkLineage()
	otherLineage, otherSelected := other.Request.SelectedForkLineage()
	return slices.Equal(o.Cuts, other.Cuts) && o.Request.ID() == other.Request.ID() &&
		o.Request.RunID() == other.Request.RunID() && o.Request.Type() == other.Request.Type() &&
		o.Request.ExecutionMode() == other.Request.ExecutionMode() && o.Request.RoutingSource() == other.Request.RoutingSource() &&
		o.Request.CreatedAt().Equal(other.Request.CreatedAt()) && bytes.Equal(o.Request.Payload(), other.Request.Payload()) &&
		o.Request.Producer().Equal(other.Request.Producer()) && o.Request.TaskID() == other.Request.TaskID() &&
		o.Request.ParentEventID() == other.Request.ParentEventID() && o.Request.ChainDepth() == other.Request.ChainDepth() &&
		o.Request.AdmissionClass() == other.Request.AdmissionClass() &&
		reflect.DeepEqual(o.Request.NormalizedEnvelope(), other.Request.NormalizedEnvelope()) &&
		admitted == otherAdmitted && reflect.DeepEqual(admission, otherAdmission) &&
		selected == otherSelected && reflect.DeepEqual(lineage, otherLineage)
}

type activityRequestScope struct{ run, request string }

// WithActivityHistory retains only declarations compiled from the named
// artifact, without replacing the current source's loop or output roles.
func (c OriginalLoopCarriage) WithActivityHistory(source Source) (OriginalLoopCarriage, error) {
	if c.state == nil {
		return OriginalLoopCarriage{}, fmt.Errorf("activity history requires original carriage")
	}
	history, err := CompileOriginalLoopCarriage(source)
	if err != nil {
		return OriginalLoopCarriage{}, err
	}
	state := *c.state
	state.activityHistory = maps.Clone(state.activityHistory)
	if state.activityHistory == nil {
		state.activityHistory = make(map[string]*originalLoopCarriage)
	}
	state.activityHistory[history.state.bundleHash] = history.state
	return OriginalLoopCarriage{state: &state}, nil
}

func (c OriginalLoopCarriage) WithActivityOrigin(origin ActivityRequestOrigin) (OriginalLoopCarriage, error) {
	if c.state == nil {
		return OriginalLoopCarriage{}, fmt.Errorf("activity origin requires original carriage")
	}
	if err := origin.Validate(); err != nil {
		return OriginalLoopCarriage{}, err
	}
	if _, err := c.activityOriginDeclaration(origin); err != nil {
		return OriginalLoopCarriage{}, err
	}
	key := activityRequestScope{origin.Cuts[0].RunID, origin.Cuts[0].RequestID}
	state := *c.state
	state.activityOrigins = maps.Clone(state.activityOrigins)
	if state.activityOrigins == nil {
		state.activityOrigins = make(map[activityRequestScope]ActivityRequestOrigin)
	}
	if prior, found := state.activityOrigins[key]; found && !prior.Matches(origin) {
		return OriginalLoopCarriage{}, fmt.Errorf("activity preparation contains conflicting original provenance")
	}
	origin.Cuts = slices.Clone(origin.Cuts)
	state.activityOrigins[key] = origin
	return OriginalLoopCarriage{state: &state}, nil
}

func (c OriginalLoopCarriage) activityOriginDeclaration(origin ActivityRequestOrigin) (OriginalLoopCarriage, error) {
	if c.state == nil {
		return OriginalLoopCarriage{}, fmt.Errorf("activity declaration carriage is absent")
	}
	hash := origin.Source().BundleHash()
	if c.state.bundleHash == hash {
		return c, nil
	}
	if declaration := c.state.activityHistory[hash]; declaration != nil {
		return OriginalLoopCarriage{state: declaration}, nil
	}
	return OriginalLoopCarriage{}, fmt.Errorf("activity origin has no exact prepared source artifact")
}

func (c OriginalLoopCarriage) ResolveOriginalActivityApproval(origin ActivityRequestOrigin, node identity.ExecutableNode, handler, activity, tool string) (string, error) {
	declaration, err := c.requirePreparedActivityOrigin(origin)
	if err != nil {
		return "", err
	}
	return declaration.ResolveActivityApproval(node, handler, activity, tool)
}

func (c OriginalLoopCarriage) ResolveOriginalActivity(origin ActivityRequestOrigin, node identity.ExecutableNode, handler, activity, tool string) (LoopRevisionRole, bool, error) {
	declaration, err := c.requirePreparedActivityOrigin(origin)
	if err != nil {
		return LoopRevisionRole{}, false, err
	}
	return declaration.ResolveActivity(node, handler, activity, tool)
}

func (c OriginalLoopCarriage) requirePreparedActivityOrigin(origin ActivityRequestOrigin) (OriginalLoopCarriage, error) {
	if err := origin.Validate(); err != nil {
		return OriginalLoopCarriage{}, err
	}
	if c.state == nil {
		return OriginalLoopCarriage{}, fmt.Errorf("activity origin has no prepared declaration")
	}
	prepared, found := c.state.activityOrigins[activityRequestScope{origin.Cuts[0].RunID, origin.Cuts[0].RequestID}]
	if !found || !prepared.Matches(origin) {
		return OriginalLoopCarriage{}, fmt.Errorf("activity origin changed between preparation and commit")
	}
	return c.activityOriginDeclaration(origin)
}
