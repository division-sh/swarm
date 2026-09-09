package loopruntime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
)

type forkLoopScope struct {
	flow string
	loop string
}

type forkActivationPair struct {
	source Activation
	child  Activation
}

// ForkCorrespondence translates references from an admitted source snapshot.
// The caller owns admission of that snapshot's run/entity context. Translation
// establishes historical ownership, never permission to execute a generation.
type ForkCorrespondence struct {
	state *forkCorrespondence
}

type forkCorrespondence struct {
	runID    string
	entityID string
	pairs    map[forkLoopScope]forkActivationPair
}

// ForkSourceReference and ForkChildReference cannot be interchanged or created
// from a partial label. They retain the exact correspondence used for admission.
type ForkSourceReference struct {
	owner      *forkCorrespondence
	generation attemptgeneration.Generation
}

type ForkChildReference struct {
	source     ForkSourceReference
	generation attemptgeneration.Generation
}

// RequireDestination also rejects a zero-value correspondence on non-loop paths.
func (c *ForkCorrespondence) RequireDestination(runID, entityID string) error {
	if c == nil || c.state == nil || c.state.pairs == nil || c.state.runID == "" || c.state.entityID == "" || c.state.runID != runID || c.state.entityID != entityID {
		return fmt.Errorf("fork correspondence does not own the exact child run and entity")
	}
	return nil
}

func NewForkCorrespondence(source []Activation, childRunID, childEntityID string) (*ForkCorrespondence, error) {
	if childRunID == "" || childRunID != strings.TrimSpace(childRunID) || childEntityID == "" || childEntityID != strings.TrimSpace(childEntityID) {
		return nil, fmt.Errorf("fork correspondence requires exact child run and entity identity")
	}
	inventory, err := forkActivationInventory(source)
	if err != nil {
		return nil, fmt.Errorf("source fork correspondence: %w", err)
	}
	c := &ForkCorrespondence{state: &forkCorrespondence{runID: childRunID, entityID: childEntityID, pairs: make(map[forkLoopScope]forkActivationPair, len(inventory))}}
	for scope, activation := range inventory {
		child, err := Fork(activation, childRunID, childEntityID)
		if err != nil {
			return nil, err
		}
		c.state.pairs[scope] = forkActivationPair{source: activation, child: child}
	}
	return c, nil
}

func forkActivationInventory(activations []Activation) (map[forkLoopScope]Activation, error) {
	out := make(map[forkLoopScope]Activation, len(activations))
	identities := make(map[string]struct{}, len(activations))
	for _, a := range activations {
		if err := a.Validate(); err != nil {
			return nil, err
		}
		g := a.Generation()
		if a.FlowID != g.FlowID || a.LoopID != g.LoopID || a.ActivationID != g.ActivationID || a.RevisionField != g.RevisionField || a.RevisionID != g.RevisionID || !a.OwnsGeneration(g) {
			return nil, fmt.Errorf("loop activation %s has noncanonical generation evidence", a.ActivationID)
		}
		scope := forkLoopScope{a.FlowID, a.LoopID}
		if _, exists := out[scope]; exists {
			return nil, fmt.Errorf("duplicate loop activation evidence for flow %q loop %q", a.FlowID, a.LoopID)
		}
		if _, exists := identities[a.ActivationID]; exists {
			return nil, fmt.Errorf("loop activation identity %s has multiple scope owners", a.ActivationID)
		}
		identities[a.ActivationID] = struct{}{}
		out[scope] = a
	}
	return out, nil
}

// ProjectedActivations returns detached values in canonical scope order.
func (c *ForkCorrespondence) ProjectedActivations() []Activation {
	if c == nil || c.state == nil {
		return nil
	}
	out := make([]Activation, 0, len(c.state.pairs))
	for _, pair := range c.state.pairs {
		out = append(out, pair.child)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FlowID != out[j].FlowID {
			return out[i].FlowID < out[j].FlowID
		}
		return out[i].LoopID < out[j].LoopID
	})
	return out
}

func (c *ForkCorrespondence) AdmitSource(g attemptgeneration.Generation) (ForkSourceReference, error) {
	if c == nil || c.state == nil || !g.Valid() || g != g.Normalize() {
		return ForkSourceReference{}, fmt.Errorf("source fork generation is not canonical")
	}
	pair, ok := c.state.pairs[forkLoopScope{g.FlowID, g.LoopID}]
	if !ok || !pair.source.OwnsGeneration(g) {
		return ForkSourceReference{}, fmt.Errorf("source fork generation is not owned at the admitted revision")
	}
	return ForkSourceReference{owner: c.state, generation: g}, nil
}

// AdmitSourceContext reads only the exact projection produced by Context. Frozen
// business maps are not contexts and must never be searched for matching values.
func (c *ForkCorrespondence) AdmitSourceContext(context map[string]any) (ForkSourceReference, error) {
	if len(context) != 7 {
		return ForkSourceReference{}, fmt.Errorf("loop context must contain its complete seven-field projection")
	}
	var evidence struct {
		FlowID        string `json:"flow_id"`
		LoopID        string `json:"id"`
		ActivationID  string `json:"activation_id"`
		RevisionField string `json:"revision_field"`
		RevisionID    string `json:"revision_id"`
		Attempt       int    `json:"attempt"`
		MaxAttempts   int    `json:"max_attempts"`
	}
	raw, err := json.Marshal(context)
	if err != nil {
		return ForkSourceReference{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return ForkSourceReference{}, fmt.Errorf("decode original loop context: %w", err)
	}
	reference, err := c.AdmitSource(attemptgeneration.Generation{FlowID: evidence.FlowID, LoopID: evidence.LoopID,
		ActivationID: evidence.ActivationID, RevisionField: evidence.RevisionField, RevisionID: evidence.RevisionID, Attempt: evidence.Attempt})
	if err != nil {
		return ForkSourceReference{}, err
	}
	if c.state.pairs[forkLoopScope{evidence.FlowID, evidence.LoopID}].source.MaxAttempts != evidence.MaxAttempts {
		return ForkSourceReference{}, fmt.Errorf("original loop context disagrees with its activation cap")
	}
	return reference, nil
}

func (r ForkChildReference) Context() (map[string]any, error) {
	if r.source.owner == nil {
		return nil, fmt.Errorf("child loop context requires admitted correspondence")
	}
	c := &ForkCorrespondence{state: r.source.owner}
	want, err := c.Bind(r.source)
	if err != nil || want.generation != r.generation {
		return nil, fmt.Errorf("child loop context disagrees with its correspondence")
	}
	context := c.state.pairs[forkLoopScope{r.generation.FlowID, r.generation.LoopID}].child.Context()
	context["attempt"], context["revision_id"] = r.generation.Attempt, r.generation.RevisionID
	return context, nil
}

func (r ForkChildReference) RequireDestination(runID, entityID string) error {
	c := &ForkCorrespondence{state: r.source.owner}
	if err := c.RequireDestination(runID, entityID); err != nil {
		return err
	}
	want, err := c.Bind(r.source)
	if err != nil || want.generation != r.generation {
		return fmt.Errorf("child reference contradicts its destination correspondence")
	}
	return nil
}

// AdmitSourceRevision is for a declared revision-field reference. Its caller
// must establish the original declaration role before supplying the exact scope.
// Lookup is confined to that activation's owned history, never other loops or
// the child's current revision.
func (c *ForkCorrespondence) AdmitSourceRevision(flowID, loopID, field, revision string) (ForkSourceReference, error) {
	if c == nil || c.state == nil || revision == "" || revision != strings.TrimSpace(revision) {
		return ForkSourceReference{}, fmt.Errorf("source revision reference is not canonical")
	}
	pair, ok := c.state.pairs[forkLoopScope{flowID, loopID}]
	if !ok || field != pair.source.RevisionField {
		return ForkSourceReference{}, fmt.Errorf("source revision declaration has no exact activation")
	}
	g := pair.source.Generation()
	for attempt := 1; attempt <= pair.source.Attempt; attempt++ {
		if revisionID(pair.source.ActivationID, attempt) == revision {
			g.Attempt, g.RevisionID = attempt, revision
			return c.AdmitSource(g)
		}
	}
	return ForkSourceReference{}, fmt.Errorf("source revision is not owned at the admitted revision")
}

// AdmitSourceKey completes only the field omitted by the canonical key codec.
// Every encoded coordinate must already match the admitted source activation.
func (c *ForkCorrespondence) AdmitSourceKey(g attemptgeneration.Generation) (ForkSourceReference, error) {
	if c == nil || c.state == nil || g.RevisionField != "" || g != g.Normalize() {
		return ForkSourceReference{}, fmt.Errorf("source generation key is not canonical")
	}
	pair, ok := c.state.pairs[forkLoopScope{g.FlowID, g.LoopID}]
	if !ok {
		return ForkSourceReference{}, fmt.Errorf("source generation key has no admitted activation")
	}
	g.RevisionField = pair.source.RevisionField
	return c.AdmitSource(g)
}

func (c *ForkCorrespondence) Bind(ref ForkSourceReference) (ForkChildReference, error) {
	if c == nil || c.state == nil || ref.owner != c.state {
		return ForkChildReference{}, fmt.Errorf("source reference belongs to a different fork correspondence")
	}
	if _, err := c.AdmitSource(ref.generation); err != nil {
		return ForkChildReference{}, err
	}
	g, err := ForkGeneration(ref.generation, c.state.runID, c.state.entityID)
	if err != nil {
		return ForkChildReference{}, err
	}
	return ForkChildReference{source: ref, generation: g}, nil
}

func (r ForkSourceReference) Generation() attemptgeneration.Generation { return r.generation }
func (r ForkChildReference) Generation() attemptgeneration.Generation  { return r.generation }
func (r ForkChildReference) Source() ForkSourceReference               { return r.source }

// ValidateChild checks actual child evidence, allowing lawful repeat/close after
// initial projection. A current child list cannot supply missing source evidence.
func (c *ForkCorrespondence) ValidateChild(ref ForkChildReference, actual []Activation) error {
	expected, err := c.Bind(ref.source)
	if err != nil {
		return err
	}
	if expected.generation != ref.generation {
		return fmt.Errorf("child reference contradicts its fork correspondence")
	}
	inventory, err := forkActivationInventory(actual)
	if err != nil {
		return fmt.Errorf("child fork correspondence: %w", err)
	}
	g := ref.generation
	pair := c.state.pairs[forkLoopScope{g.FlowID, g.LoopID}]
	a, ok := inventory[forkLoopScope{g.FlowID, g.LoopID}]
	if !ok || a.MaxAttempts != pair.child.MaxAttempts || a.Attempt < pair.child.Attempt || !a.OwnsGeneration(g) {
		return fmt.Errorf("child fork generation lacks exact activation ownership")
	}
	return nil
}

// AdmitChild is explicitly for already-child-bound evidence, not a fallback from
// failed source admission. The attempt is mapped back only within its exact pair.
func (c *ForkCorrespondence) AdmitChild(g attemptgeneration.Generation, actual []Activation) (ForkChildReference, error) {
	if c == nil || c.state == nil || !g.Valid() || g != g.Normalize() {
		return ForkChildReference{}, fmt.Errorf("child fork generation is not canonical")
	}
	pair, ok := c.state.pairs[forkLoopScope{g.FlowID, g.LoopID}]
	if !ok {
		return ForkChildReference{}, fmt.Errorf("child fork generation has no admitted source")
	}
	source := pair.source.Generation()
	source.Attempt = g.Attempt
	source.RevisionID = revisionID(source.ActivationID, source.Attempt)
	ref, err := c.AdmitSource(source)
	if err != nil {
		return ForkChildReference{}, err
	}
	child, err := c.Bind(ref)
	if err != nil {
		return ForkChildReference{}, err
	}
	if child.generation != g {
		return ForkChildReference{}, fmt.Errorf("child fork generation contradicts its exact source")
	}
	if err := c.ValidateChild(child, actual); err != nil {
		return ForkChildReference{}, err
	}
	return child, nil
}
