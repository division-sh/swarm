package runtime

import (
	"bytes"
	"fmt"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type s1CallerDirectedTriggerInputs struct {
	semanticview.Source
	pins []runtimecontracts.CompiledFlowInputPin
}

func (s s1CallerDirectedTriggerInputs) FlowInputEventPins(flow string) []runtimecontracts.CompiledFlowInputPin {
	if flow == "." {
		return append([]runtimecontracts.CompiledFlowInputPin(nil), s.pins...)
	}
	return s.Source.FlowInputEventPins(flow)
}

func TestS1DirectoryTriggerSchemasUseCapturedInputEvidence(t *testing.T) {
	const genuine = "inbound.team-bot.raw.genuine"
	const foreign = "inbound.team-bot.raw.foreign"
	membersFor := func(event string) map[string][]byte {
		members := s1EffectiveTriggerMembers()
		members["team-bot/trigger.yaml"] = bytes.Replace(members["team-bot/trigger.yaml"], []byte("event_name: {literal: raw}"), []byte("event_name: {template: 'raw.{event_type}'}"), 1)
		if event != "" {
			members["schema.yaml"] = []byte(fmt.Sprintf("stages: []\npins:\n  inputs: [%s]\n  outputs: [%s]\n", event, event))
		}
		return members
	}
	foreignBundle, _, _ := loadS1EffectiveProviderScope(t, membersFor(foreign))
	foreignPins := foreignBundle.FlowInputEventPins(".")
	if len(foreignPins) != 1 || foreignPins[0].EventType() != foreign {
		t.Fatal("fixture lost its separately compiled foreign input")
	}
	for _, cell := range []struct {
		name, event string
		pins        []runtimecontracts.CompiledFlowInputPin
	}{
		{"insert", "", foreignPins},
		{"replace", genuine, foreignPins},
		{"delete", genuine, nil},
	} {
		t.Run(cell.name, func(t *testing.T) {
			bundle, rooted, _ := loadS1EffectiveProviderScope(t, membersFor(cell.event))
			fact := sourceartifactfixture.FactFor(rooted.Artifact())
			first, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact})
			if err != nil {
				t.Fatal(err)
			}
			readback := s1CallerDirectedTriggerInputs{Source: first.Source(), pins: cell.pins}
			after, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: readback, SourceArtifactFact: fact})
			if err != nil || !after.Identity().Equal(first.Identity()) {
				t.Fatalf("caller input readback replaced captured trigger selection: %v", err)
			}
			if semanticview.ResolveEventSchema(after.Source(), ".", foreign).HasSchema || semanticview.ResolveEventSchema(after.Source(), "", foreign).HasSchema {
				t.Error("a foreign input pin minted a provider schema")
			}
			if cell.event != "" {
				input, found := after.Source().FlowInputEventPin(".", genuine)
				producer, bound := input.ProducerEventSchema()
				output, outputFound := after.Source().FlowOutputEventPin(".", genuine)
				emitted, outputBound := output.EventSchema()
				if !found || !bound || !outputFound || !outputBound || !compiledSchemaHasField(producer, "provider") || emitted.AcceptanceSchemaDigest() != producer.AcceptanceSchemaDigest() {
					t.Fatal("caller readback erased the captured raw input/output binding")
				}
			}
			if len(after.Source().SemanticCapabilities().ProviderTriggerOutputBindings()) != 0 || len(after.Source().SemanticCapabilities().ProviderIngressEvents()) != 0 {
				t.Fatal("input readback created publication or ingress authority")
			}
		})
	}
}
