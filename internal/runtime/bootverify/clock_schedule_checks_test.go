package bootverify

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestClockScheduleBareEventAndConsumerLaw(t *testing.T) {
	for _, tc := range []struct {
		name, fields, pins, nodes, agents, want string
	}{
		{name: "root export", pins: "pins:\n  outputs: [poll.tick]\n"},
		{name: "actual local handler", nodes: "observer:\n  event_handlers:\n    poll.tick: {}\n"},
		{name: "actual local agent", agents: "observer:\n  intent: {inline: 'Observe the clock.'}\n  model: regular\n  subscriptions: [poll.tick]\n  emit_events: []\n"},
		{name: "unsubscribed local agent", agents: "observer:\n  intent: {inline: 'Observe nothing.'}\n  model: regular\n  subscriptions: []\n  emit_events: []\n", want: "actual consumer"},
		{name: "dangling emission", want: "actual consumer"},
		{name: "input is not consumer", pins: "pins:\n  inputs: [poll.tick]\n", want: "actual consumer"},
		{name: "required field", fields: "  reason: text\n", pins: "pins:\n  outputs: [poll.tick]\n", want: "bare event"},
		{name: "optional field", fields: "  reason: text?\n", pins: "pins:\n  outputs: [poll.tick]\n", want: "bare event"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for name, content := range map[string]string{
				"schema.yaml": "stages: []\nschedules:\n  poll: {every: 5m, emit: poll.tick}\n" + tc.pins,
				"events.yaml": "poll.tick:\n" + tc.fields,
			} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.nodes != "" {
				if err := os.WriteFile(filepath.Join(root, "nodes.yaml"), []byte(tc.nodes), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.agents != "" {
				if err := os.WriteFile(filepath.Join(root, "agents.yaml"), []byte(tc.agents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			repo := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repo, root, contracts.DefaultPlatformSpecFile(repo))
			findings := checkClockScheduleValidation(newCheckerContext(context.Background(), semanticview.Wrap(bundle), Options{}))
			if tc.want == "" {
				if len(findings) != 0 {
					t.Fatalf("lawful clock refused: %#v", findings)
				}
				if report := Run(t.Context(), semanticview.Wrap(bundle), Options{Purpose: StructuralValidation}); len(report.Errors()) != 0 {
					t.Fatalf("lawful clock failed full structural verification: %#v", report.Errors())
				}
			} else if len(findings) != 1 || !strings.Contains(findings[0].Message, tc.want) {
				t.Fatalf("missing exact clock refusal %q: %#v", tc.want, findings)
			}
			t.Run("readback projection is non-authoritative", func(t *testing.T) {
				source := clockScheduleSchemaProbe{
					Source: semanticview.Wrap(bundle), hideFields: tc.fields != "",
				}
				projected := checkClockScheduleValidation(newCheckerContext(t.Context(), source, Options{}))
				if tc.want == "" {
					if len(projected) != 0 {
						t.Fatalf("readback projection overruled admitted bare schema: %#v", projected)
					}
				} else if len(projected) != 1 || !strings.Contains(projected[0].Message, tc.want) {
					t.Fatalf("readback projection overruled admitted schema refusal %q: %#v", tc.want, projected)
				}
			})
			if tc.want == "" {
				for _, unavailable := range []struct {
					name string
					err  error
				}{
					{name: "missing admitted schema"},
					{name: "failed admitted schema", err: errors.New("compiled schema unavailable")},
				} {
					t.Run(unavailable.name, func(t *testing.T) {
						source := clockScheduleSchemaProbe{Source: semanticview.Wrap(bundle), hideFields: true, unavailable: true, schemaErr: unavailable.err}
						findings := checkClockScheduleValidation(newCheckerContext(t.Context(), source, Options{}))
						if len(findings) != 1 || !strings.Contains(findings[0].Message, "requires an admitted event schema") {
							t.Fatalf("unavailable compiled schema fell back to readback (error %v): %#v", unavailable.err, findings)
						}
					})
				}
			}
		})
	}
}

type clockScheduleSchemaProbe struct {
	semanticview.Source
	hideFields  bool
	unavailable bool
	schemaErr   error
}

func (s clockScheduleSchemaProbe) ResolveFlowEventCatalogEntry(flowID, eventType string) (contracts.EventCatalogEntry, string, bool) {
	entry, key, found := s.Source.ResolveFlowEventCatalogEntry(flowID, eventType)
	entry.Payload = contracts.EventPayloadSpec{}
	if !s.hideFields {
		entry.Payload = contracts.EventPayloadSpec{
			Properties: map[string]contracts.EventFieldSpec{"readback_only": {Type: "text"}},
			Required:   []string{"readback_only"},
		}
	}
	return entry, key, found
}

func (s clockScheduleSchemaProbe) ResolveEffectiveCompiledFlowEventSchema(flowID, eventType string) (contracts.CompiledEventSchema, bool, error) {
	if s.unavailable {
		return contracts.CompiledEventSchema{}, false, s.schemaErr
	}
	return s.Source.ResolveEffectiveCompiledFlowEventSchema(flowID, eventType)
}

func TestClockScheduleRequiresActualConnectionToPrivateConsumer(t *testing.T) {
	for _, connected := range []bool{false, true} {
		t.Run(map[bool]string{false: "unconnected same name", true: "compiled connection"}[connected], func(t *testing.T) {
			root := canonicalrouting.CopyParentConnectClock(t, connected)
			repo := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repo, root, contracts.DefaultPlatformSpecFile(repo))
			findings := checkClockScheduleValidation(newCheckerContext(t.Context(), semanticview.Wrap(bundle), Options{}))
			if connected && len(findings) != 0 {
				t.Fatalf("real compiled consumer refused: %#v", findings)
			}
			if connected {
				if report := Run(t.Context(), semanticview.Wrap(bundle), Options{Purpose: StructuralValidation}); len(report.Errors()) != 0 {
					t.Fatalf("connected clock failed full structural verification: %#v", report.Errors())
				}
			}
			if !connected && (len(findings) != 1 || !strings.Contains(findings[0].Message, "actual consumer")) {
				t.Fatalf("same-name child manufactured consumer: %#v", findings)
			}
		})
	}
}
