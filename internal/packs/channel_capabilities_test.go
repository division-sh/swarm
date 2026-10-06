package packs_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/yamlsource"
)

const channelCapabilityFixture = `capabilities:
  card_render: true
  reply_to_reference: true
  actions_as_buttons: false
  actions_as_text: true
  edit: false
  acknowledgment: false
  inbox_listing: true
`

func TestChannelCapabilitiesRequireCompleteTextReplyBaseline(t *testing.T) {
	for mask := 0; mask < 128; mask++ {
		t.Run(fmt.Sprintf("vector_%07b", mask), func(t *testing.T) {
			vector := packs.ChannelCapabilityVector{
				CardRender: mask&1 != 0, ReplyToReference: mask&2 != 0,
				ActionsAsButtons: mask&4 != 0, ActionsAsText: mask&8 != 0,
				Edit: mask&16 != 0, Acknowledgment: mask&32 != 0,
				InboxListing: mask&64 != 0,
			}
			capabilities, err := packs.CompileChannelCapabilities(vector)
			want := vector.CardRender && vector.ReplyToReference && vector.ActionsAsText && vector.InboxListing
			if (err == nil) != want {
				t.Fatalf("baseline admission=%v, want %v: %v", err == nil, want, err)
			}
			if want && capabilities.Vector() != vector {
				t.Fatalf("admitted vector changed: %+v", capabilities.Vector())
			}
			body, marshalErr := json.Marshal(map[string]any{"capabilities": vector})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			fromSource, sourceErr := admitChannelCapabilities(t, body)
			if (sourceErr == nil) != want || (want && fromSource.Vector() != capabilities.Vector()) {
				t.Fatalf("source/typed admission differs: %+v: %v", fromSource.Vector(), sourceErr)
			}
		})
	}
}

func TestChannelCapabilitySourceAdmissionRequiresEveryBoolean(t *testing.T) {
	for _, capability := range packs.ChannelCapabilityNames() {
		for _, state := range []string{"missing", "null", "empty_text", "empty_list", "empty_map", "valid", "merge"} {
			t.Run(string(capability)+"/"+state, func(t *testing.T) {
				body := channelPresenceBody(t, []byte(channelCapabilityFixture), []string{"capabilities", string(capability)}, state)
				_, err := admitChannelCapabilities(t, body)
				want := state == "valid" || state == "merge"
				if (err == nil) != want {
					t.Fatalf("admission=%v, want %v: %v", err == nil, want, err)
				}
				if !want && (!strings.Contains(err.Error(), string(capability)) || !strings.Contains(err.Error(), "channel.yaml:")) {
					t.Fatalf("failure lost exact field/source: %v", err)
				}
			})
		}
		for _, scalar := range []string{"'false'", "'true'", "0", "1", "no", "yes"} {
			t.Run(string(capability)+"/scalar_"+scalar, func(t *testing.T) {
				body := strings.Replace(channelCapabilityFixture, string(capability)+": "+capabilityFixtureValue(capability), string(capability)+": "+scalar, 1)
				if _, err := admitChannelCapabilities(t, []byte(body)); err == nil {
					t.Fatal("non-boolean capability admitted")
				}
			})
		}
	}
	for _, extra := range []string{"  menu: false\n", "  edit: false\n", "  acknowledgment: null\n"} {
		if _, err := admitChannelCapabilities(t, []byte(channelCapabilityFixture+extra)); err == nil {
			t.Fatalf("unknown/duplicate capability admitted: %q", extra)
		}
	}
	for _, capability := range packs.ChannelTextReplyBaselineCapabilities() {
		body := strings.Replace(channelCapabilityFixture, string(capability)+": true", string(capability)+": false", 1)
		var missing *packs.RequiredChannelBaselineCapabilityError
		_, err := admitChannelCapabilities(t, []byte(body))
		if !errors.As(err, &missing) || missing.Capability != capability || !strings.Contains(err.Error(), "[\"capabilities\"][\""+string(capability)+"\"]") {
			t.Fatalf("baseline false field not precisely refused: %v", err)
		}
	}
}

func TestChannelCapabilityOptionalRefusalIsTyped(t *testing.T) {
	capabilities, err := admitChannelCapabilities(t, []byte(channelCapabilityFixture))
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range packs.ChannelCapabilityNames() {
		err := capabilities.Require(capability)
		if capabilityFixtureValue(capability) == "true" {
			if err != nil {
				t.Fatalf("supported capability %s refused: %v", capability, err)
			}
			continue
		}
		var refused *packs.UnsupportedChannelCapabilityError
		if !errors.As(err, &refused) || refused.Capability != capability || !strings.Contains(err.Error(), string(capability)) {
			t.Fatalf("capability %s did not return exact typed refusal: %v", capability, err)
		}
	}
	if err := capabilities.Require(packs.ChannelCapability("unknown")); err == nil {
		t.Fatal("unknown capability accepted")
	}
	var zero packs.CompiledChannelCapabilities
	if err := zero.Require(packs.ChannelCapabilityCardRender); err == nil {
		t.Fatal("unadmitted capability vector grants baseline authority")
	}
}

func TestChannelCapabilitiesMaterializationIsImmutable(t *testing.T) {
	capabilities, err := admitChannelCapabilities(t, []byte(channelCapabilityFixture))
	if err != nil {
		t.Fatal(err)
	}
	vector := capabilities.Vector()
	vector.Edit = true
	vector.CardRender = false
	if err := capabilities.Require(packs.ChannelCapabilityEdit); err == nil || !capabilities.Vector().CardRender {
		t.Fatal("mutating returned vector changed admitted capability authority")
	}
	names := packs.ChannelCapabilityNames()
	names[0] = "edited"
	if packs.ChannelCapabilityNames()[0] == "edited" {
		t.Fatal("caller changed canonical capability inventory")
	}
	body, err := json.Marshal(capabilities)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]bool
	if err := json.Unmarshal(body, &fields); err != nil || len(fields) != 7 {
		t.Fatalf("complete boolean readback=%s: %v", body, err)
	}
	for _, capability := range packs.ChannelCapabilityNames() {
		value, present := fields[string(capability)]
		if !present || value != (capabilityFixtureValue(capability) == "true") {
			t.Fatalf("readback lost explicit false/presence for %s", capability)
		}
	}
	if err := json.Unmarshal(body, &capabilities); err == nil {
		t.Fatal("JSON bypassed typed capability admission")
	}
	var zero packs.CompiledChannelCapabilities
	if _, err := json.Marshal(zero); err == nil {
		t.Fatal("unadmitted vector produced valid capability readback")
	}
}

func TestChannelCapabilityContractMatchesNormativeTextReplyBaseline(t *testing.T) {
	body, err := os.ReadFile("../../platform-spec.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		ToolModel struct {
			Channel struct {
				Capabilities struct {
					Fields   []packs.ChannelCapability `yaml:"exact_fields"`
					Baseline struct {
						Language string                    `yaml:"language"`
						Required []packs.ChannelCapability `yaml:"required_capabilities"`
					} `yaml:"text_reply_baseline"`
				} `yaml:"capability_model"`
			} `yaml:"hitl_channel_pack_interface"`
		} `yaml:"tool_model"`
	}
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Decode(&document); err != nil {
		t.Fatal(err)
	}
	contract := document.ToolModel.Channel.Capabilities
	if !reflect.DeepEqual(contract.Fields, packs.ChannelCapabilityNames()) ||
		!reflect.DeepEqual(contract.Baseline.Required, packs.ChannelTextReplyBaselineCapabilities()) || contract.Baseline.Language != "en" {
		t.Fatalf("normative capability/baseline drift: %+v", contract)
	}
}

func admitChannelCapabilities(t *testing.T, body []byte) (packs.CompiledChannelCapabilities, error) {
	t.Helper()
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return packs.CompiledChannelCapabilities{}, err
	}
	field, err := snapshot.Document("channel.yaml").Root().Lookup("capabilities")
	if err != nil {
		return packs.CompiledChannelCapabilities{}, err
	}
	return packs.AdmitChannelCapabilities(field.Value)
}

func capabilityFixtureValue(capability packs.ChannelCapability) string {
	switch capability {
	case packs.ChannelCapabilityCardRender, packs.ChannelCapabilityReplyToReference,
		packs.ChannelCapabilityActionsAsText, packs.ChannelCapabilityInboxListing:
		return "true"
	default:
		return "false"
	}
}
