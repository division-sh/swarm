package contracts

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestReceiverConfigurationRetiredForms(t *testing.T) {
	for _, value := range []string{"null", "{}", "[]", "false", "''", "{variables: {brief: text}}"} {
		t.Run("variables/"+value, func(t *testing.T) {
			_, err := admitSchemaFragment("name: retired\ninstance_variables: " + value + "\n")
			if err == nil || !strings.Contains(err.Error(), "instance_variables") || !strings.Contains(err.Error(), "is not supported") {
				t.Fatalf("retired variable form admitted: %v", err)
			}
		})
	}
	for _, value := range []string{"null", "{}", "{brief: payload.brief}", "{brief: payload.settings.brief}"} {
		t.Run("initialize/"+value, func(t *testing.T) {
			_, err := admitSchemaFragment("pins:\n  inputs:\n    - event: work.ready\n      initialize: " + value + "\n")
			if err == nil {
				t.Fatal("retired input mapping admitted")
			}
		})
	}
	if _, err := admitSchemaFragment("pins: {inputs: [work.ready], outputs: [work.done]}\n"); err != nil {
		t.Fatal(err)
	}
}

func TestReceiverConfigurationTypedCarriersRetired(t *testing.T) {
	for _, row := range []struct {
		value any
		field string
	}{{FlowSchemaDocument{}, "InstanceVariables"}, {FlowInputEventPin{}, "Initialize"}, {FlowPinCompilationContext{}, "Configuration"}} {
		if _, found := reflect.TypeOf(row.value).FieldByName(row.field); found {
			t.Fatalf("retired carrier restored: %T.%s", row.value, row.field)
		}
	}
	if _, found := reflect.TypeOf(CompiledFlowInputPin{}).MethodByName("Initialization"); found {
		t.Fatal("compiled initialization accessor restored")
	}
}

func TestCompiledInputPinsOwnBoundaryAndSchemaOnly(t *testing.T) {
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyReceiverInitialization(t), DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	pin, found := bundle.FlowInputEventPin("account", "account.ready")
	if !found || pin.Digest() == "" {
		t.Fatal("names-only pin lost exact compiled schema evidence")
	}
	if pin.EventType() != "account.ready" {
		t.Fatal("pin changed receiver-local identity")
	}
	before := pin.Digest()
	if err := CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	again, found := bundle.FlowInputEventPin("account", "account.ready")
	if !found || again.Digest() != before {
		t.Fatal("recompilation changed admitted identity")
	}
}
