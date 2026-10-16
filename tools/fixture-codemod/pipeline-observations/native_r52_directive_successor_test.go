package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type r52DirectiveSuccessorPin struct {
	family, function, history, prior, successor string
}

var r52DirectiveSuccessorPins = []r52DirectiveSuccessorPin{
	{"native-execution-construction-tail", "TestDestructiveResetFailsClosedWhileDirectiveBoardStepIsRunning", "04d19d5ff20ef8cf41e53bc1fe05448c3bb3bf4e7a78744ee39620d0ae8f822e", "8eb848f0e723a1237c047a1d28dc40491ee2a724eb152574660c1947a3d536cf", "33b3728c4c020ce9c5ea9a247c3538ad1aee6eed5b9ecf7b1e4f2d5a3e94d798"},
	{"native-api-adjacent-physical-counter", "TestOperatorAgentSendDirectivePersistsDirectiveEventOnceOnReplay", "e8d989054244da2f565a981f2216df53f0d452b4711bc57cacfa0e0487141185", "066cc1e33cb20476729a7747bbb55eebdcb038557c26249945eb935362a5bffe", "95215bdf6e95a85244925a8e15df23dc226586cde25f329fec899ce87f71af1f"},
	{"native-run-setup-family144", "TestOperatorAgentSendDirectiveUsesCanonicalRuntimeSourceArtifact", "c0eaf89bc345b262f37e11cc05167e28265aca31d9b410fdcee8b38bdd7b9062", "26ad327def48912c3491bc904123ec5b006057f6e3b3f06df01cc0463ff8ea96", "8ebb5ec37d9696aed6360fc029546ab4f8a234241282004856659569e4951f94"},
	{"native-shared-coordinator-authority", "TestTemplateInstanceAutoEmitDispatchesLocalHandlerAndEmpireStyleSideEffect", "7347cf0251892924de72a0c6b4690a62d0c905e5dded04032e46a4afd46b86ae", "cc1b95c98cc97dd7328a2a19969b10860af079adb54a7a1432e245fb23747009", "80fb0aecd3a5382d16e40f8256e43bfcef9af610a9da881eef83132d9777035b"},
	{"native-shared-coordinator-authority", "TestTemplateInstanceActivationConfigSubscriberPersistsRenderedRouteAndDeliveryRows", "965d5e1bbdf33ca4b352b28ddba3d1802689bc0f6a2a9c09d4c90259489827ad", "bd73bdb4b74a58332bad5b7b710dfd38e36830acb055e2100cf82060442bbe78", "88fcee7dc761248ec3b3eb95a6183692724cb2f6b0b8ff6ef5a1d154d61e22ae"},
	{"native-shared-coordinator-authority", "TestTemplateInstanceConnectLifecyclePublishRollbackDoesNotLeakInstanceOrRoute", "ce067ed1f6dc41eaaf9a816097443abcf8262ed8ecf18f42eea8c22243becbbd", "8a64abfc56dda32c5468cfc5d02992ce8b324905948a815fa8c7691d1d8cefc0", "66132e2a5a7e9084c31ae79d98f9463de8b8e7dae13f0f4439a21c5769355077"},
	{"native-shared-coordinator-authority", "TestTemplateInstanceAcknowledgedPublishDispatchesRoutedSystemNodeWithoutInternalCarrierAndEmpireStyleSideEffect", "6058228764f30bcca2ad50e0c5d98ef6f5f064216f7baedf04edfb1e95810462", "e367e1c62e38d10214ecb1f8340543bb00479219c4f8661b6f82e052e89d4bf5", "197c62cf40ee26c8b06a36733f440803672bc6d54c7e8a75b90e8ff203a225a1"},
	{"native-shared-coordinator-authority", "TestTemplateInstanceRootOutboxEventDispatchesRoutedSystemNodeAndEmpireStyleSideEffect", "a207e449344318c6595e45192410bcb5772f8e475c0003311f18e01353fe82fd", "99025c4eeadc090a6c162b2ac0ac1c7cb249c59c5225993ca46a07dab7aa7687", "db0d0d87613c3518a43ca82c37c0c0704e22726ed7d01df69b6fa547656c064d"},
	{"native-run-setup-family144", "TestRuntimeShutdownWithOptions_PropagatesConfiguredGraceToManagerDrain", "b7f8482e0e4859be436d92868a54508a0d98a0b935b97d5690d83daf0fa3f962", "46d2f1303db218bb9b8b1fde3aff6fe64bda642ab6c35cf82714cebb1c760337", "97e8e4fc60d7166b1a55137c401f728aa70f6f0b35096ce7b3a6b8f09cd06a3c"},
	{"native-run-setup-family144", "TestRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngress", "9d38fa158ef1f8a90d96635bae68b2bed0f74fd2970d06bf9ba3f8d5b34688d8", "19643b91c17b4d38799b05bbdad02cdbd6e2480e4e334438a56389d69e90a86d", "4bac50377a6731e7c2e3b1647c3c43bbdf7a9bf35642a10a6ab054f2432023fb"},
	{"native-run-setup-family144", "TestRuntimeShutdownFanOutCommittedTurnKeepsDependenciesLive", "b55ec843499608332eee94465a98fd277ba83910d51b4b9b89c25a86cb614444", "0463a4018e692254aea945f97420d5d8930df6c821e251690f7e24d14d42a4f7", "01589fdfd342ba7d6f303b8c2b515d73c2c3b5b48b7d322085765faf561bd4f7"},
}

// Finite endpoints authorized by reviewer-b6145065880/6145833450/6146134392/6150892514.
func reverseR52DirectiveSuccessor(row recipe, actual string, pin r52DirectiveSuccessorPin) error {
	row, actual, err := catchUp2542PriorMasterPair(row, actual)
	if err != nil {
		return err
	}
	composition, found, err := sharedCoordinatorComposition2617(row)
	if err != nil {
		return err
	}
	if found {
		return validateSharedCoordinatorComposition2617(row, actual, pin, composition)
	}
	return reverseR52OriginalDirectiveSuccessor(row, actual, pin)
}

func reverseR52OriginalDirectiveSuccessor(row recipe, actual string, pin r52DirectiveSuccessorPin) error {
	wantFile := "internal/apiv1/operator_agent_control_test.go"
	if pin.family == "native-shared-coordinator-authority" {
		wantFile = "internal/runtime/template_instance_delivery_test.go"
	}
	if pin.family == "native-execution-construction-tail" {
		wantFile = "internal/store/internal/runtimepersistence/destructive_reset_directive_integration_test.go"
	}
	shutdown := strings.HasPrefix(pin.function, "TestRuntimeShutdown")
	if shutdown {
		wantFile = "internal/runtime/runtime_shutdown_admission_test.go"
		if pin.function == "TestRuntimeShutdownFanOutCommittedTurnKeepsDependenciesLive" {
			wantFile = "internal/runtime/runtime_shutdown_fan_out_test.go"
		}
	}
	if row.Family != pin.family || row.Function != pin.function || row.File != wantFile {
		return fmt.Errorf("foreign directive recipe")
	}
	history := row
	history.Successor = ""
	raw, err := json.Marshal(history)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != pin.history {
		return fmt.Errorf("directive proof history changed")
	}
	current, err := canonicalFunction(actual)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(current))) != pin.successor {
		return fmt.Errorf("unreviewed directive endpoint")
	}
	reviewed, err := canonicalFunction(row.Successor)
	if err != nil || current != reviewed {
		return fmt.Errorf("directive source differs from its successor")
	}
	var reversed string
	if pin.family == "native-execution-construction-tail" {
		reversed, err = reverseR52NativeDirectiveAdoption(current)
		if err != nil {
			return err
		}
	} else if shutdown {
		block := "lifecycle, roles := runtimeTestEphemeralManagerLifecycle(t)\n\tbusRoles := runtimeTestManagerBusRoles(bus)\n\tbusRoles.LifecycleState, busRoles.LifecycleOutcome = roles.LifecycleState, roles.LifecycleOutcome\n\t"
		if strings.Count(current, block) != 1 || strings.Count(current, "LifecycleStore: lifecycle, ") != 1 || strings.Count(current, "PersistenceRoles: busRoles") != 1 {
			return fmt.Errorf("shutdown lifecycle wiring cardinality changed")
		}
		reversed = strings.Replace(current, block, "", 1)
		reversed = strings.Replace(reversed, "LifecycleStore: lifecycle, ", "", 1)
		reversed = strings.Replace(reversed, "PersistenceRoles: busRoles", "PersistenceRoles: runtimeTestManagerBusRoles(bus)", 1)
	} else if pin.family == "native-shared-coordinator-authority" {
		call := "externalRuntimeTestManagerLifecycleRoles(bus, pg)"
		if strings.Count(current, call) != 1 {
			return fmt.Errorf("template role cardinality changed")
		}
		reversed = strings.Replace(current, call, "externalRuntimeTestManagerBusRoles(bus)", 1)
	} else {
		roles := "runtimemanager.PersistenceRoles{LifecycleState: pg, LifecycleOutcome: pg, DirectiveOperations: pg, DirectiveTargets: pg}"
		config := ", Config: json.RawMessage(`{}`)"
		if strings.Count(current, roles) != 1 || strings.Count(current, config) != 1 {
			return fmt.Errorf("directive role/config cardinality changed")
		}
		reversed = strings.Replace(current, roles, "runtimemanager.PersistenceRoles{DirectiveOperations: pg, DirectiveTargets: pg}", 1)
		reversed = strings.Replace(reversed, config, "", 1)
	}
	prior, err := canonicalFunction(reversed)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(prior))) != pin.prior {
		return fmt.Errorf("directive reversal changed other setup or assertions")
	}
	return nil
}

func reverseR52NativeDirectiveAdoption(current string) (string, error) {
	hydration := "state, found, err := pg.LoadAgentLifecycleState(ctx, rec.Config.Identity)\n\tif err != nil || !found {\n\t\tt.Fatalf(\"hydrate exact directive fixture lifecycle: found=%t err=%v\", found, err)\n\t}\n\texecutionRec.LifecycleEpoch, executionRec.LifecycleGeneration, executionRec.LifecyclePhase = state.RuntimeEpoch, state.Generation, state.Phase\n\texecutionRec.LifecycleRunMode, executionRec.ProcessBinding, executionRec.Topology = state.RunMode, state.ProcessBinding, state.Topology"
	prior := "executionRec.Topology, err = runtimeagenttopology.NewEphemeralAdmission(uuid.NewString(), \"runtime_shard\")\n\tif err != nil {\n\t\tt.Fatalf(\"build ephemeral topology admission: %v\", err)\n\t}"
	for _, splice := range [][2]string{
		{"LifecycleStore: agentfixture.Lifecycle(t, pg), ", ""},
		{"LifecycleState: pg, LifecycleOutcome: pg, ", ""},
		{hydration, prior},
	} {
		if strings.Count(current, splice[0]) != 1 {
			return "", fmt.Errorf("native directive wiring cardinality changed")
		}
		current = strings.Replace(current, splice[0], splice[1], 1)
	}
	return current, nil
}

func r52NativeExecutionCurrentBody(row recipe, actual string) (string, error) {
	if row.Successor == "" {
		return row.After, nil
	}
	for _, pin := range r52DirectiveSuccessorPins {
		if pin.family == "native-execution-construction-tail" && row.Family == pin.family && row.Function == pin.function {
			if err := reverseR52DirectiveSuccessor(row, actual, pin); err != nil {
				return "", err
			}
			return row.Successor, nil
		}
	}
	return "", fmt.Errorf("unlisted native-execution successor")
}

func r52SharedCoordinatorCurrentBody(row recipe, actual string) (string, error) {
	if row.Successor == "" {
		return row.After, nil
	}
	for _, pin := range r52DirectiveSuccessorPins {
		if pin.family == "native-shared-coordinator-authority" && row.Family == pin.family && row.Function == pin.function {
			if err := reverseR52DirectiveSuccessor(row, actual, pin); err != nil {
				return "", err
			}
			return row.Successor, nil
		}
	}
	return "", fmt.Errorf("unlisted shared-coordinator successor")
}

func TestR52DirectiveFixtureSuccessorsPreserveNativeSetupAndHostility(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	t.Run("same_selected_role_composer", func(t *testing.T) {
		name := "externalRuntimeTestManagerLifecycleRoles"
		path := "internal/runtime/author_activity_external_test_context_test.go"
		source, err := os.ReadFile(filepath.Join("../../..", path))
		if err != nil {
			t.Fatal(err)
		}
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		function, err := uniqueFunction(file, name)
		if err != nil {
			t.Fatal(err)
		}
		actual := string(source[set.Position(function.Pos()).Offset:set.Position(function.End()).Offset])
		valid := func(body string) bool {
			canonical, err := canonicalFunction(body)
			return err == nil && fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))) == "c25fdca7fa33457442b94812bcf49d47c0cdd51fb9a5ce1b32c30df35139a914"
		}
		if !valid(actual) {
			t.Fatal("role composer changed bus roles or same-selected lifecycle roles")
		}
		for _, mutant := range []string{
			strings.Replace(actual, "selected, selected", "selected, foreign", 1),
			strings.Replace(actual, "roles.LifecycleState, roles.LifecycleOutcome = selected, selected", "roles.LifecycleState = selected", 1),
			strings.Replace(actual, "externalRuntimeTestManagerBusRoles(bus)", "runtimemanager.PersistenceRoles{}", 1),
		} {
			if mutant == actual || valid(mutant) {
				t.Fatal("hostile role composer accepted")
			}
		}
	})
	t.Run("same_session_lifecycle_composer", func(t *testing.T) {
		actual := selectedCausalObservationBody(t, "internal/runtime/author_activity_test_context_test.go", "runtimeTestEphemeralManagerLifecycle")
		valid := func(body string) bool {
			canonical, err := canonicalFunction(body)
			return err == nil && fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))) == "7c0cf92f29945884fb8226b5f0905926dea2be1eae8a73a6d165dee494162eb4"
		}
		if !valid(actual) {
			t.Fatal("unit lifecycle composer changed exact session/grant ownership")
		}
		for _, mutant := range []string{
			strings.Replace(actual, "LifecycleOutcome: session", "LifecycleOutcome: foreign", 1),
			strings.Replace(actual, ", LifecycleOutcome: session", "", 1),
			strings.Replace(actual, "authorActivityTestRuntimeInstanceID, session)", "authorActivityTestRuntimeInstanceID, foreign)", 1),
		} {
			if mutant == actual || valid(mutant) {
				t.Fatal("hostile unit lifecycle composer accepted")
			}
		}
	})
	for _, pin := range r52DirectiveSuccessorPins {
		t.Run(pin.function, func(t *testing.T) {
			var row *recipe
			for i := range rows {
				if rows[i].Family == pin.family && rows[i].Function == pin.function {
					if row != nil {
						t.Fatal("duplicate keyed directive recipe")
					}
					row = &rows[i]
				}
			}
			if row == nil {
				t.Fatal("authorized directive recipe missing")
			}
			source, err := os.ReadFile(filepath.Join("../../..", row.File))
			if err != nil {
				t.Fatal(err)
			}
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, row.File, source, parser.AllErrors)
			if err != nil {
				t.Fatal(err)
			}
			function, err := uniqueRecipeFunction(file, *row)
			if err != nil {
				t.Fatal(err)
			}
			actual := repair2698HistoricalBody(t, row.File, function.Name.Name)
			actual, err = canonicalFunction(actual)
			if err != nil {
				t.Fatal(err)
			}
			if err := reverseR52DirectiveSuccessor(*row, actual, pin); err != nil {
				t.Fatal(err)
			}
			mutants := map[string]string{
				"foreign_store":   strings.Replace(actual, "LifecycleState: pg", "LifecycleState: foreign", 1),
				"missing_outcome": strings.Replace(actual, "LifecycleOutcome: pg,", "", 1),
				"late_config":     strings.Replace(actual, ", Config: json.RawMessage(`{}`)", "", 1),
				"assertion":       strings.Replace(actual, "t.Fatalf", "t.Logf", 1),
				"unrelated_body":  strings.Replace(actual, "{", "{ unreviewedSetup();", 1),
			}
			if pin.family == "native-shared-coordinator-authority" {
				mutants = map[string]string{
					"foreign_store":  strings.Replace(actual, "externalRuntimeTestManagerLifecycleRoles(bus, pg)", "externalRuntimeTestManagerLifecycleRoles(bus, foreign)", 1),
					"missing_roles":  strings.Replace(actual, "externalRuntimeTestManagerLifecycleRoles(bus, pg)", "externalRuntimeTestManagerBusRoles(bus)", 1),
					"assertion":      strings.Replace(actual, "t.Fatalf", "t.Logf", 1),
					"unrelated_body": strings.Replace(actual, "{", "{ unreviewedSetup();", 1),
				}
			}
			if pin.family == "native-execution-construction-tail" {
				mutants = map[string]string{
					"foreign_store":   strings.Replace(actual, "LifecycleState: pg", "LifecycleState: foreign", 1),
					"foreign_writer":  strings.Replace(actual, "agentfixture.Lifecycle(t, pg)", "agentfixture.Lifecycle(t, foreign)", 1),
					"missing_writer":  strings.Replace(actual, "agentfixture.Lifecycle(t, pg)", "nil", 1),
					"missing_state":   strings.NewReplacer("LifecycleState: pg, ", "", ", LifecycleState: pg", "", "LifecycleState: pg", "").Replace(actual),
					"missing_outcome": strings.NewReplacer("LifecycleOutcome: pg, ", "", ", LifecycleOutcome: pg", "", "LifecycleOutcome: pg", "").Replace(actual),
					"native_identity": strings.Replace(actual, "pg.LoadAgentLifecycleState(ctx, rec.Config.Identity)", "pg.LoadAgentLifecycleState(ctx, foreignIdentity)", 1),
					"missing_row":     strings.Replace(actual, "err != nil || !found", "err != nil", 1),
					"native_topology": strings.Replace(actual, "state.ProcessBinding, state.Topology", "state.ProcessBinding, foreignTopology", 1),
					"assertion":       strings.Replace(actual, "state=executing", "state=idle", 1),
					"unrelated_body":  strings.Replace(actual, "{", "{ unreviewedSetup();", 1),
				}
			}
			if strings.HasPrefix(pin.function, "TestRuntimeShutdown") {
				mutants = map[string]string{
					"foreign_store":  strings.Replace(actual, "LifecycleStore:", "ForeignStore:", 1),
					"missing_roles":  strings.Replace(actual, "busRoles.LifecycleState, busRoles.LifecycleOutcome = roles.LifecycleState, roles.LifecycleOutcome", "busRoles.LifecycleState = roles.LifecycleState", 1),
					"delivery_owner": strings.Replace(actual, "DeliveryStore:", "ForeignDeliveryStore:", 1),
					"assertion":      strings.Replace(actual, "t.Fatalf", "t.Logf", 1),
					"unrelated_body": strings.Replace(actual, "{", "{ unreviewedSetup();", 1),
				}
			}
			for name, mutant := range mutants {
				if mutant == actual || reverseR52DirectiveSuccessor(*row, mutant, pin) == nil {
					t.Fatalf("hostile directive delta accepted: %s", name)
				}
			}
			unlisted := *row
			unlisted.Function += "Unlisted"
			if reverseR52DirectiveSuccessor(unlisted, actual, pin) == nil {
				t.Fatal("unlisted recipe accepted")
			}
			for _, field := range []string{"before", "after", "mechanical"} {
				mutant := *row
				switch field {
				case "before":
					mutant.Before += " changed"
				case "after":
					mutant.After += " changed"
				case "mechanical":
					mutant.Mechanical = &mechanicalSnapshot{SourceCommit: "foreign"}
				}
				if reverseR52DirectiveSuccessor(mutant, actual, pin) == nil {
					t.Fatalf("historical directive metadata changed: %s", field)
				}
			}
		})
	}
}
