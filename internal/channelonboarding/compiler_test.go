package channelonboarding

import (
	"strings"
	"testing"
)

func TestChannelActivationPublicationModesHaveDistinctIdentity(t *testing.T) {
	executable, err := NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	declared, err := NewDeclaredOnlyChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	if executable.Generation().Equal(declared.Generation()) {
		t.Fatal("executable and declared-only publications share one generation")
	}
	if !executable.Executable() || declared.Executable() {
		t.Fatalf("publication modes executable=%s declared=%s", executable.Mode(), declared.Mode())
	}
}

func TestMergeCompiledActivationsRejectsInvalidPlanBeforeUnion(t *testing.T) {
	coordinate := testCoordinate()
	left := CompiledActivation{Source: ActivationSourceDeclared, Coordinate: coordinate}
	right := CompiledActivation{Source: ActivationSourceLearned, Coordinate: coordinate, ActivationRevision: 1}
	// Invalid plans fail before a producer can use source precedence as a
	// fallback. Integration coverage supplies executable plans.
	if _, err := MergeCompiledActivations([]CompiledActivation{left}, []CompiledActivation{right}); err == nil || !strings.Contains(err.Error(), "generation") {
		t.Fatalf("invalid activation union error = %v", err)
	}
}

func TestLearnedBindingIDIsStableAndSecretFree(t *testing.T) {
	left := LearnedBindingID("slot-a")
	if left != LearnedBindingID("slot-a") || left == LearnedBindingID("slot-b") || strings.Contains(left, "slot-a") {
		t.Fatalf("learned binding identity is not a stable opaque slot projection: %q", left)
	}
}
