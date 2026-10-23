package main

import (
	"crypto/sha256"
	_ "embed"

	"fmt"

	"strings"
)

var retainedJoinSuccessorEdits = []struct{ before, block string }{
	{"\t\t\tif len(joins) != 1 || first.Attempt != 1 {\n\t\t\t\tt.Fatalf(\"first ordinary join: loop=%+v joins=%+v\", first, joins)\n\t\t\t}\n", "\t\t\tfirstRef := joins[0].JoinRef()\n\t\t\tnativeEvents, hasNativeEvents := rt.ReceiverStateReader.(runtimebus.EventStore)\n\t\t\tif !hasNativeEvents || nativeEvents == nil {\n\t\t\t\tt.Fatal(\"retained join outcome requires its original selected event reader\")\n\t\t\t}\n\t\t\tnativeHandoff := servedWorkspaceProofRuntime{Events: nativeEvents}\n\t\t\tfirstCtx, cancelFirst := context.WithTimeout(t.Context(), servedProofPollDeadline)\n\t\t\tfirstWaitErr := waitServedRetainedJoinOutcome(firstCtx, started.RunID, firstRef, func() (loopruntime.Activation, []joinruntime.Activation) {\n\t\t\t\treturn readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n\t\t\t})\n\t\t\tcancelFirst()\n\t\t\tif firstWaitErr != nil {\n\t\t\t\tt.Fatalf(\"first exact retained join outcome: %v\", firstWaitErr)\n\t\t\t}\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)\n\t\t\twaitWorkspaceProofPipelineHandoff(t, nativeHandoff, started.RunID)\n\t\t\tfirst, joins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n"},
	{"\t\t\tif current.Attempt != 2 || current.ActivationID != first.ActivationID || len(sourceJoins) != 2 {\n\t\t\t\tt.Fatalf(\"ordinary repeat lost retained joins: loop=%+v joins=%+v\", current, sourceJoins)\n\t\t\t}\n", "\t\t\tvar currentRef timeridentity.JoinRef\n\t\t\tcurrentMatches := 0\n\t\t\tfor _, join := range sourceJoins {\n\t\t\t\tif join.Generation() == current.Generation() {\n\t\t\t\t\tcurrentRef = join.JoinRef()\n\t\t\t\t\tcurrentMatches++\n\t\t\t\t}\n\t\t\t}\n\t\t\tif currentMatches != 1 {\n\t\t\t\tt.Fatalf(\"current retained join reference is not unique: %d\", currentMatches)\n\t\t\t}\n\t\t\tcurrentCtx, cancelCurrent := context.WithTimeout(t.Context(), servedProofPollDeadline)\n\t\t\tcurrentWaitErr := waitServedRetainedJoinOutcome(currentCtx, started.RunID, currentRef, func() (loopruntime.Activation, []joinruntime.Activation) {\n\t\t\t\treturn readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n\t\t\t})\n\t\t\tcancelCurrent()\n\t\t\tif currentWaitErr != nil {\n\t\t\t\tt.Fatalf(\"current exact retained join outcome: %v\", currentWaitErr)\n\t\t\t}\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)\n\t\t\twaitWorkspaceProofPipelineHandoff(t, nativeHandoff, started.RunID)\n\t\t\tcurrent, sourceJoins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n"},
}

//go:embed testdata/retained_join_r7_reviewed.go.txt
var retainedJoinR7Reviewed string

func validateRetainedJoinSuccessor(row recipe, candidate string) error {
	predecessor := strings.TrimSuffix(retainedJoinR7Reviewed, "\n")
	if fmt.Sprintf("%x", sha256.Sum256([]byte(predecessor))) != "9fe6d8918d3fc2716ec8db18d7415b6445afa8da23014a4b20965f6a85155867" {
		return fmt.Errorf("reviewed R7 predecessor changed")
	}
	expected, restored := predecessor, candidate
	const superseded = "\n\t\t\twaitForServeStory(t, func() bool {\n\t\t\t\tcurrent, sourceJoins = readRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n\t\t\t\treturn len(sourceJoins) == 2 && sourceJoins[0].OutcomeFired && sourceJoins[1].OutcomeFired\n\t\t\t})\n\t\t\twaitRetainedJoinPipelineHandoff(t, rt.ReceiverStateReader, started.RunID)\n"
	if strings.Count(expected, superseded) != 1 {
		return fmt.Errorf("reviewed R7 wait changed")
	}
	expected = strings.Replace(expected, superseded, "", 1)
	for _, edit := range retainedJoinSuccessorEdits {
		if strings.Count(expected, edit.before) != 1 || strings.Count(candidate, edit.before+edit.block) != 1 {
			return fmt.Errorf("exact join checkpoint/wait block missing, duplicated or moved")
		}
		expected = strings.Replace(expected, edit.before, edit.before+edit.block, 1)
		restored = strings.Replace(restored, edit.before+edit.block, edit.before, 1)
	}
	actual, actualErr := canonicalFunction(candidate)
	want, wantErr := canonicalFunction(expected)
	checkpoint := "\t\t\tif current.Attempt != 2 || current.ActivationID != first.ActivationID || len(sourceJoins) != 2 {"
	restored = strings.Replace(restored, checkpoint, superseded+checkpoint, 1)
	original, originalErr := canonicalFunction(predecessor)
	reversed, reversedErr := canonicalFunction(restored)
	if actualErr != nil || wantErr != nil || originalErr != nil || reversedErr != nil || actual != want || reversed != original {
		return fmt.Errorf("join successor changed work beyond exact outcome composition")
	}
	return nil
}

func q10RetainedJoinCurrentSource(row recipe) (string, error) {
	if row.File != "internal/serveapp/fork_retained_join_test.go" || row.Function != "testServedJoinWriterCompletedHistoryRefusal" {
		return "", fmt.Errorf("unreviewed retained-join current endpoint")
	}
	if err := validateRetainedJoinSuccessor(row, row.Successor); err != nil {
		return "", err
	}
	current := row.Successor
	for _, edit := range []publicationSuccessorEdit{
		{"\t\t\tfirstRef := joins[0].JoinRef()\n", `			firstRef := joins[0].JoinRef()
			if joins[0].Generation() != first.Generation() {
				t.Fatal("first join borrowed another loop generation")
			}
`},
		{"\t\t\tfirst, joins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n", `			first, joins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			if complete, err := retainedJoinOutcomesComplete(joins, []timeridentity.JoinRef{firstRef}); err != nil || !complete {
				t.Fatalf("first retained join lost its exact completed outcome: complete=%t err=%v", complete, err)
			}
`},
		{"\t\t\tcurrentCtx, cancelCurrent := context.WithTimeout(t.Context(), servedProofPollDeadline)\n", `			if !currentRef.Valid() || !currentRef.Declaration().Equal(firstRef.Declaration()) {
				t.Fatal("repeat lost the declared join in its exact loop generation")
			}
			currentCtx, cancelCurrent := context.WithTimeout(t.Context(), servedProofPollDeadline)
`},
		{"\t\t\tcurrent, sourceJoins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n", `			current, sourceJoins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			if complete, err := retainedJoinOutcomesComplete(sourceJoins, []timeridentity.JoinRef{firstRef, currentRef}); err != nil || !complete {
				t.Fatalf("retained joins lost their exact completed outcomes: complete=%t err=%v", complete, err)
			}
`},
	} {
		if strings.Count(current, edit.before) != 1 {
			return "", fmt.Errorf("missing exact retained-join current binding %q", edit.before)
		}
		current = strings.Replace(current, edit.before, edit.after, 1)
	}
	return current, nil
}
