package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testtiming"
)

func TestFailureObservationRetainsNativeBoundaries(t *testing.T) {
	job := testtiming.ObservedFailure{RunID: 1, Attempt: 1, JobID: 2, HeadSHA: strings.Repeat("a", 40), Source: strings.Repeat("b", 40), Job: "Go proof owned"}
	var transcript bytes.Buffer
	write := func(action, name, output string) {
		line, err := json.Marshal(struct{ Action, Package, Test, Output string }{action, "proof", name, output})
		if err != nil {
			t.Fatal(err)
		}
		transcript.WriteString("2026-10-13T00:00:00Z ")
		transcript.Write(line)
		transcript.WriteByte('\n')
	}
	write("output", "TestPassing", `{"Action":"fail","Package":"child","Test":"TestExpected"}`)
	write("pass", "TestPassing", "")
	write("output", "TestReal/sqlite", "precise assertion")
	write("fail", "TestReal/sqlite", "")
	write("fail", "TestReal", "")
	write("fail", "", "")
	failures, err := nativeFailureEvents(transcript.Bytes(), job)
	if err != nil || len(failures) != 1 || failures[0].Test != "TestReal/sqlite" || failures[0].Detail != "precise assertion" || failures[0].Source != job.Source {
		t.Fatalf("lost actual leaf or adopted nested expected RED: %+v / %v", failures, err)
	}
	if !failureAggregateJob("Required test summary") || !failureAggregateJob("CI timing budget") || failureAggregateJob("Go proof owned") {
		t.Fatal("primary failure attribution changed")
	}
	if err := runFailures([]string{"-run", "1", "-last", "2"}, &bytes.Buffer{}); err == nil {
		t.Fatal("ambiguous observation command accepted")
	}
}
