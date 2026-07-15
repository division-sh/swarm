package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestCLIArgCountPromotedCommandsUseSharedDiagnostic(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "completion missing shell", args: []string{"completion"}, want: "'swarm completion' requires <bash|zsh|fish|powershell>."},
		{name: "secrets set missing key", args: []string{"secrets", "set"}, want: "'swarm secrets set' requires <key>."},
		{name: "secrets rm missing key", args: []string{"secrets", "rm"}, want: "'swarm secrets rm' requires <key>."},
		{name: "connections connect missing key", args: []string{"connections", "connect"}, want: "'swarm connections connect' requires <key>."},
		{name: "connections callback missing key", args: []string{"connections", "callback"}, want: "'swarm connections callback' requires <key>."},
		{name: "connections status extra key", args: []string{"connections", "status", "a", "b"}, want: "'swarm connections status' accepts at most one argument ([key]); got 2: \"a\" \"b\"."},
		{name: "connections disconnect missing key", args: []string{"connections", "disconnect"}, want: "'swarm connections disconnect' requires <key>."},
		{name: "run fork missing source", args: []string{"run", "fork"}, want: "'swarm run fork' requires <source-run-id>."},
		{name: "run status extra", args: []string{"run", "status", "run-1", "extra"}, want: "'swarm run status' accepts at most one argument ([run-id]); got 2: \"run-1\" \"extra\"."},
		{name: "run trace extra", args: []string{"run", "trace", "run-1", "extra"}, want: "'swarm run trace' accepts at most one argument ([run-id]); got 2: \"run-1\" \"extra\"."},
		{name: "control pause missing target", args: []string{"control", "pause"}, want: "'swarm control pause' requires <run-id>."},
		{name: "control continue extra target", args: []string{"control", "continue", "run-1", "run-2"}, want: "'swarm control continue' accepts at most one argument ([<run-id>]); got 2: \"run-1\" \"run-2\"."},
		{name: "control stop missing target", args: []string{"control", "stop"}, want: "'swarm control stop' requires <run-id>."},
		{name: "agent deliveries missing id", args: []string{"agent", "deliveries"}, want: "'swarm agent deliveries' requires <agent-id>."},
		{name: "agent diagnose missing id", args: []string{"agent", "diagnose"}, want: "'swarm agent diagnose' requires <agent-id>."},
		{name: "agent view missing id", args: []string{"agent", "view"}, want: "'swarm agent view' requires <agent-id>."},
		{name: "agent restart missing id", args: []string{"agent", "restart"}, want: "'swarm agent restart' requires <agent-id>."},
		{name: "agent replay missing id", args: []string{"agent", "replay", "--event-id", "event-1"}, want: "'swarm agent replay' requires <agent-id>."},
		{name: "agent replay-backlog missing id", args: []string{"agent", "replay-backlog"}, want: "'swarm agent replay-backlog' requires <agent-id>."},
		{name: "agent directive missing message", args: []string{"agent", "directive", "agent-1"}, want: "'swarm agent directive' requires <message> (got <agent-id>)."},
		{name: "event view missing id", args: []string{"event", "view"}, want: "'swarm event view' requires <event-id>."},
		{name: "event replay missing id", args: []string{"event", "replay"}, want: "'swarm event replay' requires <event-id>."},
		{name: "event publish missing name", args: []string{"event", "publish", "--payload-json", "{}"}, want: "'swarm event publish' requires <event-name>."},
		{name: "conversation view missing session", args: []string{"conversation", "view"}, want: "'swarm conversation view' requires <session-id>."},
		{name: "conversation turn missing id", args: []string{"conversation", "turn", "sess-1"}, want: "'swarm conversation turn' requires <turn-id-or-prefix> (got <session-id>)."},
		{name: "entity view missing id", args: []string{"entity", "view"}, want: "'swarm entity view' requires <entity-id>."},
		{name: "mailbox view missing id", args: []string{"mailbox", "view"}, want: "'swarm mailbox view' requires <mailbox-id>."},
		{name: "mailbox defer missing id", args: []string{"mailbox", "defer"}, want: "'swarm mailbox defer' requires <card-id>."},
		{name: "bundle show missing hash", args: []string{"bundle", "show"}, want: "'swarm bundle show' requires <bundle-hash>."},
		{name: "bundle agents missing hash", args: []string{"bundle", "agents"}, want: "'swarm bundle agents' requires <bundle-hash>."},
		{name: "bundle delete missing hash", args: []string{"bundle", "delete"}, want: "'swarm bundle delete' requires <bundle-hash>."},
		{name: "bundle register missing envelope", args: []string{"bundle", "register"}, want: "'swarm bundle register' requires <registration-envelope-yaml>."},
		{name: "bundle register contracts extra envelope", args: []string{"bundle", "register", "envelope.yaml", "--contracts", "contracts"}, want: "'swarm bundle register' accepts no positional arguments; got 1: \"envelope.yaml\"."},
		{name: "forkchat new missing source", args: []string{"forkchat", "new"}, want: "'swarm forkchat new' requires <source-session-id>."},
		{name: "forkchat resume missing fork", args: []string{"forkchat", "resume"}, want: "'swarm forkchat resume' requires <fork-id>."},
		{name: "forkchat view missing fork", args: []string{"forkchat", "view"}, want: "'swarm forkchat view' requires <fork-id>."},
		{name: "forkchat delete missing fork", args: []string{"forkchat", "delete"}, want: "'swarm forkchat delete' requires <fork-id>."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommand(context.Background(), t.TempDir(), tc.args, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			got := stderr.String()
			if !strings.Contains(got, "ERROR: "+tc.want) {
				t.Fatalf("stderr = %q, want problem substring %q", got, "ERROR: "+tc.want)
			}
			if !strings.Contains(got, "\nUsage: swarm ") {
				t.Fatalf("stderr = %q, want one-line Usage", got)
			}
			rawOne := "accepts 1 " + "arg(s)"
			rawMaxOne := "accepts at most 1 " + "arg(s)"
			if strings.Contains(got, rawOne) || strings.Contains(got, rawMaxOne) {
				t.Fatalf("stderr contains stale raw Cobra arg-count text: %q", got)
			}
		})
	}
}
