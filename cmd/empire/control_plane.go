package main

import (
	"strings"

	empireruntime "empireai/internal/runtime"
)

func defaultControlPlaneAgentID() string {
	ensureEmpireDefaults()
	return strings.TrimSpace(empireruntime.DefaultControlPlaneRecipient())
}

func withControlPlaneRecipient(recipients ...string) []string {
	out := make([]string, 0, len(recipients)+1)
	seen := map[string]struct{}{}
	appendOne := func(raw string) {
		id := strings.TrimSpace(raw)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, recipient := range recipients {
		appendOne(recipient)
	}
	appendOne(defaultControlPlaneAgentID())
	return out
}
