package cliapp

import (
	"strings"
)

func serveProjectContextEntryByName(entries []localContextEntry, contextName string) (localContextEntry, bool) {
	for _, entry := range entries {
		if entry.Descriptor.Name == contextName {
			return entry, true
		}
	}
	return localContextEntry{}, false
}

func serveProjectContextEntryReclaimable(entry localContextEntry) bool {
	if strings.TrimSpace(entry.Descriptor.Name) == "" {
		return false
	}
	switch entry.Status {
	case localContextStatusNoServer, localContextStatusStaleDescriptor, localContextStatusIdentityMismatch:
		return true
	default:
		return false
	}
}
