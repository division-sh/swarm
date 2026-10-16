package main

import (
	"fmt"
	"strings"
)

func family145MockMemoryConstructionProofError(function, before, after string) error {
	if function != "runMockAgentSupportedSurface" && function != "runStandingTelegramMemorySupportedSurface" {
		return fmt.Errorf("unreviewed pre-open constructor: %s", function)
	}
	for _, old := range []string{"runtimePG", "openStore", "prepareRestart", "store.NewPostgresStore(", "BootstrapPostgresRuntimeStore(", "DatabaseForTest(", "openSelectedPostgresOwner("} {
		if strings.Contains(after, old) {
			return fmt.Errorf("detached mock/memory construction survived: %s", old)
		}
	}
	normalized, err := family145ServedRestartConstructionInverse(before, after)
	if err != nil {
		return err
	}
	start := strings.Index(before, "\n\t\tvar runtimePG ")
	end := strings.Index(before, "\n\t\toldBuildStores := ")
	if start < 0 || end <= start {
		return fmt.Errorf("missing original detached adapter lifetime")
	}
	normalized = strings.Replace(normalized, "\n\t\toldBuildStores := ", before[start:end]+"\n\t\toldBuildStores := ", 1)
	normalized = strings.Replace(normalized, "\n\topts := cliapp.ServeOptions{", "\n\tvar prepareRestart func()\n\topts := cliapp.ServeOptions{", 1)
	marker := "\n\t\topts.ConfigPath = writeMockAgentRuntimeConfig(t, \"postgres\", \"\")"
	if function == "runStandingTelegramMemorySupportedSurface" {
		marker = "\n\t\topts.ConfigPath = writeStandingMockRuntimeConfig(t, \"postgres\", \"\")"
	}
	if strings.Count(normalized, marker) != 1 {
		return fmt.Errorf("missing original PostgreSQL configuration boundary")
	}
	normalized = strings.Replace(normalized, marker, "\n\t\tprepareRestart = openStore"+marker, 1)
	marker = "\n\tsecond := startOwnedMockLifecycleTestProcess("
	if function == "runStandingTelegramMemorySupportedSurface" {
		marker = "\n\tif !recoveryOnRestart {"
	}
	if strings.Count(normalized, marker) != 1 {
		return fmt.Errorf("missing real served restart boundary")
	}
	normalized = strings.Replace(normalized, marker, "\n\tif prepareRestart != nil {\n\t\tprepareRestart()\n\t}"+marker, 1)
	if normalized != before {
		return fmt.Errorf("mock/memory construction lost source/credentials/claims/recovery/identity/teardown or an assertion: %s", function)
	}
	return nil
}
