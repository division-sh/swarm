package main

const selectedConfigFile1801 = "internal/serveapp/startup_creation_handoff_test.go"
const selectedConfigFunction1801 = "TestComposedStartupCreationPublicationHandoffOnBothStores"

const priorConfigSetup1801 = `cfg, err := config.Load(writeServeRuntimeTestConfig(t))
				if err != nil {
					t.Fatal(err)
				}`

const selectedConfigSetup1801 = `loadedConfig, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{RepoRoot: repoRootForTest(), ExplicitPath: writeServeRuntimeTestConfig(t)})
				if err != nil {
					t.Fatal(err)
				}
				cfg := loadedConfig.Config`
