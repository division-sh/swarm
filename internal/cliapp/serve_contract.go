package cliapp

import "github.com/division-sh/swarm/internal/platform"

const (
	defaultPlatformSpecPath = platform.DefaultPlatformSpecPath
	defaultAPIListenAddr    = "127.0.0.1:8081"
	defaultMCPListenAddr    = "127.0.0.1:8082"
	RuntimeStoreBackendHelp = "Runtime store backend: sqlite (local/dev default) or postgres (explicit opt-in production/external backend)"
)
