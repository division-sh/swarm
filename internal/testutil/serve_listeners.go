package testutil

// EphemeralServeListenerConfig keeps verification and process fixtures on the
// same OS-allocated loopback listener policy. Explicit binding tests omit it.
func EphemeralServeListenerConfig() string {
	return "serve:\n  api_listen_addr: '127.0.0.1:0'\n  mcp_listen_addr: '127.0.0.1:0'\n"
}
