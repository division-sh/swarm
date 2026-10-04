package toolgateway

import (
	"net"
	"testing"
)

func TestListenerEndpointsUsesBoundSocketAndPreservesExplicitInterface(t *testing.T) {
	for _, test := range []struct {
		ip, host, workspace string
	}{
		{"127.0.0.1", "http://127.0.0.1:12345", "http://host.docker.internal:12345"},
		{"0.0.0.0", "http://127.0.0.1:12345", "http://host.docker.internal:12345"},
		{"::1", "http://[::1]:12345", "http://host.docker.internal:12345"},
		{"::", "http://[::1]:12345", "http://host.docker.internal:12345"},
		{"172.18.0.1", "http://172.18.0.1:12345", "http://172.18.0.1:12345"},
		{"2001:db8::1", "http://[2001:db8::1]:12345", "http://[2001:db8::1]:12345"},
	} {
		t.Run(test.ip, func(t *testing.T) {
			host, workspace, err := ListenerEndpoints(&net.TCPAddr{IP: net.ParseIP(test.ip), Port: 12345})
			if err != nil || host != test.host || workspace != test.workspace {
				t.Fatalf("socket projection = %q %q err=%v", host, workspace, err)
			}
		})
	}
	if _, _, err := ListenerEndpoints(nil); err == nil {
		t.Fatal("absent listener accepted")
	}
}
