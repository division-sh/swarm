package store_test

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store"
)

func TestStoreFacadeDoesNotExposeHeartbeatProofFaults(t *testing.T) {
	for _, facade := range []reflect.Type{
		reflect.TypeFor[*store.PostgresStore](),
		reflect.TypeFor[*store.SQLiteRuntimeStore](),
	} {
		for _, method := range []string{
			"ClaimManagerHeartbeatProof", "RenewManagerHeartbeatProof",
			"ClaimManagerHeartbeatProofForTest", "RenewManagerHeartbeatProofForTest",
		} {
			if _, exposed := facade.MethodByName(method); exposed {
				t.Errorf("%s exposes the private fixed-duration heartbeat fault %s", facade, method)
			}
		}
	}
}
