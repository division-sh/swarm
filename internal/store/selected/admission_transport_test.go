package selected

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestAdmissionInspectionBoundsSilentPostgresHandshake(t *testing.T) {
	for _, interrupt := range []bool{false, true} {
		t.Run(strconv.FormatBool(interrupt), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			accepted := make(chan struct{})
			closed := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					closed <- err
					return
				}
				defer conn.Close()
				close(accepted)
				var data [4096]byte
				for {
					if _, err := conn.Read(data[:]); err != nil {
						closed <- nil
						return
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				inspection, err := OpenAdmissionInspection(ctx, AuthorityRequest{
					Selection:   storebackend.Selection{Backend: storebackend.BackendPostgres},
					PostgresDSN: "host=127.0.0.1 port=" + strconv.Itoa(listener.Addr().(*net.TCPAddr).Port) + " dbname=probe user=probe sslmode=disable",
				})
				if inspection != nil {
					err = errors.Join(err, inspection.Close(), errors.New("silent peer admitted a selected store"))
				}
				result <- err
			}()
			select {
			case <-accepted:
			case <-time.After(time.Second):
				t.Fatal("selected inspection did not attempt the native handshake")
			}
			if interrupt {
				cancel()
			}
			select {
			case err := <-result:
				want := context.DeadlineExceeded
				if interrupt {
					want = context.Canceled
				}
				if !errors.Is(err, want) {
					t.Fatalf("selected transport lost caller outcome: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("selected store opening escaped its actual I/O deadline")
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("inspection returned without closing its native transport")
			}
		})
	}
}
