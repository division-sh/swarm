package publicingress

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExposureStopJoinsGenerationWriterBeforeFinalRevocation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		var independent error
		if fail {
			name, independent = "independent_error", errors.New("retained generation callback failure")
		}
		t.Run(name, func(t *testing.T) {
			launcher, readiness := &fakeManagedLauncher{}, NewReadinessOwner(true)
			entered, release := make(chan struct{}), make(chan struct{})
			retained := make(chan bool, 1)
			var releaseOnce sync.Once
			resume := func() { releaseOnce.Do(func() { close(release) }) }
			defer resume()
			var calls atomic.Int32
			var controller *Controller
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				switch request.URL.Path {
				case "/ready":
					return testHTTPResponse(http.StatusOK, ""), nil
				case "/quicktunnel":
					return testHTTPResponse(http.StatusOK, `{"hostname":"unit.trycloudflare.com"}`), nil
				default:
					clone := request.Clone(request.Context())
					clone.URL.Scheme, clone.URL.Host = "http", controller.listener.Addr().String()
					return http.DefaultTransport.RoundTrip(clone)
				}
			})}
			var err error
			controller, err = NewController(Options{Mode: ModeManagedQuickTunnel, Launcher: launcher,
				Handler: http.NotFoundHandler(), HTTPClient: client, Readiness: readiness,
				OnGeneration: func(_ context.Context, generation Generation) error {
					if calls.Add(1) == 2 {
						close(entered)
						<-release
						retained <- readiness.Snapshot(time.Now()).Exposure != nil
						return independent
					}
					return nil
				}})
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			defer func() { resume(); _ = controller.Stop(context.Background()) }()
			launcher.process(0).fail(errors.New("injected tunnel loss"))
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("replacement did not reach its existing generation callback")
			}
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			stopped := make(chan error, 1)
			go func() { stopped <- controller.Stop(canceled) }()
			var result error
			early := false
			select {
			case result = <-stopped:
				early = true
			case <-time.After(5 * time.Second):
			}
			resume()
			if !early {
				result = <-stopped
			}
			<-controller.done
			if !<-retained {
				t.Error("exposure was revoked before the admitted generation writer joined")
			}
			if early {
				t.Error("stop returned before its admitted generation writer joined")
			}
			if readiness.Snapshot(time.Now()).Exposure != nil {
				t.Error("a generation writer restored exposure after final revocation")
			}
			if !errors.Is(result, context.Canceled) {
				t.Errorf("original canceled budget lost: %v", result)
			}
			if independent != nil && (!errors.Is(result, independent) || strings.Count(result.Error(), independent.Error()) != 1) {
				t.Errorf("independent generation failure not retained exactly once: %v", result)
			}
			for index := range 2 {
				select {
				case <-launcher.process(index).Done():
				default:
					t.Errorf("managed child%d remains live after stop", index)
				}
			}
		})
	}
}
