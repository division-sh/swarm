package runforkexecution

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 3 && os.Args[1] == "--selected-fork-provider-worker-fixture" {
		os.Exit(runSelectedForkProviderWorkerFixture(os.Args[2]))
	}
	if len(os.Args) == 2 {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if code, handled := worker.RunArgument(ctx, os.Args[1], os.Stdin, os.Stdout); handled {
			os.Exit(code)
		}
	}
	os.Exit(m.Run())
}

// The provider-framing fixture simulates Docker selection and address mapping,
// but executes the real native worker and HTTP observation. It is not Docker
// transport or remote-lifetime proof; those have separate real-Docker controls.
func runSelectedForkProviderWorkerFixture(argument string) int {
	input, err := worker.InterruptibleInput(os.Stdin)
	if err != nil {
		return 2
	}
	defer input.Close()
	reader := bufio.NewReader(input)
	raw, err := worker.ReadFrame(reader)
	if err != nil {
		return 2
	}
	var request worker.Request
	if err := json.Unmarshal(raw, &request); err != nil {
		return 2
	}
	endpoint, err := url.Parse(request.Gateway.URL)
	if err != nil || endpoint.Hostname() != "host.docker.internal" || endpoint.Port() == "" {
		return 2
	}
	endpoint.Host = "127.0.0.1:" + endpoint.Port()
	request.Gateway.URL = endpoint.String()
	raw, err = json.Marshal(request)
	if err != nil {
		return 2
	}
	framedInput := selectedForkProviderWorkerInput{
		Reader: io.MultiReader(bytes.NewReader(append(raw, '\n')), reader),
		Closer: input,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	code, handled := worker.RunArgument(ctx, argument, framedInput, os.Stdout)
	if !handled {
		return 2
	}
	return code
}

type selectedForkProviderWorkerInput struct {
	io.Reader
	io.Closer
}
