package builder

import (
	"net/http"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/gorilla/websocket"
)

type handler struct {
	health         HealthChecker
	entities       EntityReader
	runtime        RuntimeController
	credentials    runtimecredentials.Store
	authToken      string
	version        string
	semanticSource semanticview.Source
	currentSource  SourceProvider
	currentRuntime RuntimeProvider
	projectControl ProjectController
	runHub         *runHub
	mux            *http.ServeMux
}

var healthHeartbeatInterval = 5 * time.Second

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}
