package dashboard

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"empireai/internal/protocolheaders"
)

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/dashboard", s.handlePage)
	mux.HandleFunc("/dashboard/", s.handlePage)
	mux.Handle("/dashboard/assets/", http.StripPrefix("/dashboard/assets/", http.FileServer(http.FS(dashboardStatic))))
	mux.HandleFunc("/dashboard/api/overview", s.handleOverview)
	mux.HandleFunc("/dashboard/api/agents", s.handleAgents)
	mux.HandleFunc("/dashboard/api/agents/", s.handleAPIAgentPrompt)
	mux.HandleFunc("/dashboard/api/events", s.handleEvents)
	mux.HandleFunc("/dashboard/api/events/stream", s.handleEventStream)
	mux.HandleFunc("/dashboard/api/events/flow", s.handleFlowEvents)
	mux.HandleFunc("/dashboard/api/events/", s.handleEventDetail)
	mux.HandleFunc("/dashboard/api/runtime/logs", s.handleRuntimeLogs)
	mux.HandleFunc("/dashboard/api/runtime/incidents", s.handleRuntimeIncidents)
	mux.HandleFunc("/dashboard/api/conversations", s.handleConversations)
	mux.HandleFunc("/dashboard/api/conversations/", s.handleConversationDetail)
	mux.HandleFunc("/dashboard/api/funnel", s.handleFunnel)
	mux.HandleFunc("/dashboard/api/pipeline/shards", s.handlePipelineShards)
	mux.HandleFunc("/dashboard/api/pipeline/shards/", s.handlePipelineShardDetail)
	mux.HandleFunc("/dashboard/api/mailbox", s.handleMailbox)
	mux.HandleFunc("/dashboard/api/tasks", s.handleTasks)
	mux.HandleFunc("/dashboard/api/tasks/stats", s.handleTaskStats)
	mux.HandleFunc("/dashboard/api/tasks/", s.handleTaskDetail)
	mux.HandleFunc("/dashboard/api/digest", s.handleDigest)
	mux.HandleFunc("/dashboard/api/health", s.handleHealth)
	mux.HandleFunc("/dashboard/api/health/pipeline", s.handlePipelineHealth)
	mux.HandleFunc("/dashboard/api/graph", s.handleGraph)
	mux.HandleFunc("/dashboard/api/pipeline/graph", s.handlePipelineGraph)
	mux.HandleFunc("/dashboard/api/control/targets", s.handleControlTargets)
	mux.HandleFunc("/dashboard/api/control/seed-org", s.handleControlSeedOrg)
	mux.HandleFunc("/dashboard/api/control/verticals/create", s.handleControlCreateVertical)
	mux.HandleFunc("/dashboard/api/control/agents/restart", s.handleControlAgentRestart)
	mux.HandleFunc("/dashboard/api/control/agents/replay", s.handleControlAgentReplay)
	mux.HandleFunc("/dashboard/api/control/events/requeue", s.handleControlEventRequeue)
	mux.HandleFunc("/dashboard/api/control/runtime", s.handleControlRuntime)
	mux.HandleFunc("/dashboard/api/control/directive", s.handleControlDirective)
	mux.HandleFunc("/dashboard/api/control/chat", s.handleControlChat)
	mux.HandleFunc("/dashboard/api/control/mailbox/decide", s.handleControlMailboxDecide)
	mux.HandleFunc("/dashboard/api/holding", s.handleHolding)
	mux.HandleFunc("/dashboard/api/holding/vertical", s.handleHoldingVerticalDetail)
	mux.HandleFunc("/dashboard/api/verticals/", s.handleVerticalTrace)
	mux.HandleFunc("/dashboard/api/templates/publish", s.handleAPITemplatePublish)
	mux.HandleFunc("/dashboard/api/templates/", s.handleAPITemplatePrompt)

	// Spec v2.0 API surface aliases (Phase 1). These are the "real" API routes.
	mux.HandleFunc("/api/tasks", s.handleTasks)
	mux.HandleFunc("/api/tasks/stats", s.handleTaskStats)
	mux.HandleFunc("/api/tasks/", s.handleTaskDetail)
	mux.HandleFunc("/api/mailbox", s.handleMailbox)
	mux.HandleFunc("/api/mailbox/", s.handleAPIMailboxDetail)
	mux.HandleFunc("/api/events", s.handleAPIEvents)
	mux.HandleFunc("/api/events/flow", s.handleFlowEvents)
	mux.HandleFunc("/api/events/", s.handleEventDetail)
	mux.HandleFunc("/api/runtime/logs", s.handleRuntimeLogs)
	mux.HandleFunc("/api/runtime/incidents", s.handleRuntimeIncidents)
	mux.HandleFunc("/api/verticals", s.handleAPIVerticals)
	mux.HandleFunc("/api/verticals/", s.handleAPIVerticalDetail)
	mux.HandleFunc("/api/chat/", s.handleAPIChat)
	mux.HandleFunc("/api/conversations", s.handleConversations)
	mux.HandleFunc("/api/conversations/", s.handleConversationDetail)
	mux.HandleFunc("/api/agents/", s.handleAPIAgentPrompt)
	mux.HandleFunc("/api/templates/publish", s.handleAPITemplatePublish)
	mux.HandleFunc("/api/templates/", s.handleAPITemplatePrompt)
	mux.HandleFunc("/api/directive", s.handleAPIDirective)
	mux.HandleFunc("/api/budget", s.handleAPIBudget)
	mux.HandleFunc("/api/holding", s.handleHolding)
	mux.HandleFunc("/api/holding/vertical", s.handleHoldingVerticalDetail)
	mux.HandleFunc("/api/health/pipeline", s.handlePipelineHealth)
	mux.HandleFunc("/api/pipeline/shards", s.handlePipelineShards)
	mux.HandleFunc("/api/pipeline/shards/", s.handlePipelineShardDetail)
	mux.HandleFunc("/api/pipeline/graph", s.handlePipelineGraph)
	mux.HandleFunc("/api/graph", s.handleGraph)
	mux.HandleFunc("/health/pipeline", s.handlePipelineHealth)
	return s.authMiddleware(mux)
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	key := strings.TrimSpace(os.Getenv("EMPIREAI_API_KEY"))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Dashboard page + assets are always accessible locally; APIs require a key.
		if strings.HasPrefix(r.URL.Path, "/dashboard/api/") || strings.HasPrefix(r.URL.Path, "/api/") {
			if key == "" {
				writeErr(w, http.StatusInternalServerError, fmt.Errorf("EMPIREAI_API_KEY is not set"))
				return
			}
			supplied := strings.TrimSpace(r.Header.Get(protocolheaders.APIKeyHeader))
			if supplied == "" {
				// SSE EventSource can't set headers; allow query param fallback.
				supplied = strings.TrimSpace(r.URL.Query().Get("key"))
			}
			if supplied == "" || supplied != key {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// API surface (§14.7). These handlers are thin aliases over the dashboard store layer.
