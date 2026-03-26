.PHONY: test test-cover test-cover-runtime check-runtime-cover check-key-package-cover \
	lint dashboard-build dashboard-redeploy dashboard-logs dashboard-ps \
	dashboard-local dashboard-local-build dashboard-local-stop \
	postgres-up postgres-reset empire-local \
	sync-current-spec

COMPOSE ?= docker compose
DASHBOARD_LOCAL_PORT ?= 4173
DASHBOARD_API_ORIGIN ?= http://127.0.0.1:8081
EMPIRE_HEALTH_PORT ?= 18082
EMPIRE_CONTRACTS ?= docs/specs/mas-platform/empire/contracts
MAS_DB_HOST ?= 127.0.0.1
MAS_DB_PORT ?= 5432
MAS_DB_NAME ?= empireai
MAS_DB_USER ?= postgres
MAS_DB_PASSWORD ?= postgres
MAS_DB_SSLMODE ?= disable

COVER_DIR ?= coverage
MIN_RUNTIME_COVER ?= 32
MIN_PIPELINE_COVER ?= 34
MIN_TOOLS_COVER ?= 32
MIN_MANAGER_COVER ?= 22
MIN_CONTRACTS_COVER ?= 43
MIN_BUS_COVER ?= 38

test:
	go test ./...

test-cover:
	mkdir -p $(COVER_DIR)
	go test ./... -coverprofile=$(COVER_DIR)/all.out
	go tool cover -html=$(COVER_DIR)/all.out -o $(COVER_DIR)/all.html
	go tool cover -func=$(COVER_DIR)/all.out | tail -n 1
	@echo "Wrote $(COVER_DIR)/all.html"

test-cover-runtime:
	mkdir -p $(COVER_DIR)
	go test ./internal/runtime/... -coverprofile=$(COVER_DIR)/runtime.out
	go tool cover -html=$(COVER_DIR)/runtime.out -o $(COVER_DIR)/runtime.html
	go tool cover -func=$(COVER_DIR)/runtime.out | tail -n 1
	@echo "Wrote $(COVER_DIR)/runtime.html"

check-runtime-cover: test-cover-runtime
	./scripts/check_coverage.sh $(COVER_DIR)/runtime.out $(MIN_RUNTIME_COVER)

check-key-package-cover:
	mkdir -p $(COVER_DIR)
	go test ./internal/runtime/pipeline -coverprofile=$(COVER_DIR)/pipeline.out
	./scripts/check_coverage.sh $(COVER_DIR)/pipeline.out $(MIN_PIPELINE_COVER)
	go test ./internal/runtime/tools -coverprofile=$(COVER_DIR)/tools.out
	./scripts/check_coverage.sh $(COVER_DIR)/tools.out $(MIN_TOOLS_COVER)
	go test ./internal/runtime/manager -coverprofile=$(COVER_DIR)/manager.out
	./scripts/check_coverage.sh $(COVER_DIR)/manager.out $(MIN_MANAGER_COVER)
	go test ./internal/runtime/contracts -coverprofile=$(COVER_DIR)/contracts.out
	./scripts/check_coverage.sh $(COVER_DIR)/contracts.out $(MIN_CONTRACTS_COVER)
	go test ./internal/runtime/bus -coverprofile=$(COVER_DIR)/bus.out
	./scripts/check_coverage.sh $(COVER_DIR)/bus.out $(MIN_BUS_COVER)

lint:
	golangci-lint run ./...

dashboard-build:
	$(COMPOSE) build dashboard

dashboard-redeploy:
	$(COMPOSE) up -d --build dashboard
	@echo "Dashboard redeployed: http://localhost:8070/dashboard/"

dashboard-logs:
	$(COMPOSE) logs -f dashboard

dashboard-ps:
	$(COMPOSE) ps dashboard orchestrator

dashboard-local-build:
	cd internal/dashboard/ui && npm run build

dashboard-local-stop:
	@pids="$$(lsof -ti tcp:$(DASHBOARD_LOCAL_PORT) 2>/dev/null)"; \
	if [ -n "$$pids" ]; then \
		echo "Stopping dashboard server on port $(DASHBOARD_LOCAL_PORT): $$pids"; \
		kill $$pids; \
	else \
		echo "No dashboard server listening on port $(DASHBOARD_LOCAL_PORT)"; \
	fi

dashboard-local: dashboard-local-build dashboard-local-stop
	cd internal/dashboard/ui && PLAYWRIGHT_DASHBOARD_PORT=$(DASHBOARD_LOCAL_PORT) DASHBOARD_API_ORIGIN=$(DASHBOARD_API_ORIGIN) node scripts/serve-smoke-dashboard.mjs

postgres-up:
	$(COMPOSE) up -d postgres

postgres-reset: postgres-up
	PGPASSWORD=$(MAS_DB_PASSWORD) psql -h $(MAS_DB_HOST) -p $(MAS_DB_PORT) -U $(MAS_DB_USER) -d $(MAS_DB_NAME) -v ON_ERROR_STOP=1 \
		-c "DROP SCHEMA IF EXISTS public CASCADE;" \
		-c "CREATE SCHEMA public;" \
		-c "GRANT ALL ON SCHEMA public TO $(MAS_DB_USER);" \
		-c "GRANT ALL ON SCHEMA public TO public;"

empire-local: postgres-up
	MAS_DB_HOST=$(MAS_DB_HOST) MAS_DB_PORT=$(MAS_DB_PORT) MAS_DB_NAME=$(MAS_DB_NAME) MAS_DB_USER=$(MAS_DB_USER) MAS_DB_PASSWORD=$(MAS_DB_PASSWORD) MAS_DB_SSLMODE=$(MAS_DB_SSLMODE) \
		go run ./cmd/mas -contracts $(EMPIRE_CONTRACTS) -store postgres -health-addr 127.0.0.1:$(EMPIRE_HEALTH_PORT)

sync-current-spec:
	./scripts/sync_current_spec.sh
