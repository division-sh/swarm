.PHONY: test test-cover test-cover-runtime check-runtime-cover check-key-package-cover \
	lint postgres-up postgres-reset swarm-local \
	sync-current-spec

COMPOSE ?= docker compose
SWARM_HEALTH_PORT ?= 18082
SWARM_DB_HOST ?= 127.0.0.1
SWARM_DB_PORT ?= 5432
SWARM_DB_NAME ?= swarm
SWARM_DB_USER ?= postgres
SWARM_DB_PASSWORD ?= postgres
SWARM_DB_SSLMODE ?= disable

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

postgres-up:
	$(COMPOSE) up -d postgres

postgres-reset: postgres-up
	PGPASSWORD=$(SWARM_DB_PASSWORD) psql -h $(SWARM_DB_HOST) -p $(SWARM_DB_PORT) -U $(SWARM_DB_USER) -d $(SWARM_DB_NAME) -v ON_ERROR_STOP=1 \
		-c "DROP SCHEMA IF EXISTS public CASCADE;" \
		-c "CREATE SCHEMA public;" \
		-c "GRANT ALL ON SCHEMA public TO $(SWARM_DB_USER);" \
		-c "GRANT ALL ON SCHEMA public TO public;"

swarm-local: postgres-up
	SWARM_DB_HOST=$(SWARM_DB_HOST) SWARM_DB_PORT=$(SWARM_DB_PORT) SWARM_DB_NAME=$(SWARM_DB_NAME) SWARM_DB_USER=$(SWARM_DB_USER) SWARM_DB_PASSWORD=$(SWARM_DB_PASSWORD) SWARM_DB_SSLMODE=$(SWARM_DB_SSLMODE) \
		go run ./cmd/swarm -store postgres -health-addr 127.0.0.1:$(SWARM_HEALTH_PORT)

sync-current-spec:
	./scripts/sync_current_spec.sh
