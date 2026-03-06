.PHONY: test test-cover test-cover-runtime check-runtime-cover \
	dashboard-build dashboard-redeploy dashboard-logs dashboard-ps \
	sync-current-spec

COMPOSE ?= docker compose

COVER_DIR ?= coverage
MIN_RUNTIME_COVER ?= 74

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
	go test ./internal/runtime -coverprofile=$(COVER_DIR)/runtime.out
	go tool cover -html=$(COVER_DIR)/runtime.out -o $(COVER_DIR)/runtime.html
	go tool cover -func=$(COVER_DIR)/runtime.out | tail -n 1
	@echo "Wrote $(COVER_DIR)/runtime.html"

check-runtime-cover: test-cover-runtime
	./scripts/check_coverage.sh $(COVER_DIR)/runtime.out $(MIN_RUNTIME_COVER)

dashboard-build:
	$(COMPOSE) build dashboard

dashboard-redeploy:
	$(COMPOSE) up -d --build dashboard
	@echo "Dashboard redeployed: http://localhost:8070/dashboard/"

dashboard-logs:
	$(COMPOSE) logs -f dashboard

dashboard-ps:
	$(COMPOSE) ps dashboard orchestrator

sync-current-spec:
	./scripts/sync_current_spec.sh
