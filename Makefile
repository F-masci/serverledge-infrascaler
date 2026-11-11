BIN=bin
GO=go
all: serverledge executor serverledge-cli serverledge-profiling lb

serverledge:
	$(GO) build -o $(BIN)/$@ cmd/$@/main.go

lb:
	CGO_ENABLED=0 $(GO) build -o $(BIN)/$@ cmd/$@/main.go

serverledge-cli:
	CGO_ENABLED=0 $(GO) build -o $(BIN)/$@ cmd/cli/main.go

serverledge-profiling:
	CGO_ENABLED=0 $(GO) build -o $(BIN)/$@ ./cmd/profiling

executor:
	CGO_ENABLED=0 $(GO) build -o $(BIN)/$@ cmd/$@/executor.go

DOCKERHUB_USER=grussorusso
images:  image-python image-nodejs17ng image-base image-go
image-python:
	docker build -t $(DOCKERHUB_USER)/serverledge-python314 -f images/python314/Dockerfile .
	docker build -t $(DOCKERHUB_USER)/serverledge-python312ml -f images/python312ml/Dockerfile .
image-base:
	docker build -t $(DOCKERHUB_USER)/serverledge-base -f images/base-alpine/Dockerfile .
image-nodejs17ng:
	docker build -t $(DOCKERHUB_USER)/serverledge-nodejs17ng -f images/nodejs17ng/Dockerfile .
image-go:
	docker build -t $(DOCKERHUB_USER)/serverledge-go125 -f images/go125/Dockerfile .

push-images:
	docker push $(DOCKERHUB_USER)/serverledge-python314
	docker push $(DOCKERHUB_USER)/serverledge-base
	docker push $(DOCKERHUB_USER)/serverledge-nodejs17ng
	docker push $(DOCKERHUB_USER)/serverledge-python312ml
	docker push $(DOCKERHUB_USER)/serverledge-go125

# Runs integration tests (all tests EXCEPT unit tests)
test:
	$(GO) test -v $(shell $(GO) list ./... | grep -Ev 'internal/container|examples')
	$(GO) test -v -count=1 -timeout=2m ./internal/test -args -platform-profile=lb
	$(GO) test -v -count=1 -timeout=2m -run '^TestGeneralLoadBalancer' ./internal/test -args -platform-profile=lb -lb-test-mode=Random
	$(GO) test -v -count=1 -timeout=2m -run '^TestGeneralLoadBalancer' ./internal/test -args -platform-profile=lb -lb-test-mode=MAB
	$(GO) test -v -count=1 -timeout=2m ./internal/test -args -platform-profile=edge
	$(GO) test -v -count=1 -timeout=2m ./internal/test -args -platform-profile=executor
	$(GO) test -v -count=1 -timeout=2m ./internal/test -args -platform-profile=discovery

# Runs only unit tests
unit-test:
	go test -v -short ./internal/container/... ./internal/lb/...


# Generates combined coverage from unit and integration tests
coverage:
	@set -eu; \
	tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	packages="$$( $(GO) list ./... | grep -v '/examples' )"; \
	coverpkg="$$(printf '%s\n' "$$packages" | paste -sd, -)"; \
	flags="-v -p 1 -count=1 -covermode=atomic -coverpkg=$$coverpkg"; \
	$(GO) test $$flags -coverprofile="$$tmp/base.out" $$packages; \
	$(GO) test $$flags -timeout=2m -coverprofile="$$tmp/lb.out" ./internal/test -args -platform-profile=lb; \
	$(GO) test $$flags -timeout=2m -coverprofile="$$tmp/lb-random.out" -run '^TestGeneralLoadBalancer' ./internal/test -args -platform-profile=lb -lb-test-mode=Random; \
	$(GO) test $$flags -timeout=2m -coverprofile="$$tmp/lb-mab.out" -run '^TestGeneralLoadBalancer' ./internal/test -args -platform-profile=lb -lb-test-mode=MAB; \
	$(GO) test $$flags -timeout=2m -coverprofile="$$tmp/edge.out" ./internal/test -args -platform-profile=edge; \
	$(GO) test $$flags -timeout=2m -coverprofile="$$tmp/executor.out" ./internal/test -args -platform-profile=executor; \
	$(GO) test $$flags -timeout=2m -coverprofile="$$tmp/discovery.out" ./internal/test -args -platform-profile=discovery; \
	awk 'FNR == 1 { next } \
	     { key = $$1 " " $$2; counts[key] += $$3 } \
	     END { print "mode: atomic"; \
	           for (key in counts) print key, counts[key] }' \
	     "$$tmp"/*.out > coverage.out

	@$(GO) tool cover -func=coverage.out | tail -n 1


.PHONY: serverledge serverledge-cli serverledge-profiling lb executor test unit-test coverage integration-test images

clean:
	@test -n "$(BIN)" && [ -d "$(BIN)" ] && rm -rf $(BIN) || { echo "Invalid BIN directory: $(BIN)"; exit 1; } && go clean -testcache

