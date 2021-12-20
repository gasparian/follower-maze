TEST=go test -v -cover -race -count=1 -timeout 30s $(1)
.DEFAULT_GOAL := build

.SILENT:
.PHONY: build
build:
	go build -v ./cmd/server

build-static:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -v -a ./cmd/server

run-simulator:
	./simulator/followermaze.sh

.PHONY: test
test:
	$(call TEST,./internal/...)
