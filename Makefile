TEST=go test -v -cover -race -count=1 -timeout 30s $(1)

.SILENT:
.PHONY: build
build:
	go build -v ./cmd/server

build-static:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -v -a ./cmd/server

.PHONY: test
test:
	$(call TEST,./internal/...)

.DEFAULT_GOAL := build