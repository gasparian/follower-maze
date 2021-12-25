TEST=go test -v -cover -race -count=1 -timeout 30s $(1)
DEFAULT_GOAL := help

.SILENT:
.PHONY: help, build, build-static, run-simulator, test

help:
	echo
	echo '  Usage:'
	echo '    make <target>'
	echo
	echo '  Targets:'
	echo '    build          generate binary'
	echo '    build          generate statically linked binary'
	echo '    simulate       run events and clients simulator'
	echo '    simulate-test  run simulator configured just to test app functionality'
	echo '    test           run tests'

build:
	go build -v ./cmd/server

build-static:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
		-ldflags "-w -extldflags -static" \
		-tags osusergo,netgo \
		-v -a ./cmd/server

simulate:
	./simulator/followermaze.sh

simulate-test:
	totalEvents=100 concurrencyLevel=2 \
	numberOfUsers=2 timeout=20000 \
	maxEventSourceBatchSize=2 \
	./simulator/followermaze.sh

.PHONY: test
test:
	$(call TEST,./internal/...)
