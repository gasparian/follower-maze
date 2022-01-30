GO_BETA_CHECK=$(shell go1.18beta1 version 2>/dev/null)
GO=go1.18beta1
ifeq (${GO_BETA_CHECK},)
	GO=go
endif

DEFAULT_GOAL := help

ifndef LOG_LEVEL
	export LOG_LEVEL=info
endif

ifeq "${LOG_LEVEL}" "info"
	V=0
endif
ifeq "${LOG_LEVEL}" "debug"
	V=1
endif

.SILENT:
.PHONY: \
	help \
	install-hooks \
	build \
	build-static \
	run \
	run-simulator \
	test

help:
	echo
	echo '  Usage:'
	echo '    make <target>'
	echo
	echo '  Targets:'
	echo '    build          generate binary'
	echo '    build-static   generate statically linked binary'
	echo '    run            run compiled application'
	echo '    simulate       run events and clients simulator'
	echo '    simulate-test  run simulator configured just to test app functionality'
	echo '    test           run tests'

install-hooks:
	cp -rf .githooks/pre-commit .git/hooks/pre-commit

build:
	${GO} build -v ./cmd/server

build-static:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 ${GO} build \
		-ldflags "-w -extldflags -static" \
		-tags osusergo,netgo \
		-v -a ./cmd/server

run:
	./server -logtostderr -v=${V}

simulate:
	concurrencyLevel=100 \
	./simulator/followermaze.sh

simulate-test:
	logLevel=info \
	totalEvents=10000 \
	concurrencyLevel=10 \
	numberOfUsers=100 \
	timeout=20000 \
	maxEventSourceBatchSize=100 \
	./simulator/followermaze.sh

.PHONY: test
test:
	${GO} test -v -cover -race -count=1 -timeout 30s ./...
