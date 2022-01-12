package main

import (
	"runtime"

	"github.com/gasparian/follower-maze/internal/server"
)

func main() {

	// TODO: keep it in config toml
	runtime.GOMAXPROCS(4)
	EventsQueueMaxSize := 2000
	MaxBuffSizeBytes := 8000
	ClientPort := ":9099"
	EventsPort := ":9090"
	SendEventsQueueMaxSize := 100
	// MaxBatchSize := 100
	// ReadTimeoutMs := 500

	srv := &server.FollowerServer{
		ClientServer:           server.NewClientAcceptor(MaxBuffSizeBytes, ClientPort),
		EventsServer:           server.NewEventsParserPQueue(MaxBuffSizeBytes, EventsQueueMaxSize, EventsPort),
		SendEventsQueueMaxSize: SendEventsQueueMaxSize,
		// EventsServer: server.NewEventsParserBatched(
		// 	MaxBuffSizeBytes,
		// 	MaxBatchSize: MaxBatchSize,
		// 	ReadTimeoutMs: ReadTimeoutMs,
		// 	EventsQueueMaxSize,
		// 	EventsPort,
		// ),
	}
	srv.Start()
}
