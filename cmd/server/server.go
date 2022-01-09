package main

import (
	"runtime"

	"github.com/gasparian/follower-maze/internal/server"
)

func main() {
	runtime.GOMAXPROCS(4)
	srv := server.New(
		&server.Config{
			EventsQueueMaxSize: 10000,
			// MaxBuffSizeBytes:   65536,
			MaxBuffSizeBytes: 8000,
			ClientPort:       ":9099",
			EventsPort:       ":9090",
			// ConnDeadlineMs: 20000,
			ConnDeadlineMs: 100000, // TODO: drop this paramter? For instance, 20s. is not enough to process 100k events
		},
	)
	srv.Start()
}
