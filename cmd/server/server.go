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
			MaxBatchSizeBytes:  65536,
			ClientPort:         ":9099",
			EventsPort:         ":9090",
			ConnDeadlineMs:     20000,
		},
	)
	srv.Start()
}
