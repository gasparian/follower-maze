package main

import (
	"runtime"

	srv "github.com/gasparian/follower-maze/internal/server"
)

func main() {
	runtime.GOMAXPROCS(4)
	server := srv.New(
		&srv.Config{
			EventsQueueMaxSize: 10000,
			MaxBatchSizeBytes:  65536,
			ClientPort:         ":9099",
			EventsPort:         ":9090",
			ConnDeadlineMs:     20000,
		},
	)
	server.Start()
}
