package main

import (
	"runtime"

	"github.com/gasparian/follower-maze/internal/server"
)

func main() {
	runtime.GOMAXPROCS(4)
	srv := server.New(
		&server.Config{
			EventsQueueMaxSize: 2000,
			// MaxBuffSizeBytes:   65536,
			MaxBuffSizeBytes: 8000,
			ClientPort:       ":9099",
			EventsPort:       ":9090",
		},
	)
	srv.Start()
}
