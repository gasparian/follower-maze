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

	srv := &server.FollowerServer{
		ClientServer: server.NewClientServer(MaxBuffSizeBytes, ClientPort),
		EventsServer: server.NewEventsServer(MaxBuffSizeBytes, EventsQueueMaxSize, EventsPort),
	}
	srv.Start()
}
