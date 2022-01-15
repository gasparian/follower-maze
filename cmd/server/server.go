package main

import (
	"flag"
	"log"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/gasparian/follower-maze/internal/server"
)

var (
	configPath string
)

func init() {
	flag.StringVar(&configPath, "config-path", "configs/follower-server.toml", "path to config file")
}

func main() {

	// TODO: keep it in config toml
	runtime.GOMAXPROCS(4)
	EventsQueueMaxSize := 1000
	MaxBuffSizeBytes := 8000
	ClientPort := ":9099"
	EventsPort := ":9090"
	SendEventsQueueMaxSize := 100
	// MaxBatchSize := 1000
	// ReadTimeoutMs := 500

	flag.Parse()

	config := &server.FollowerServerConfig{}
	_, err := toml.DecodeFile(configPath, config)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Config: ", config)

	srv := &server.FollowerServer{
		ClientServer: server.NewClientAcceptor(MaxBuffSizeBytes, ClientPort),
		EventsServer: server.NewEventsParserPQueue(MaxBuffSizeBytes, EventsQueueMaxSize, EventsPort),
		// EventsServer: server.NewEventsParserBatched(
		// 	MaxBuffSizeBytes,
		// 	MaxBatchSize,
		// 	EventsQueueMaxSize,
		// 	ReadTimeoutMs,
		// 	EventsPort,
		// ),
		SendEventsQueueMaxSize: SendEventsQueueMaxSize,
	}
	srv.Start()
}
