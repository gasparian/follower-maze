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
	flag.Parse()

	var config server.FollowerServerConfig
	_, err := toml.DecodeFile(configPath, &config)
	if err != nil {
		log.Fatal(err)
	}

	runtime.GOMAXPROCS(config.Runtime.MaxProcs)

	var eventsServer server.EventsServer
	if config.Events.Batched {
		eventsServer = server.NewEventsParserBatched(
			config.Events.MaxBuffSizeBytes,
			config.Events.MaxBatchSize,
			config.Events.EventsQueueMaxSize,
			config.Events.ReadTimeoutMs,
			config.Events.Port,
		)
	} else {
		eventsServer = server.NewEventsParserPQueue(
			config.Events.MaxBuffSizeBytes,
			config.Events.EventsQueueMaxSize,
			config.Events.Port,
		)
	}

	clientServer := server.NewClientAcceptor(
		config.Client.MaxBuffSizeBytes,
		config.Client.SendEventsQueueMaxSize,
		config.Client.Port,
	)

	srv := server.NewFollowerServer(clientServer, eventsServer)
	srv.Start()
}
