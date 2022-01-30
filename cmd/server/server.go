package main

import (
	"flag"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/gasparian/follower-maze/internal/server"
	"github.com/golang/glog"
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
		glog.Fatal(err)
	}

	runtime.GOMAXPROCS(config.Runtime.MaxProcs)

	eventsServer := server.NewEventsParserPQueue(
		config.Events.MaxBuffSizeBytes,
		config.Events.EventsQueueMaxSize,
		config.Events.Port,
	)
	clientServer := server.NewClientAcceptor(
		config.Client.MaxBuffSizeBytes,
		config.Client.EventsQueueMaxSize,
		config.Client.Port,
	)

	srv := server.NewFollowerServer(clientServer, eventsServer)
	glog.Infof("INFO: Starting socket servers: Events %s, Client %s\n", config.Events.Port, config.Client.Port)
	srv.Start()
}
