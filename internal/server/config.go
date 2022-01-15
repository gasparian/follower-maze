package server

type runtime struct {
	maxProcs int `toml:"max_procs"`
}

type eventsServer struct {
	port               string
	eventsQueueMaxSize int `toml:"events_queue_max_size"`
	maxBuffSizeBytes   int `toml:"max_buff_size_bytes"`
	maxBatchSize       int `toml:"max_batch_size"`
	readTimeoutMs      int `toml:"read_timeout_ms"`
}

type clientServer struct {
	port                   string
	maxBuffSizeBytes       int `toml:"max_buff_size_bytes"`
	sendEventsQueueMaxSize int `toml:"send_events_queue_max_size"`
}

type FollowerServerConfig struct {
	Runtime runtime      `toml:"runtime"`
	Events  eventsServer `toml:"events"`
	Client  clientServer `toml:"client"`
}
