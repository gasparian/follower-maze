package server

// Runtime defines go runtime config
type Runtime struct {
	MaxProcs int `toml:"max_procs"`
}

// EventsConfig holds params for event parsing server
type EventsConfig struct {
	Port               string
	Batched            bool
	EventsQueueMaxSize int `toml:"events_queue_max_size"`
	MaxBuffSizeBytes   int `toml:"max_buff_size_bytes"`
	MaxBatchSize       int `toml:"max_batch_size"`
	ReadTimeoutMs      int `toml:"read_timeout_ms"`
}

// ClientConfig holds params for client acceptor server
type ClientConfig struct {
	Port                   string
	MaxBuffSizeBytes       int `toml:"max_buff_size_bytes"`
	SendEventsQueueMaxSize int `toml:"send_events_queue_max_size"`
}

// FollowerServerConfig ...
type FollowerServerConfig struct {
	Runtime Runtime      `toml:"runtime"`
	Events  EventsConfig `toml:"events"`
	Client  ClientConfig `toml:"client"`
}
