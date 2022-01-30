package server

// Runtime defines go runtime config
type Runtime struct {
	MaxProcs int `toml:"max_procs"`
}

// EventsConfig holds params for event parsing server
type EventsConfig struct {
	Port               string
	EventsQueueMaxSize int `toml:"events_queue_max_size"`
	MaxBuffSizeBytes   int `toml:"max_buff_size_bytes"`
}

// FollowerServerConfig ...
type FollowerServerConfig struct {
	Runtime Runtime      `toml:"runtime"`
	Events  EventsConfig `toml:"events"`
	Client  EventsConfig `toml:"client"`
}
