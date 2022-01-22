package follower

// Request holds data being transmitted to the client and it's answer
type Request struct {
	Payload  string
	Response chan error
}

// Client holds client ID and channel to communicate with this client
type Client struct {
	ID   uint64
	Chan chan *Request
}
