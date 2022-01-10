package follower

type Request struct {
	Payload  string
	Response chan error
}

type Client struct {
	ID   uint64
	Chan chan *Request
}
