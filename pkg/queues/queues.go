package queues

type Queue interface {
	Push(interface{})
	Pop() interface{}
}
