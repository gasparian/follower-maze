package kvstore

type KVStore map[uint64]interface{}

type KeysIterator chan uint64

func (it KeysIterator) Next() (uint64, bool) {
	id, opened := <-it
	if !opened {
		return 0, false
	}
	return id, true
}

func (k KVStore) GetIterator() KeysIterator {
	it := make(KeysIterator)
	go func() {
		for k := range k {
			it <- k
		}
		close(it)
	}()
	return it
}
