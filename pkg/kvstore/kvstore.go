package kvstore

import (
	"sync"
)

type KVStore struct {
	mx    sync.RWMutex
	items map[uint64]interface{}
}

func New() *KVStore {
	return &KVStore{
		items: make(map[uint64]interface{}),
	}
}

func (k *KVStore) Get(id uint64) interface{} {
	k.mx.RLock()
	defer k.mx.RUnlock()
	return k.items[id]
}

func (k *KVStore) Set(id uint64, val interface{}) {
	k.mx.Lock()
	defer k.mx.Unlock()
	k.items[id] = val
}

func (k *KVStore) Del(id uint64) {
	k.mx.Lock()
	defer k.mx.Unlock()
	delete(k.items, id)
}

func (k *KVStore) Len() int {
	return len(k.items)
}

type KeysIterator chan uint64

func (it KeysIterator) Next() (uint64, bool) {
	id, opened := <-it
	if !opened {
		return 0, false
	}
	return id, true
}

func (k *KVStore) GetIterator() KeysIterator {
	k.mx.RLock()
	defer k.mx.RUnlock()

	it := make(KeysIterator, len(k.items))
	go func() {
		for k := range k.items {
			it <- k
		}
		close(it)
	}()
	return it
}
