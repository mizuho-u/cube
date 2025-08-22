package store

import (
	"fmt"
)

type InMemoryTaskStore[T any] struct {
	Db map[string]T
}

func NewInMemoryTaskStore[T any]() *InMemoryTaskStore[T] {
	return &InMemoryTaskStore[T]{
		Db: make(map[string]T),
	}
}

func (i *InMemoryTaskStore[T]) Count() (int, error) {
	return len(i.Db), nil
}

func (i *InMemoryTaskStore[T]) Get(key string) (v T, err error) {

	v, ok := i.Db[key]
	if !ok {
		return v, fmt.Errorf("key %s not found", key)
	}

	return v, nil
}

func (i *InMemoryTaskStore[T]) List() ([]T, error) {
	var vs []T
	for _, v := range i.Db {
		vs = append(vs, v)
	}

	return vs, nil
}

// Put implements Store.
func (i *InMemoryTaskStore[T]) Put(key string, value T) error {
	i.Db[key] = value
	return nil
}
