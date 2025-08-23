package store

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"go.etcd.io/bbolt"
)

type PersistentTaskStore[T any] struct {
	Db       *bbolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

func (p *PersistentTaskStore[T]) CreateBucket() error {
	return p.Db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte(p.Bucket))
		return err
	})
}

func (p *PersistentTaskStore[T]) Count() (int, error) {
	count := 0

	err := p.Db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(p.Bucket))
		err := b.ForEach(func(k, v []byte) error {
			count++
			return nil
		})

		return err
	})

	if err != nil {
		return -1, err
	}

	return count, nil

}

func (p *PersistentTaskStore[T]) Get(key string) (v T, err error) {

	err = p.Db.View(func(tx *bbolt.Tx) error {

		b := tx.Bucket([]byte(p.Bucket))
		t := b.Get([]byte(key))
		if t == nil {
			return fmt.Errorf("key %s not found", key)
		}

		err := json.Unmarshal(t, &v)
		if err != nil {
			return err
		}

		return nil
	})

	return
}

func (p *PersistentTaskStore[T]) List() (vs []T, err error) {

	err = p.Db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(p.Bucket))
		err := b.ForEach(func(k, v []byte) error {

			var ret T
			err := json.Unmarshal(v, &ret)
			if err != nil {
				return err
			}

			vs = append(vs, ret)
			return nil
		})

		return err
	})

	return

}

func (p *PersistentTaskStore[T]) Put(key string, value T) error {

	return p.Db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(p.Bucket))

		buf, err := json.Marshal(value)
		if err != nil {
			return err
		}

		err = b.Put([]byte(key), buf)
		if err != nil {
			return err
		}

		return nil
	})

}

func NewPersistentTaskStore[T any](file string, mode os.FileMode, bucket string) (*PersistentTaskStore[T], error) {

	db, err := bbolt.Open(file, mode, nil)
	if err != nil {
		return nil, err
	}

	t := &PersistentTaskStore[T]{
		Db:       db,
		DbFile:   file,
		FileMode: mode,
		Bucket:   bucket,
	}

	err = t.CreateBucket()
	if err != nil {
		log.Printf("bucket already exists, will use it instead of creating new one")
	}

	return t, nil
}

func (p *PersistentTaskStore[T]) Close() error {
	return p.Db.Close()
}
