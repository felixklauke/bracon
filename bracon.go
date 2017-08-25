package bracon

import "bytes"
import "encoding/gob"
import "errors"
import "time"
import "github.com/boltdb/bolt"

type Storage struct {

	db *bolt.DB
}

var ErrorNotFound = errors.New("bacon didn't find anything, maybe the wrong door?")
var ErrorInvalidContent  = errors.New("bacon did find something not as good as excpected.")
var storageName = []byte("bracon")

func Open(path string) (*Storage, error) {
	opts := &bolt.Options{
		Timeout: 50 * time.Millisecond,
	}
	if db, err := bolt.Open(path, 0640, opts); err != nil {
		return nil, err
	} else {
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(storageName)
			return err
		})
		if err != nil {
			return nil, err
		} else {
			return &Storage{db: db}, nil
		}
	}
}

func (kvs *Storage) Put(key string, value interface{}) error {
	if value == nil {
		return ErrorInvalidContent
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return nil
	}
	return kvs.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(storageName).Put([]byte(key), buf.Bytes())
	})
}

func (kvs *Storage) Get(key string, value interface{}) error {
	return kvs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(storageName).Cursor()
		if k, v := c.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrorNotFound
		} else if value == nil {
			return nil
		} else {
			d := gob.NewDecoder(bytes.NewReader(v))
			return d.Decode(value)
		}
	})
}

func (kvs *Storage) Delete(key string) error {
	return kvs.db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket(storageName).Cursor()
		if k, _ := c.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrorNotFound
		} else {
			return c.Delete()
		}
	})
}

func (kvs *Storage) Close() error {
	return kvs.db.Close()
}
