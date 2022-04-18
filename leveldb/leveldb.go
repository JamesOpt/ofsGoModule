package ofsGoModule

import (
	"encoding/json"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type ULeveldb struct {
	Db *leveldb.DB
}

func OpenDb(path string) *ULeveldb  {
	newdb, err := leveldb.OpenFile(path, nil)

	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		newdb, err = leveldb.RecoverFile(path, nil)
	}

	if err != nil {
		panic(err)
	}

	uLeveldb := ULeveldb{}
	uLeveldb.Db = newdb
	return &uLeveldb
}

func (u *ULeveldb) Close()  {
	u.Db.Close()
}

/**
获取key
 */
func (u *ULeveldb) Get(key interface{}) ([]byte, error)  {
	return u.Db.Get([]byte(key.(string)), nil)
}

func (u *ULeveldb) Put(key string, value interface{}) error {
	data, err := json.Marshal(value)

	if err != nil{
		return err
	}
	
	return u.Db.Put([]byte(key), data, nil)
}

/**
删除
 */
func (u *ULeveldb)Delete(key string) error  {
	return u.Db.Delete([]byte(key), nil)
}

/**
返回iterator迭代器
 */
func (u *ULeveldb) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	return u.Db.NewIterator(slice, ro)
}
