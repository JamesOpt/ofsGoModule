package main

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/util"
	db "ofsGoModule/leveldb"
	"strconv"
	"time"
)

func main()  {
	levelDbTransaction()
}

func levelDbBasic()  {
	db := db.OpenDb("leveldb/db")

	defer db.Close()

	db.Put("user-1", "b")
	db.Put("user-2", "vv")
	db.Put("user-3", "vv")
	//s, _ := db.Get("a")

	ran := util.Range{
		Start: []byte("user-1"),
		Limit: []byte("user-3"),
	}
	iter := db.NewIterator(&ran, nil)
	for iter.Next() {
		fmt.Println(string(iter.Key()), string(iter.Value()))
	}

	iter.Release()

	ran1 := util.BytesPrefix([]byte("user-"))
	iter = db.NewIterator(ran1, nil)

	for iter.Next() {
		fmt.Println(string(iter.Key()), string(iter.Value()))
	}
	iter.Release()
}

func levelDbTransaction()  {
	db := db.OpenDb("leveldb/db")
	defer db.Close()

	// 创建快照 存在内存中
	ss, err := db.Db.GetSnapshot()
	if err != nil{
		panic(err)
	}

	defer ss.Release() // 释放内存

	for i:=0;i< 2;i++ {
		go func(i int) {
			t, _ := db.Db.OpenTransaction()
			data, _ := t.Get([]byte("user-1"), nil)
			fmt.Println("go before", string(data))
			t.Put([]byte("user-1"), []byte("user-1 value go fun " + strconv.Itoa(i)), nil)
			data, _ = t.Get([]byte("user-1"), nil)
			fmt.Println("go after", string(data))
			time.Sleep(5 * time.Second)
			if i == 1 {
				t.Commit()
			} else {
				t.Discard()
			}
		}(i)
	}

	time.Sleep(10 * time.Second)
	data, _ := db.Db.Get([]byte("user-1"), nil)
	fmt.Println("main", string(data))
	//t, err := db.Db.OpenTransaction()
	//if err != nil{
	//	panic(err)
	//}
	//data, _ := ss.Get([]byte("user-1"), nil)
	//fmt.Println(string(data))

	//t.Put([]byte("user-2"), []byte("user-2 value"), nil)
	//dbData, _ := t.Get([]byte("user-2"), nil)
	//fmt.Println("在事务内读取", string(dbData))
	//t.Commit()
	//dbData, _ = db.Db.Get([]byte("user-2"), nil)
	//fmt.Println("回滚后读取", string(dbData))
}