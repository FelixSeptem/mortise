package main

import (
	"fmt"
	"github.com/FelixSeptem/mortise"
	"github.com/gomodule/redigo/redis"
	"time"
)

func accessResource(key string, ftoken int64, mutex *mortise.MutexManager) error {
	if _, err := mutex.CheckCurrentFencingToken(key, ftoken); err != nil {
		return err
	}
	// mock resource operate
	func() {
		time.Sleep(time.Millisecond * 100)
	}()
	return nil
}

func job(mutex *mortise.MutexManager, resourceKey string) error {
	token, err := mutex.Lock(resourceKey, time.Millisecond*300)
	if err != nil {
		return err
	}
	if err := accessResource(resourceKey, token, mutex); err != nil {
		return err
	}
	return nil
}

func main() {
	conn, err := redis.Dial("tcp", "localhost:6379", redis.DialConnectTimeout(time.Millisecond*100), redis.DialDatabase(0))
	if err != nil {
		fmt.Println(err.Error())
	}
	mutexMgr := mortise.MutexManager{
		Conn: conn,
		Name: "SyncJob",
	}
	resourceKey := fmt.Sprintf("Sync:%d", time.Now().UnixNano())
	for i := 0; i < 100; i++ {
		if err:=job(&mutexMgr, resourceKey);err!=nil{
			fmt.Println(err)
		}
	}
}
