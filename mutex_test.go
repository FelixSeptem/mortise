package mortise

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"testing"
	"time"
)

func TestMutexManager_Lock(t *testing.T) {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Fatal(err)
	}
	mutexMgr := MutexManager{
		Conn: conn,
		Name: "test",
	}
	key := fmt.Sprintf("test-%d", time.Now().UnixNano()/1000)
	_, err = mutexMgr.Lock(key, time.Millisecond*300)
	if err != nil {
		t.Fatal(err)
	}
	_, err = mutexMgr.Lock(key, time.Millisecond*300)
	if _, ok := err.(*ErrMutexOccupied); !ok {
		t.Fatal(err)
	}
}

func TestMutexManager_Unlock(t *testing.T) {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Fatal(err)
	}
	mutexMgr := MutexManager{
		Conn: conn,
		Name: "test",
	}
	key := fmt.Sprintf("test-%d", time.Now().UnixNano()/1000)

	ft1, err := mutexMgr.Lock(key, time.Millisecond*10)
	if err != nil {
		t.Fatal(err)
	}
	if err := mutexMgr.Unlock(key, ft1); err != nil {
		t.Fatal(err)
	}

	ft2, err := mutexMgr.Lock(key, time.Millisecond*30)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	_, err = mutexMgr.Lock(key, time.Millisecond*300)
	if err != nil {
		t.Fatal(key)
		t.Fatal(err)
	}
	err = mutexMgr.Unlock(key, ft2)
	if _, ok := err.(*ErrOutdatedToken); !ok {
		t.Fatal(err)
	}
}

func TestMutexManager_SetRetries(t *testing.T) {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Fatal(err)
	}
	mutexMgr := MutexManager{
		Conn: conn,
		Name: "test",
	}
	key := fmt.Sprintf("test-%d", time.Now().UnixNano()/1000)
	mutexMgr.SetRetries(3, time.Millisecond*50)
	_, err = mutexMgr.Lock(key, time.Millisecond*300)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMutexManager_GetCurrentFencingToken(t *testing.T) {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Fatal(err)
	}
	mutexMgr := MutexManager{
		Conn: conn,
		Name: "test",
	}
	key := fmt.Sprintf("test-%d", time.Now().UnixNano()/1000)
	ft, err := mutexMgr.Lock(key, time.Millisecond*300)
	if err != nil {
		t.Fatal(err)
	}
	gotft, err := mutexMgr.GetCurrentFencingToken(key)
	if err != nil {
		t.Log(key)
		t.Fatal(err)
	}
	if gotft != ft {
		t.Errorf("expect %d got %d", ft, gotft)
	}
}

func BenchmarkMutexManager_Lock(b *testing.B) {
	b.StopTimer()
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		b.Fatal(err)
	}
	mutexMgr := MutexManager{
		Conn: conn,
		Name: "test",
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench:%d", time.Now().UnixNano()/1000)
		if _, err := mutexMgr.Lock(key, 10*time.Millisecond); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMutexManager_Unlock(b *testing.B) {
	b.StopTimer()
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		b.Fatal(err)
	}
	mutexMgr := MutexManager{
		Conn: conn,
		Name: "test",
	}
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("bench:%d", i)
		if _, err := mutexMgr.Lock(key, time.Second*3); err != nil {
			b.Log(err)
		}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench:%d", i)
		if err := mutexMgr.Unlock(key, int64(i)); err != nil {
			b.Log(err)
		}
	}
}

func BenchmarkMutexManager_GetCurrentFencingToken(b *testing.B) {
	b.StopTimer()
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		b.Fatal(err)
	}
	mutexMgr := MutexManager{
		Conn: conn,
		Name: "test",
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if _, err := mutexMgr.GetCurrentFencingToken("bench"); err != nil {
			b.Log(err)
		}
	}
}
