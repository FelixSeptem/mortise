// Package mortise implement a lock service based ob redis with fencing guarantee
package mortise

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"strconv"
	"sync"
	"time"
)

// Mutex manager to del with full lock process
type MutexManager struct {
	conn redis.Conn
	Name string // shall be genaral unique
	mu   sync.Mutex
}

// Generate a key to store resource fencing token
func (m *MutexManager) getFencingTokenKey() string {
	return fmt.Sprintf("mortise:%s:fencingToken", m.Name)
}

// Generate a key to store resource key
func (m *MutexManager) getResourceKey(key string) string {
	return fmt.Sprintf("mortise:%s", key)
}

// generate a fencing token
func (m *MutexManager) generateFencingToken() (int64, error) {
	fencingTokenKey := m.getFencingTokenKey()
	return redis.Int64(m.conn.Do("INCR", fencingTokenKey))
}

// use fencing token to acquire lock
func (m *MutexManager) Lock(key string, expiredTime time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	resourceKey := m.getResourceKey(key)
	fencingToken, err := m.generateFencingToken()
	if err != nil {
		return 0, &ErrRedis{err: err}
	}
	resp, err := lockScript.Do(m.conn, resourceKey, fencingToken, int(expiredTime/time.Millisecond))
	if err != nil {
		return 0, &ErrRedis{err: err}
	}
	if resp == "OK" {
		return fencingToken, nil
	}
	lToken, err := toInt64(resp)
	if err != nil {
		return 0, err
	}
	if err := processFencingToken(lToken, fencingToken); err != nil && lToken != 0 {
		return 0, err
	}
	return fencingToken, nil
}

// unlock:get compare and del key
func (m *MutexManager) Unlock(key string, fencingToken int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	resourceKey := m.getResourceKey(key)
	resp, err := unlockScript.Do(m.conn, resourceKey, fencingToken)
	if err != nil {
		return &ErrRedis{err: err}
	}
	if err != nil {
		return &ErrRedis{err: err}
	}
	lToken, err := toInt64(resp)
	if err != nil {
		return err
	}
	if lToken == 1 {
		return nil
	}
	if err := processFencingToken(lToken, fencingToken); err != nil && lToken != 0 {
		return err
	}
	return nil
}

// check current fencing token
func (m *MutexManager) GetCurrentFencingToken(key string) (int64, error) {
	resourceKey := m.getResourceKey(key)
	return redis.Int64(m.conn.Do("GET", resourceKey))
}

var lockScript = redis.NewScript(1, `
	local v = redis.call('GET', KEYS[1])
	if v then
  		return v
	end
	return redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
`)

var unlockScript = redis.NewScript(1, `
	local v = redis.call("GET", KEYS[1])
	if v == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	end
	return v
`)

func toInt64(v interface{}) (int64, error) {
	if i, ok := v.(int64); ok {
		return i, nil
	}
	if u, ok := v.([]uint8); ok {
		b := make([]byte, len(u))
		for i, v := range u {
			b[i] = byte(v)
		}
		t, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return 0, err
		}
		return t, nil
	}
	return 0, errors.New("input shall be int64 or []uint8")
}
