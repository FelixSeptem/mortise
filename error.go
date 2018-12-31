package mortise

import (
	"fmt"
)

// fencing token outdated
type ErrOutdatedToken struct {
	currentToken int64
	holdToken    int64
}

func (e *ErrOutdatedToken) Error() string {
	return fmt.Sprintf("key:%d outdated, current key is %d", e.holdToken, e.currentToken)
}

// mutex has been acquired and still not release
type ErrMutexOccupied struct {
	currentToken int64
	holdToken    int64
}

func (e *ErrMutexOccupied) Error() string {
	return fmt.Sprintf("key:%d has been occupied by %d", e.holdToken, e.currentToken)
}

// redis internal error
type ErrRedis struct {
	err error
}

func (e *ErrRedis) Error() string {
	return fmt.Sprintf("redis exception:%+v", e.err)
}

// compare fencing token and return proper error
func processFencingToken(lockedToken int64, haveToken int64) error {
	if lockedToken > haveToken {
		return &ErrOutdatedToken{currentToken: lockedToken, holdToken: haveToken}
	} else if lockedToken < haveToken {
		return &ErrMutexOccupied{currentToken: lockedToken, holdToken: haveToken}
	}
	return nil
}
