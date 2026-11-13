//go:build darwin || linux

package persistencetests

import (
	"errors"
	"os"
	"syscall"
)

// lockDSQLSchemaFile takes an exclusive lock on path without waiting. It reports false
// when another open file holds the lock, in this process or another one.
func lockDSQLSchemaFile(path string) (unlock func(), ok bool, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, false, nil
		}
		return nil, false, err
	}
	// Closing the file releases the lock.
	return func() { _ = f.Close() }, true, nil
}
