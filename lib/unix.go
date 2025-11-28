//go:build linux

package lib

import (
	"os"
	"syscall"

	"github.com/miyantara7/logger-master/interfaces"
)

func (c *Modules) Kill() {
	_ = syscall.Kill(os.Getpid(), syscall.SIGINT)
}

func (c *Modules) Clean() interfaces.Logger {
	pGid, err := syscall.Getpgid(os.Getpid())
	if err == nil {
		_ = syscall.Kill(-pGid, syscall.SIGINT)
	}
	return c
}
