//go:build windows

package lib

import (
	"os"

	"github.com/miyantara7/logger-master/interfaces"

	"github.com/shirou/gopsutil/v3/process"
)

func (c *Modules) Kill() {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		panic(err)
	}

	err = p.Kill()
	if err != nil {
		panic(err)
	}
}

func (c *Modules) Clean() interfaces.Logger {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		panic(err)
	}

	child, err := p.Children()
	if err != nil {
		panic(err)
	}

	for _, s := range child {
		err := s.Kill()
		if err != nil {
			panic(err)
		}
	}

	return c
}
