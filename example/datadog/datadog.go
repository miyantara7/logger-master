package main

import (
	"fmt"
	"time"

	"github.com/miyantara7/utils-master/system"

	"github.com/miyantara7/logger-master/interfaces"
	"github.com/miyantara7/logger-master/lib"
)

type SampleStruct struct {
	Name string
}

func main() {
	tss := time.Now()
	logger := lib.NewLib()
	logger.InitWithConfig("Testing modules", "1.0.0", &interfaces.LoggerConfig{
		Level:         "trace",
		Format:        "json",
		SendToDataDog: true,
		LogNoOfChunk:  500,
		File: &interfaces.LoggingFile{
			Enable:   true,
			MaxSize:  50,
			MaxAge:   7,
			Compress: false,
		},
	})

	go func() {
		for i := 0; i < 100; i++ {
			logger.Debug("Simple testing logger with format value '%s'", SampleStruct{Name: fmt.Sprintf("random Name with id, %d", i)})
		}
		logger.Kill()
	}()

	system.WaitAndShutdown(func() {
		logger.CloseWithTimeout(60 * time.Second)
	})
	logger.Debug("Finish in %s", time.Since(tss))
}
