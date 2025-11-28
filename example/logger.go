package main

import (
	"github.com/miyantara7/logger-master/interfaces"
	"github.com/miyantara7/logger-master/lib"
)

type SampleStruct struct {
	Name string
}

func main() {
	logger := lib.NewLib()
	logger.InitWithConfig("Testing modules", "1.0.0", nil)
	logger.SetLogLevel(interfaces.DebugLevelTrace)
	loggerOutput(logger)
	logger.SetOutputFormat(interfaces.OutputFormatJSON)
	loggerOutput(logger)

}

func loggerOutput(logger interfaces.Logger) {
	logger.Debug("Simple testing logger with fmt.Sprintf format value '%s'", "string")
	logger.Debug(SampleStruct{Name: "Auto parsing message struct to json"})
	logger.Debug(map[string]any{
		"name": "Also working with map struct",
	})
}
