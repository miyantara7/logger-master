package interfaces

import (
	"log"
	"time"
)

// Logger modules interface, using for dynamic modules
type Logger interface {
	// New - Clone logger instance
	New() Logger
	// Init - Deprecated, use InitWithConfig instead
	Init(namespace, version string)
	InitWithConfig(namespace, version string, config *LoggerConfig)
	// Close - Close logger instance, this will block until all log has been sent
	Close()
	// CloseWithTimeout - Close logger instance with timeout, this will block until all log has been sent
	CloseWithTimeout(timeout time.Duration)
	ServiceName() string
	ServiceVersion() string
	SetLogLevel(level DebugLevel)
	SetLogFile(*LoggingFile)
	GetLogLevel() (level DebugLevel)
	SetPrintToConsole(pr bool)
	GetPrintToConsole() (pr bool)
	SetOnLoggerHandler(f func(msg LoggerMessage, raw string))
	SetOutputFormat(OutputFormat)
	GetOutputFormat() OutputFormat
	ParsingLog(msg LoggerMessage) (raw string)
	SetSendToDatadog(send bool)
	SetLogNoOfChunk(cc int)
	SetLogSendInterval(cc int)
	SetDatadogExporterContentEncoding(en string)
	Write(p []byte) (int, error)
	Trace(format any, input ...any)
	Debug(format any, input ...any)
	Notice(format any, input ...any)
	Info(format any, input ...any)
	Warning(format any, input ...any)
	Success(format any, input ...any)
	Error(format any, input ...any) Logger
	NewSystemLogger() *log.Logger
	Printf(string, ...any)
	Quit()
	// Clean - Clean logger instance, send interrupt signal to this group process
	Clean() Logger
	// Kill - Kill logger instance, send interrupt signal to this process
	Kill()
	// SendDataDogTraceLog - Send datadog trace log
	// DD_API_KEY and DD_SITE must be set
	SendDataDogTraceLog(data *DatadogTraceLog)
	// RunDatadogTraceLogExporter - Run datadog trace log exporter
	RunDatadogTraceLogExporter()
}
