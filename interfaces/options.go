package interfaces

import (
	"encoding/json"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type LogLevel int

const (
	LogLevelTrace LogLevel = iota + 1
	LogLevelDebug
	LogLevelNotice
	LogLevelInfo
	LogLevelWarning
	LogLevelError
	LogLevelSuccess
	LogLevelFatal
)

type DebugLevel int

const (
	DebugLevelTrace DebugLevel = iota + 1
	DebugLevelVerbose
	DebugLevelInfo
	DebugLevelWarning
	DebugLevelError
)

func GetDebugLevelFromString(level string) DebugLevel {
	switch strings.ToLower(level) {
	case "trace":
		return DebugLevelTrace
	case "verbose":
		return DebugLevelVerbose
	case "info":
		return DebugLevelInfo
	case "warning":
		return DebugLevelWarning
	case "error":
		return DebugLevelError

	}

	return -1
}

var levelString = map[LogLevel]string{
	LogLevelTrace:   "TRACE",
	LogLevelDebug:   "DEBUG",
	LogLevelNotice:  "NOTICE",
	LogLevelInfo:    "INFO",
	LogLevelWarning: "WARNING",
	LogLevelError:   "ERROR",
	LogLevelSuccess: "SUCCESS",
	LogLevelFatal:   "FATAL",
}

func GetLogLevelString(level LogLevel) string {
	return levelString[level]
}

var levelPrintString = map[LogLevel]string{
	LogLevelTrace:   "TRCE",
	LogLevelDebug:   "DBUG",
	LogLevelNotice:  "NTCE",
	LogLevelInfo:    "INFO",
	LogLevelWarning: "WARN",
	LogLevelError:   "EROR",
	LogLevelSuccess: "SUCS",
	LogLevelFatal:   "FATL",
}

func GetLogLevelPrintString(level LogLevel) string {
	return levelPrintString[level]
}

type LoggerMessage struct {
	ID        string    `json:"id"`
	Level     LogLevel  `json:"level"`
	LevelName string    `json:"levelName"`
	File      string    `json:"file"`
	Line      int       `json:"line"`
	FuncName  string    `json:"funcName"`
	Time      time.Time `json:"time"`
	Message   any       `json:"message"`
	Internal  bool      `json:"internal"`
}

type OutputFormat int

const (
	OutputFormatDefault OutputFormat = iota + 1
	OutputFormatJSON
)

func GetOutputFormatFromString(op string) OutputFormat {
	switch strings.ToLower(op) {
	case "default":
		return OutputFormatDefault
	case "json":
		return OutputFormatJSON
	}
	return OutputFormatDefault
}

type Caller struct {
	File       string `json:"file"`
	Line       int    `json:"line"`
	FName      string `json:"fName"`
	FNameShort string `json:"-"`
}

func (c Caller) String() string {
	bs, _ := json.Marshal(c)
	return string(bs)
}

func GetCaller(skip int) (cs Caller) {
	if skip == 0 {
		skip = 1
	}

	pc, file, line, ok := runtime.Caller(skip)
	if ok {
		cs.File = file
		cs.Line = line
		fc := runtime.FuncForPC(pc)
		cs.FName = fc.Name()
		drName := filepath.Dir(fc.Name())
		sPsd := strings.Split(drName, "/")
		if len(sPsd) >= 2 {
			sPsd = sPsd[len(sPsd)-2:]
		}
		sPsd = append(sPsd, filepath.Base(fc.Name()))
		cs.FNameShort = filepath.Join(sPsd...)
	}

	return cs

}

type DatadogTraceLog struct {
	ServiceName string
	TraceID     string
	SpanID      string
	Msg         any
	Key         string
	Level       LogLevel
	Env         string
	Caller      string
	Operation   string
}
