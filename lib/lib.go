package lib

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/miyantara7/utils-master/validator"
	"go.opentelemetry.io/otel/trace"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/shirou/gopsutil/v3/process"
	"golang.org/x/sync/errgroup"

	"github.com/miyantara7/logger-master/interfaces"

	"github.com/miyantara7/utils-master/crypto/hash"
	"github.com/miyantara7/utils-master/parsing"

	"github.com/jwalton/gchalk"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Modules struct {
	namespace                      string
	version                        string
	printToConsole                 bool
	sendToDatadog                  bool
	fileConfig                     *interfaces.LoggingFile
	level                          interfaces.DebugLevel
	onLogger                       func(msg interfaces.LoggerMessage, raw string)
	outputFormat                   interfaces.OutputFormat
	logWriter                      io.Writer
	logTraceWriter                 io.Writer
	datadogApi                     *datadogV2.LogsApi
	datadogChannel                 chan int
	datadogTraceChannel            chan int
	onClosing                      bool
	pendingLogs                    []*interfaces.LoggerMessage
	pendingTracerDatadogLogs       []*interfaces.DatadogTraceLog
	datadogExporterIsRun           bool
	datadogTracerExporterIsRun     bool
	datadogTraceLogExporterIsClose bool
	datadogLogExporterIsClose      bool
	config                         *interfaces.LoggerConfig
	sync.RWMutex
}

func NewLib() interfaces.Logger {
	return &Modules{
		printToConsole: true,
		outputFormat:   interfaces.OutputFormatDefault,
		level:          interfaces.DebugLevelVerbose,
		config: &interfaces.LoggerConfig{
			Level:                          "verbose",
			Format:                         "default",
			LogSendInterval:                200,
			LogNoOfChunk:                   10,
			DatadogExporterContentEncoding: "gzip",
		},
	}
}

func (c *Modules) New() interfaces.Logger {
	return NewLib()
}

// Init - Deprecated, use InitWithConfig instead
func (c *Modules) Init(namespace, version string) {
	c.InitWithConfig(namespace, version, nil)
}

func (c *Modules) InitWithConfig(namespace, version string, config *interfaces.LoggerConfig) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	log.SetOutput(c)
	c.namespace = strings.ReplaceAll(strings.ToLower(namespace), " ", "-")
	c.version = version

	c.setConfig(config)

	// setup datadog client
	configuration := datadog.NewConfiguration()
	apiClient := datadog.NewAPIClient(configuration)
	c.datadogApi = datadogV2.NewLogsApi(apiClient)

	if c.sendToDatadog {
		go c.runDatadogLogExporter()
	}
}

func (c *Modules) Close() {
	c.CloseWithTimeout(60 * time.Second)
}

func (c *Modules) CloseWithTimeout(timeout time.Duration) {
	if c.onClosing {
		return
	}
	c.onClosing = true
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	g, newCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.closeDataDogPendingLogs(newCtx)
	})
	g.Go(func() error {
		return c.closeDataDogPendingTraceLog(newCtx)
	})
	_ = g.Wait()
}

func (c *Modules) ServiceName() string {
	return c.namespace
}

func (c *Modules) ServiceVersion() string {
	return c.version
}

func (c *Modules) SetLogFile(config *interfaces.LoggingFile) {
	if c.ServiceName() == "" {
		c.internal(interfaces.LogLevelError, "service name is empty")
		return
	}

	if config == nil {
		return
	}

	if len(config.Output) == 0 {
		c.internal(interfaces.LogLevelWarning, "file output is empty")
		config.Output = "./logs"
	}

	if config.MaxSize <= 0 {
		config.MaxSize = 100
	}

	if config.MaxAge <= 0 {
		config.MaxAge = 28
	}

	c.internal(interfaces.LogLevelTrace, "config log output %s", config.Output)
	stat, err := os.Stat(config.Output)
	if err == nil {
		if !stat.IsDir() {
			c.internal(interfaces.LogLevelError, "log output is not directory")
			return
		}
	}

	c.fileConfig = config
	if err := os.MkdirAll(filepath.Dir(c.fileConfig.Output), 0750); err != nil {
		c.Error(err)
	} else {
		if c.fileConfig.Enable {
			c.logWriter = &lumberjack.Logger{
				Filename: filepath.Join(filepath.Dir(config.Output), c.ServiceName()+".log"),
				MaxSize:  c.fileConfig.MaxSize,
				MaxAge:   c.fileConfig.MaxAge,
				Compress: c.fileConfig.Compress,
			}

			c.logTraceWriter = &lumberjack.Logger{
				Filename: filepath.Join(filepath.Dir(config.Output), c.ServiceName()+"datadog.trace.log"),
				MaxSize:  c.fileConfig.MaxSize,
				MaxAge:   c.fileConfig.MaxAge,
				Compress: c.fileConfig.Compress,
			}
		}

	}

}

func (c *Modules) setConfig(config *interfaces.LoggerConfig) interfaces.Logger {
	if config == nil {
		return c
	}

	if len(config.Level) == 0 {
		config.Level = c.config.Level
	}

	if len(config.Format) == 0 {
		config.Format = c.config.Format
	}

	if config.LogNoOfChunk == 0 {
		config.LogNoOfChunk = c.config.LogNoOfChunk
	}

	if config.LogSendInterval == 0 {
		config.LogSendInterval = c.config.LogSendInterval
	}

	if len(config.DatadogExporterContentEncoding) == 0 {
		config.DatadogExporterContentEncoding = c.config.DatadogExporterContentEncoding
	}

	err := validator.GetValidator().Struct(config)
	if err != nil {
		c.Error(err).Close()
		c.Quit()
		return nil
	}

	c.SetLogLevel(interfaces.GetDebugLevelFromString(config.Level))
	c.SetOutputFormat(interfaces.GetOutputFormatFromString(config.Format))
	c.SetSendToDatadog(config.SendToDataDog)
	if config.File != nil {
		c.SetLogFile(config.File)
	}
	c.SetLogNoOfChunk(config.LogNoOfChunk)
	c.SetLogSendInterval(config.LogSendInterval)
	c.SetDatadogExporterContentEncoding(config.DatadogExporterContentEncoding)

	if c.sendToDatadog {
		go c.runDatadogLogExporter()
	}

	return c
}

func (c *Modules) SetSendToDatadog(send bool) {
	c.sendToDatadog = send
	if c.sendToDatadog {
		go c.runDatadogLogExporter()
	}
}

func (c *Modules) SetLogNoOfChunk(cc int) {
	c.config.LogNoOfChunk = cc
	err := validator.GetValidator().Struct(c.config)
	if err != nil {
		c.Error(err).Close()
		c.Quit()
		return
	}
}

func (c *Modules) SetLogSendInterval(cc int) {
	c.config.LogSendInterval = cc
	err := validator.GetValidator().Struct(c.config)
	if err != nil {
		c.Error(err).Close()
		c.Quit()
		return
	}
}

func (c *Modules) SetDatadogExporterContentEncoding(cc string) {
	c.config.DatadogExporterContentEncoding = cc
	err := validator.GetValidator().Struct(c.config)
	if err != nil {
		c.Error(err).Close()
		c.Quit()
		return
	}
}

func (c *Modules) SetLogLevel(level interfaces.DebugLevel) {
	c.level = level
}

func (c *Modules) GetLogLevel() (level interfaces.DebugLevel) {
	return c.level
}

func (c *Modules) SetPrintToConsole(pr bool) {
	c.printToConsole = pr
}

func (c *Modules) SetOnLoggerHandler(f func(msg interfaces.LoggerMessage, raw string)) {
	c.onLogger = f
}

func (c *Modules) GetPrintToConsole() (pr bool) {
	return c.printToConsole
}

func (c *Modules) GetOutputFormat() interfaces.OutputFormat {
	return c.outputFormat
}

func (c *Modules) SetOutputFormat(op interfaces.OutputFormat) {
	c.outputFormat = op
}

func (c *Modules) internal(level interfaces.LogLevel, format any, input ...any) {
	createMsg := c.createMsg(level, interfaces.GetCaller(2), format, input)
	createMsg.Internal = true
	c.output(createMsg)
}

func (c *Modules) Trace(format any, input ...any) {
	c.output(c.createMsg(interfaces.LogLevelTrace, interfaces.GetCaller(2), format, input))
}

func (c *Modules) Debug(format any, input ...any) {
	c.output(c.createMsg(interfaces.LogLevelDebug, interfaces.GetCaller(2), format, input))
}

func (c *Modules) Notice(format any, input ...any) {
	c.output(c.createMsg(interfaces.LogLevelNotice, interfaces.GetCaller(2), format, input))
}

func (c *Modules) Info(format any, input ...any) {
	c.output(c.createMsg(interfaces.LogLevelInfo, interfaces.GetCaller(2), format, input))
}

func (c *Modules) Warning(format any, input ...any) {
	c.output(c.createMsg(interfaces.LogLevelWarning, interfaces.GetCaller(2), format, input))
}

func (c *Modules) Success(format any, input ...any) {
	c.output(c.createMsg(interfaces.LogLevelSuccess, interfaces.GetCaller(2), format, input))
}

func (c *Modules) Error(format any, input ...any) interfaces.Logger {
	c.output(c.createMsg(interfaces.LogLevelError, interfaces.GetCaller(2), format, input))
	return c
}

func (c *Modules) createMsg(level interfaces.LogLevel,
	caller interfaces.Caller,
	format any,
	input ...any) (msg interfaces.LoggerMessage) {

	var inp []any
	for _, s := range input {
		if val, ok := s.([]any); ok {
			inp = append(inp, val...)
		} else {
			inp = append(inp, s)
		}
	}

	var ffs string
	var msGs any
	if val, ok := format.(string); ok {
		ffs = val
		msGs = fmt.Sprintf(ffs, inp...)
	} else if val, ok := format.(error); ok {
		ffs = val.Error()
		msGs = fmt.Sprintf(ffs, inp...)
	} else {
		if len(inp) > 0 {
			msGs = fmt.Sprintf("%v %v", format, inp)
		} else {
			msGs = format
		}
	}

	messageStr := fmt.Sprintf("%v", msGs)

	return interfaces.LoggerMessage{
		ID:        hash.CreateRandomId(10),
		Time:      time.Now(),
		Level:     level,
		LevelName: interfaces.GetLogLevelString(level),
		File:      caller.File,
		Line:      caller.Line,
		FuncName:  caller.FName,
		Message:   messageStr,
	}
}

func (c *Modules) print(a any) {
	fmt.Println(a)
}

func (c *Modules) insertDatadogQue(msg interfaces.LoggerMessage) {
	c.Lock()
	defer c.Unlock()

	if !c.onClosing && c.sendToDatadog && !c.datadogLogExporterIsClose && !msg.Internal {
		c.pendingLogs = append(c.pendingLogs, &msg)
	}

}

func (c *Modules) createJsonMsg(msg interfaces.LoggerMessage, print bool) (res []byte) {
	jsonOut := make(map[string]any)
	jsonOut["logId"] = msg.ID
	jsonOut["level"] = strings.ToLower(msg.LevelName)
	// gunakan RFC3339Nano agar timezone + precision tersimpan
	jsonOut["time"] = msg.Time.Format(time.RFC3339Nano)

	// caller safe
	jsonOut["caller"] = fmt.Sprintf("%s:%d", msg.File, msg.Line)

	// MESSAGE: handle nil and varied kinds safely
	if msg.Message == nil {
		jsonOut["message"] = ""
	} else {
		vv := reflect.TypeOf(msg.Message)
		if vv != nil && vv.Kind() == reflect.String {
			jsonOut["message"] = msg.Message
		} else if m, ok := msg.Message.(map[string]any); ok {
			for kk, vv := range m {
				// jangan overwrite top-level reserved keys
				if kk == "logId" || kk == "level" || kk == "time" || kk == "caller" {
					continue
				}
				jsonOut[kk] = vv
			}
		} else if m, ok := msg.Message.(map[string]string); ok {
			for kk, vv := range m {
				if kk == "logId" || kk == "level" || kk == "time" || kk == "caller" {
					continue
				}
				jsonOut[kk] = vv
			}
		} else {
			// fallback: marshal arbitrary object
			if b, err := json.Marshal(msg.Message); err == nil {
				jsonOut["message"] = string(b)
			} else {
				// fallback to fmt.Sprintf
				jsonOut["message"] = fmt.Sprintf("%v", msg.Message)
			}
		}
	}

	// add trace/span metadata if present in the message
	if meta := extractMetaFromMsg(msg); len(meta) > 0 {
		for k, v := range meta {
			// don't overwrite the main keys
			if _, exists := jsonOut[k]; !exists {
				jsonOut[k] = v
			}
		}
	}

	// marshal
	if val, err := json.Marshal(jsonOut); err == nil {
		if print {
			ssd := string(val)
			if msg.Level == interfaces.LogLevel(interfaces.DebugLevelTrace) {
				if c.printToConsole {
					c.print(ssd)
				}
			} else {
				c.print(ssd)
			}
		}
		return val
	}

	return res
}

// It is defensive: if fields are absent, it returns an empty map.
func extractMetaFromMsg(msg interfaces.LoggerMessage) map[string]any {
	out := map[string]any{}

	v := reflect.ValueOf(msg)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return out
	}

	// check TraceID field
	if f := v.FieldByName("TraceID"); f.IsValid() && f.Kind() == reflect.String {
		if s := f.String(); s != "" {
			out["trace_id"] = s
		}
	}
	// check SpanID field
	if f := v.FieldByName("SpanID"); f.IsValid() && f.Kind() == reflect.String {
		if s := f.String(); s != "" {
			out["span_id"] = s
		}
	}
	// check Meta map[string]any
	if f := v.FieldByName("Meta"); f.IsValid() && !f.IsZero() {
		if mm, ok := f.Interface().(map[string]any); ok {
			for k, vv := range mm {
				out[k] = vv
			}
		}
	}
	// also check for common keys inside Message if message is map
	if mv := reflect.ValueOf(msg.Message); mv.IsValid() && mv.Kind() == reflect.Map {
		if mm, ok := msg.Message.(map[string]any); ok {
			if val, ok := mm["trace_id"]; ok {
				out["trace_id"] = val
			}
			if val, ok := mm["span_id"]; ok {
				out["span_id"] = val
			}
		}
	}

	return out
}

func attachTraceToMsg(ctx context.Context, msg *interfaces.LoggerMessage) {
	if ctx == nil || msg == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	if span == nil {
		return
	}
	sc := span.SpanContext()
	if !sc.IsValid() {
		return
	}

	// set TraceID and SpanID if fields exist, otherwise put into Meta map if present
	setFieldInMsg(msg, "TraceID", sc.TraceID().String())
	setFieldInMsg(msg, "SpanID", sc.SpanID().String())

	// Also set datadog lower-64bits decimal if convertible
	if dd := ConvertTraceIDToDatadogFormat(sc.TraceID().String()); dd != "" {
		setFieldInMsg(msg, "datadog_trace_id", dd)
	}
}

// If the field does not exist, it will attempt to put into Meta map field if available.
func setFieldInMsg(msg *interfaces.LoggerMessage, name string, value any) {
	if msg == nil {
		return
	}
	v := reflect.ValueOf(msg)
	if v.Kind() != reflect.Ptr {
		return
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return
	}

	// try direct field set
	if f := v.FieldByName(name); f.IsValid() && f.CanSet() {
		switch f.Kind() {
		case reflect.String:
			f.SetString(fmt.Sprintf("%v", value))
			return
		}
	}

	// fallback: attempt to set Meta field if it exists and is a map[string]any
	if f := v.FieldByName("Meta"); f.IsValid() && f.CanSet() {
		if f.IsNil() {
			// initialize meta map
			newMap := map[string]any{}
			f.Set(reflect.ValueOf(newMap))
		}
		if meta, ok := f.Interface().(map[string]any); ok {
			meta[name] = value
			f.Set(reflect.ValueOf(meta))
			return
		}
	}

	// final fallback: if there's no Meta field, try Message if it's map[string]any
	if mv := reflect.ValueOf(v.FieldByName("Message").Interface()); mv.IsValid() && mv.Kind() == reflect.Map {
		if mm, ok := v.FieldByName("Message").Interface().(map[string]any); ok {
			mm[name] = value
			// set back
			v.FieldByName("Message").Set(reflect.ValueOf(mm))
			return
		}
	}
}

func (c *Modules) store(msg interfaces.LoggerMessage, raw string) {
	c.insertDatadogQue(msg)

	if c.onLogger != nil {
		c.onLogger(msg, raw)
	}

	if c.outputFormat == interfaces.OutputFormatDefault {
		if c.printToConsole {
			c.print(raw)
		}
	}

	var jsonOut []byte

	if c.outputFormat == interfaces.OutputFormatJSON {
		jsonOut = c.createJsonMsg(msg, true)

	}

	if c.fileConfig != nil {
		if c.fileConfig.Enable {
			if c.logWriter != nil {
				if jsonOut == nil {
					jsonOut = c.createJsonMsg(msg, false)
				}
				if len(jsonOut) != 0 {
					ssd := string(jsonOut) + "\n"
					if _, err := c.logWriter.Write([]byte(ssd)); err != nil {
						c.internal(interfaces.LogLevelError, err)
					}
				}
			}
		}
	}

}

func (c *Modules) output(msg interfaces.LoggerMessage) {
	raw := c.ParsingLog(msg)

	switch c.level {
	case interfaces.DebugLevelTrace:
		c.store(msg, raw)
	case interfaces.DebugLevelVerbose:
		switch msg.Level {
		case interfaces.LogLevelDebug,
			interfaces.LogLevelNotice,
			interfaces.LogLevelInfo,
			interfaces.LogLevelWarning,
			interfaces.LogLevelSuccess,
			interfaces.LogLevelError:
			c.store(msg, raw)
		}
	case interfaces.DebugLevelInfo:
		switch msg.Level {
		case interfaces.LogLevelNotice,
			interfaces.LogLevelInfo,
			interfaces.LogLevelWarning,
			interfaces.LogLevelSuccess, interfaces.LogLevelError:
			c.store(msg, raw)
		}
	case interfaces.DebugLevelWarning:
		switch msg.Level {
		case interfaces.LogLevelWarning, interfaces.LogLevelError:
			c.store(msg, raw)
		}
	case interfaces.DebugLevelError:
		switch msg.Level {
		case interfaces.LogLevelError:
			c.store(msg, raw)
		}
	}

}

func (c *Modules) ParsingLog(msg interfaces.LoggerMessage) (raw string) {
	mm := gchalk.WithBold()
	mmc := gchalk.WithBold()
	var ems string
	var vms string
	mMsg := fmt.Sprintf("%s", msg.Message)

	vv := reflect.TypeOf(msg.Message)
	if vv != nil {
		switch vv.Kind() {
		case reflect.String:
		default:
			if val, err := json.Marshal(msg.Message); err == nil {
				mMsg = string(val)
			}
		}
	} else {
		if val, err := json.Marshal(msg.Message); err == nil {
			mMsg = string(val)
		}
	}

	switch msg.Level {
	case interfaces.LogLevelTrace:
		ems = mm.BrightWhite(interfaces.GetLogLevelPrintString(msg.Level))
		vms = mmc.White(mMsg)
	case interfaces.LogLevelDebug:
		ems = mm.BrightBlue(interfaces.GetLogLevelPrintString(msg.Level))
		vms = mmc.White(mMsg)
	case interfaces.LogLevelNotice:
		ems = mm.BrightCyan(interfaces.GetLogLevelPrintString(msg.Level))
		vms = mmc.BrightCyan(mMsg)
	case interfaces.LogLevelInfo:
		ems = mm.BrightMagenta(interfaces.GetLogLevelPrintString(msg.Level))
		vms = mmc.BrightMagenta(mMsg)
	case interfaces.LogLevelWarning:
		ems = mm.Yellow(interfaces.GetLogLevelPrintString(msg.Level))
		vms = mmc.Yellow(mMsg)
	case interfaces.LogLevelError:
		ems = mm.Red(interfaces.GetLogLevelPrintString(msg.Level))
		vms = mmc.Red(mMsg)
	case interfaces.LogLevelSuccess:
		ems = mm.Green(interfaces.GetLogLevelPrintString(msg.Level))
		vms = mmc.Green(mMsg)

	}

	raw = fmt.Sprintf("[%s][%s][%s][%s][%d] %s",
		gchalk.Magenta(parsing.NewTime().TimeStringTimeOnly(msg.Time)),
		ems, gchalk.BrightWhite(c.namespace),
		gchalk.BrightCyan(filepath.Base(msg.FuncName)),
		msg.Line,
		vms)
	return raw
}

func (c *Modules) Quit() {
	os.Exit(0)
}

func (c *Modules) Write(p []byte) (int, error) {
	scanner := bufio.NewScanner(bytes.NewBuffer(p))
	for scanner.Scan() {
		text := scanner.Text()
		if len(text) != 0 {
			c.Debug("%s", text)
		}
	}
	return len(p), nil
}

func (c *Modules) NewSystemLogger() *log.Logger {
	logs := log.New(c, "", log.LstdFlags)
	logs.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	return logs
}

func (c *Modules) Printf(f string, data ...any) {
	c.Debug(f, data...)
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

func (c *Modules) Kill() {}

func ConvertTraceIDToDatadogFormat(id string) string {
	if len(id) == 0 {
		return ""
	}
	// normalize: remove optional 0x
	if strings.HasPrefix(id, "0x") || strings.HasPrefix(id, "0X") {
		id = id[2:]
	}
	// target last 16 hex chars (lower 64 bits)
	if len(id) > 16 {
		id = id[len(id)-16:]
	} else if len(id) < 16 {
		id = strings.Repeat("0", 16-len(id)) + id
	}
	intValue, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		return ""
	}
	return strconv.FormatUint(intValue, 10)
}
