package lib

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
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

// ------------- package global logger -------------
var globalLogger interfaces.Logger

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

// SetGlobalLogger sets the global logger instance used by log helpers
func SetGlobalLogger(l interfaces.Logger) {
	globalLogger = l
}

// GetGlobalLogger returns the current global logger
func GetGlobalLogger() interfaces.Logger {
	return globalLogger
}

// UseAsGlobal makes the receiver Modules instance the package-level global logger.
func (c *Modules) UseAsGlobal() {
	globalLogger = c
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

	var msGs any
	switch v := format.(type) {
	case string:
		if len(inp) > 0 {
			msGs = fmt.Sprintf(v, inp...)
		} else {
			msGs = v
		}
	case error:
		if len(inp) > 0 {
			msGs = fmt.Sprintf(v.Error(), inp...)
		} else {
			msGs = v.Error()
		}
	case map[string]any:
		msGs = v
	case map[string]string:
		m := map[string]any{}
		for k, vv := range v {
			m[k] = vv
		}
		msGs = m
	default:
		if len(inp) > 0 {
			parts := []any{format}
			parts = append(parts, inp...)
			msGs = fmt.Sprint(parts...)
		} else {
			msGs = format
		}
	}

	return interfaces.LoggerMessage{
		ID:        hash.CreateRandomId(10),
		Time:      time.Now(),
		Level:     level,
		LevelName: interfaces.GetLogLevelString(level),
		File:      caller.File,
		Line:      caller.Line,
		FuncName:  caller.FName,
		Message:   msGs,
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

// --- LogType enum + helper -------------------------------------------------
type LogType string

const (
	LogTypeLOG              LogType = "LOG"
	LogTypeAPM              LogType = "APM"
	LogTypeAuditTrail       LogType = "AUDIT_TRAIL"
	LogTypeAccessLog        LogType = "ACCESS_LOG"
	LogTypeServiceToService LogType = "SERVICE_TO_SERVICE"
)

// NormalizeLogType returns canonical upper-case LogType string (fallback to LOG)
func NormalizeLogType(v any) string {
	if v == nil {
		return string(LogTypeLOG)
	}
	var s string
	switch t := v.(type) {
	case string:
		s = strings.TrimSpace(t)
	default:
		s = fmt.Sprintf("%v", t)
	}
	if s == "" {
		return string(LogTypeLOG)
	}
	us := strings.ToUpper(s)
	switch us {
	case string(LogTypeLOG),
		string(LogTypeAPM),
		string(LogTypeAuditTrail),
		string(LogTypeAccessLog),
		string(LogTypeServiceToService):
		return us
	}
	// support some common aliases
	switch us {
	case "AUDIT":
		return string(LogTypeAuditTrail)
	case "ACCESS":
		return string(LogTypeAccessLog)
	case "SERVICE_TO_SERVICE", "SERVICE-TO-SERVICE", "SVC2SVC", "SVC_TO_SVC":
		return string(LogTypeServiceToService)
	}
	return string(LogTypeLOG)
}

func (c *Modules) createJsonMsg(msg interfaces.LoggerMessage, print bool) (res []byte) {
	// build metadata starting from fields extracted
	metadata := map[string]any{}
	if metaFromMsg := extractMetaFromMsg(msg); len(metaFromMsg) > 0 {
		for k, v := range metaFromMsg {
			metadata[k] = v
		}
	}

	var detectedLogType string
	var topMessage any

	// parse msg.Message (map with "message" + "metadata" is expected)
	if msg.Message == nil {
		topMessage = ""
	} else if m, ok := msg.Message.(map[string]any); ok {
		if v, ok := m["message"]; ok {
			topMessage = v
		} else {
			topMessage = fmt.Sprintf("%v", m)
		}

		// merge message.metadata => metadata (but skip inner log_type)
		if mm, ok := m["metadata"].(map[string]any); ok {
			for kk, vv := range mm {
				if kk == "log_type" {
					if s, ok := vv.(string); ok && detectedLogType == "" {
						detectedLogType = s
					}
					continue
				}
				metadata[kk] = vv
			}
		}

		// capture top-level keys in message map (http.*, latency_ms, etc.) into metadata
		for kk, vv := range m {
			if kk == "message" || kk == "metadata" {
				continue
			}
			if kk == "log_type" {
				if s, ok := vv.(string); ok && detectedLogType == "" {
					detectedLogType = s
				}
				continue
			}
			if strings.HasPrefix(kk, "http.") || kk == "latency_ms" {
				metadata[kk] = vv
			}
		}
	} else {
		topMessage = fmt.Sprintf("%v", msg.Message)
	}

	// if metadata contains log_type, use it and remove it from metadata to avoid duplication
	if lt, ok := metadata["log_type"]; ok {
		if s, ok := lt.(string); ok && detectedLogType == "" {
			detectedLogType = s
		}
		delete(metadata, "log_type")
	}

	// decide final log type
	finalLogType := NormalizeLogType(detectedLogType)

	// ----------------- special shape: LOG -----------------
	if finalLogType == string(LogTypeLOG) {
		jsonRoot := map[string]any{}
		jsonRoot["timestamp"] = msg.Time.Format(time.RFC3339)
		jsonRoot["log_type"] = finalLogType
		jsonRoot["level"] = strings.ToLower(msg.LevelName)

		// attach trace_id from metadata if exists
		if t, ok := metadata["trace_id"]; ok {
			jsonRoot["trace_id"] = fmt.Sprintf("%v", t)
			delete(metadata, "trace_id")
		}

		jsonRoot["msg"] = fmt.Sprintf("%v", topMessage)

		source := map[string]any{
			"function": msg.FuncName,
			"file":     msg.File,
			"line":     msg.Line,
		}
		jsonRoot["source"] = source

		if metadata == nil {
			metadata = map[string]any{}
		}
		jsonRoot["metadata"] = metadata

		if val, err := json.Marshal(jsonRoot); err == nil {
			if print {
				c.print(string(val))
			}
			return val
		}
		return nil
	}

	// ----------------- special shape: APM -----------------
	if finalLogType == string(LogTypeAPM) {
		apmRoot := map[string]any{}
		apmRoot["timestamp"] = msg.Time.Format(time.RFC3339)
		apmRoot["log_type"] = finalLogType

		data := map[string]any{}

		if v, ok := metadata["host_name"]; ok {
			data["host_name"] = v
			delete(metadata, "host_name")
		} else if v, ok := metadata["hostname"]; ok {
			data["host_name"] = v
			delete(metadata, "hostname")
		}
		if v, ok := metadata["ip"]; ok {
			data["ip"] = v
			delete(metadata, "ip")
		}
		if v, ok := metadata["action"]; ok {
			data["action"] = v
			delete(metadata, "action")
		}
		if v, ok := metadata["action_type"]; ok {
			data["action_type"] = v
			delete(metadata, "action_type")
		}
		if v, ok := metadata["duration"]; ok {
			data["duration"] = v
			delete(metadata, "duration")
		} else if v, ok := metadata["latency_ms"]; ok {
			data["duration"] = v
			delete(metadata, "latency_ms")
		}
		if v, ok := metadata["channel"]; ok {
			data["channel"] = v
			delete(metadata, "channel")
		}
		if v, ok := metadata["product"]; ok {
			data["product"] = v
			delete(metadata, "product")
		}
		if v, ok := metadata["datacenter"]; ok {
			data["datacenter"] = v
			delete(metadata, "datacenter")
		}
		if v, ok := metadata["status"]; ok {
			data["status"] = v
			delete(metadata, "status")
		}
		apmMeta := map[string]any{}
		for kk, vv := range metadata {
			if strings.HasPrefix(kk, "http.") || kk == "trace_id" || kk == "span_id" {
				continue
			}
			apmMeta[kk] = vv
		}
		data["metadata"] = apmMeta
		apmRoot["data"] = data

		if val, err := json.Marshal(apmRoot); err == nil {
			if print {
				c.print(string(val))
			}
			return val
		}
		return nil
	}

	// ----------------- special shape: AUDIT_TRAIL -----------------
	if finalLogType == string(LogTypeAuditTrail) {
		auditRoot := map[string]any{}
		// timestamp in RFC3339 (preserve timezone)
		auditRoot["timestamp"] = msg.Time.Format(time.RFC3339)
		auditRoot["log_type"] = finalLogType
		auditRoot["level"] = strings.ToLower(msg.LevelName)

		// trace_id (take from metadata if available)
		if t, ok := metadata["trace_id"]; ok {
			auditRoot["trace_id"] = fmt.Sprintf("%v", t)
			delete(metadata, "trace_id")
		} else {
			// if extractMetaFromMsg provided trace info under other keys, keep empty string if absent
			auditRoot["trace_id"] = ""
		}

		// msg / action: prefer topMessage, fallback to metadata keys
		if topMessage != "" {
			auditRoot["msg"] = fmt.Sprintf("%v", topMessage)
		} else if v, ok := metadata["action"]; ok {
			auditRoot["msg"] = fmt.Sprintf("%v", v)
			delete(metadata, "action")
		} else {
			auditRoot["msg"] = ""
		}

		// actor: prefer a structured actor in metadata, otherwise try common keys
		if act, ok := metadata["actor"]; ok {
			if am, ok2 := act.(map[string]any); ok2 {
				auditRoot["actor"] = am
			} else {
				auditRoot["actor"] = map[string]any{"user": fmt.Sprintf("%v", act)}
			}
			delete(metadata, "actor")
		} else {
			actorMap := map[string]any{}
			if v, ok := metadata["actor.user"]; ok {
				actorMap["user"] = v
				delete(metadata, "actor.user")
			} else if v, ok := metadata["actor_user"]; ok {
				actorMap["user"] = v
				delete(metadata, "actor_user")
			} else if v, ok := metadata["user"]; ok {
				actorMap["user"] = v
				delete(metadata, "user")
			}
			auditRoot["actor"] = actorMap
		}

		// status (if present)
		if v, ok := metadata["status"]; ok {
			auditRoot["status"] = v
			delete(metadata, "status")
		} else {
			auditRoot["status"] = ""
		}

		// device_info: prefer nested map or separate keys
		deviceInfo := map[string]any{}
		if di, ok := metadata["device_info"]; ok {
			if dim, ok2 := di.(map[string]any); ok2 {
				// copy known keys
				if v, ok := dim["user_agent"]; ok {
					deviceInfo["user_agent"] = v
				} else if v, ok := dim["userAgent"]; ok {
					deviceInfo["user_agent"] = v
				}
				if v, ok := dim["ip_address"]; ok {
					deviceInfo["ip_address"] = v
				} else if v, ok := dim["ip"]; ok {
					deviceInfo["ip_address"] = v
				}
				if v, ok := dim["app_version"]; ok {
					deviceInfo["app_version"] = v
				}
			}
			delete(metadata, "device_info")
		} else {
			if v, ok := metadata["user_agent"]; ok {
				deviceInfo["user_agent"] = v
				delete(metadata, "user_agent")
			}
			if v, ok := metadata["ip_address"]; ok {
				deviceInfo["ip_address"] = v
				delete(metadata, "ip_address")
			} else if v, ok := metadata["ip"]; ok {
				deviceInfo["ip_address"] = v
				delete(metadata, "ip")
			}
			if v, ok := metadata["app_version"]; ok {
				deviceInfo["app_version"] = v
				delete(metadata, "app_version")
			}
		}

		auditRoot["device_info"] = deviceInfo
		if len(metadata) > 0 {
			delete(metadata, "log_type")
			auditRoot["metadata"] = metadata
		}

		if val, err := json.Marshal(auditRoot); err == nil {
			if print {
				c.print(string(val))
			}
			return val
		}
		return nil
	}

	// ----------------- special shape: ACCESS_LOG -----------------
	if finalLogType == "ACCESS_LOG" {
		accessRoot := map[string]any{}
		accessRoot["timestamp"] = msg.Time.Format(time.RFC3339)
		accessRoot["log_type"] = "ACCESS_LOG"
		accessRoot["level"] = strings.ToLower(msg.LevelName)

		// trace id
		if t, ok := metadata["trace_id"]; ok {
			accessRoot["trace_id"] = fmt.Sprintf("%v", t)
			delete(metadata, "trace_id")
		} else {
			accessRoot["trace_id"] = ""
		}

		// message (error message or summary)
		if topMessage != "" {
			accessRoot["msg"] = fmt.Sprintf("%v", topMessage)
		} else if v, ok := metadata["message"]; ok {
			accessRoot["msg"] = fmt.Sprintf("%v", v)
			delete(metadata, "message")
		} else if v, ok := metadata["msg"]; ok {
			accessRoot["msg"] = fmt.Sprintf("%v", v)
			delete(metadata, "msg")
		} else {
			accessRoot["msg"] = ""
		}

		// method/path/host/status
		if v, ok := metadata["http.method"]; ok {
			accessRoot["method"] = fmt.Sprintf("%v", v)
			delete(metadata, "http.method")
		} else if v, ok := metadata["method"]; ok {
			accessRoot["method"] = fmt.Sprintf("%v", v)
			delete(metadata, "method")
		} else {
			accessRoot["method"] = ""
		}

		if v, ok := metadata["http.path"]; ok {
			accessRoot["path"] = fmt.Sprintf("%v", v)
			delete(metadata, "http.path")
		} else if v, ok := metadata["path"]; ok {
			accessRoot["path"] = fmt.Sprintf("%v", v)
			delete(metadata, "path")
		} else {
			accessRoot["path"] = ""
		}

		if v, ok := metadata["http.host"]; ok {
			accessRoot["host"] = fmt.Sprintf("%v", v)
			delete(metadata, "http.host")
		} else if v, ok := metadata["host"]; ok {
			accessRoot["host"] = fmt.Sprintf("%v", v)
			delete(metadata, "host")
		} else {
			accessRoot["host"] = ""
		}

		// status
		if v, ok := metadata["http.status"]; ok {
			accessRoot["status"] = v
			delete(metadata, "http.status")
		} else if v, ok := metadata["status"]; ok {
			accessRoot["status"] = v
			delete(metadata, "status")
		} else {
			accessRoot["status"] = 0
		}

		// bytes_in / bytes_out
		if v, ok := metadata["bytes_in"]; ok {
			accessRoot["bytes_in"] = v
			delete(metadata, "bytes_in")
		} else {
			accessRoot["bytes_in"] = 0
		}
		if v, ok := metadata["bytes_out"]; ok {
			accessRoot["bytes_out"] = v
			delete(metadata, "bytes_out")
		} else {
			accessRoot["bytes_out"] = 0
		}

		// latency: prefer latency_ns, else latency_ms converted to ns
		if v, ok := metadata["latency_ns"]; ok {
			accessRoot["latency"] = v
			delete(metadata, "latency_ns")
		} else if v, ok := metadata["latency_ms"]; ok {
			// convert ms -> ns
			switch t := v.(type) {
			case int:
				accessRoot["latency"] = int64(t) * 1e6
			case int64:
				accessRoot["latency"] = t * 1e6
			case float64:
				accessRoot["latency"] = int64(t) * 1e6
			case string:
				if n, err := strconv.ParseInt(t, 10, 64); err == nil {
					accessRoot["latency"] = n * 1e6
				} else {
					accessRoot["latency"] = 0
				}
			default:
				accessRoot["latency"] = 0
			}
			delete(metadata, "latency_ms")
		} else {
			accessRoot["latency"] = 0
		}

		// protocol, referer, remote_ip, user_agent
		if v, ok := metadata["protocol"]; ok {
			accessRoot["protocol"] = fmt.Sprintf("%v", v)
			delete(metadata, "protocol")
		} else {
			accessRoot["protocol"] = ""
		}
		if v, ok := metadata["referer"]; ok {
			accessRoot["referer"] = fmt.Sprintf("%v", v)
			delete(metadata, "referer")
		} else if v, ok := metadata["referrer"]; ok {
			accessRoot["referer"] = fmt.Sprintf("%v", v)
			delete(metadata, "referrer")
		} else {
			accessRoot["referer"] = ""
		}
		// remote ip
		if v, ok := metadata["remote_ip"]; ok {
			accessRoot["remote_ip"] = fmt.Sprintf("%v", v)
			delete(metadata, "remote_ip")
		} else if v, ok := metadata["x-forwarded-for"]; ok {
			accessRoot["remote_ip"] = fmt.Sprintf("%v", v)
			delete(metadata, "x-forwarded-for")
		} else {
			accessRoot["remote_ip"] = ""
		}
		// user agent
		if v, ok := metadata["user_agent"]; ok {
			accessRoot["user_agent"] = fmt.Sprintf("%v", v)
			delete(metadata, "user_agent")
		} else if v, ok := metadata["http.useragent"]; ok {
			accessRoot["user_agent"] = fmt.Sprintf("%v", v)
			delete(metadata, "http.useragent")
		} else {
			accessRoot["user_agent"] = ""
		}

		delete(metadata, "log_type")
		accessRoot["metadata"] = metadata

		if val, err := json.Marshal(accessRoot); err == nil {
			if print {
				c.print(string(val))
			}
			return val
		}
		// fallback
		return nil
	}

	// ----------------- special shape: SERVICE_TO_SERVICE -----------------
	if finalLogType == "SERVICE_TO_SERVICE" {
		serviceRoot := map[string]any{}
		serviceRoot["timestamp"] = msg.Time.Format(time.RFC3339)
		serviceRoot["log_type"] = "SERVICE_TO_SERVICE"
		serviceRoot["level"] = strings.ToLower(msg.LevelName)

		// trace id
		if t, ok := metadata["trace_id"]; ok {
			serviceRoot["trace_id"] = fmt.Sprintf("%v", t)
			delete(metadata, "trace_id")
		} else {
			serviceRoot["trace_id"] = ""
		}

		// host / path / method
		if v, ok := metadata["http.host"]; ok {
			serviceRoot["host"] = fmt.Sprintf("%v", v)
			delete(metadata, "http.host")
		} else if v, ok := metadata["host"]; ok {
			serviceRoot["host"] = fmt.Sprintf("%v", v)
			delete(metadata, "host")
		} else {
			serviceRoot["host"] = ""
		}

		if v, ok := metadata["http.path"]; ok {
			serviceRoot["path"] = fmt.Sprintf("%v", v)
			delete(metadata, "http.path")
		} else if v, ok := metadata["path"]; ok {
			serviceRoot["path"] = fmt.Sprintf("%v", v)
			delete(metadata, "path")
		} else {
			serviceRoot["path"] = ""
		}

		if v, ok := metadata["http.method"]; ok {
			serviceRoot["method"] = fmt.Sprintf("%v", v)
			delete(metadata, "http.method")
		} else if v, ok := metadata["method"]; ok {
			serviceRoot["method"] = fmt.Sprintf("%v", v)
			delete(metadata, "method")
		} else {
			serviceRoot["method"] = ""
		}

		// request object
		reqObj := map[string]any{"body": map[string]any{}, "header": map[string]any{}}
		if v, ok := metadata["request.body"]; ok {
			reqObj["body"] = v
			delete(metadata, "request.body")
		} else if v, ok := metadata["request_body"]; ok {
			reqObj["body"] = v
			delete(metadata, "request_body")
		} else if v, ok := metadata["request"]; ok {
			// maybe nested map[string]any
			if m, ok2 := v.(map[string]any); ok2 {
				if b, ok3 := m["body"]; ok3 {
					reqObj["body"] = b
				}
				if h, ok3 := m["header"]; ok3 {
					reqObj["header"] = h
				}
			}
			delete(metadata, "request")
		}
		// headers (fallback)
		if v, ok := metadata["request.header"]; ok {
			reqObj["header"] = v
			delete(metadata, "request.header")
		} else if v, ok := metadata["request_header"]; ok {
			reqObj["header"] = v
			delete(metadata, "request_header")
		}

		// response object
		resObj := map[string]any{"body": map[string]any{}, "header": map[string]any{}}
		if v, ok := metadata["response.body"]; ok {
			resObj["body"] = v
			delete(metadata, "response.body")
		} else if v, ok := metadata["response_body"]; ok {
			resObj["body"] = v
			delete(metadata, "response_body")
		} else if v, ok := metadata["response"]; ok {
			if m, ok2 := v.(map[string]any); ok2 {
				if b, ok3 := m["body"]; ok3 {
					resObj["body"] = b
				}
				if h, ok3 := m["header"]; ok3 {
					resObj["header"] = h
				}
			}
			delete(metadata, "response")
		}
		// response headers fallback
		if v, ok := metadata["response.header"]; ok {
			resObj["header"] = v
			delete(metadata, "response.header")
		} else if v, ok := metadata["response_header"]; ok {
			resObj["header"] = v
			delete(metadata, "response_header")
		}

		serviceRoot["request"] = reqObj
		serviceRoot["response"] = resObj

		delete(metadata, "log_type")
		serviceRoot["metadata"] = metadata

		if val, err := json.Marshal(serviceRoot); err == nil {
			if print {
				c.print(string(val))
			}
			return val
		}
		// fallback
		return nil
	}

	// ----------------- other types (ACCESS_LOG, SERVICE_TO_SERVICE etc.) -----------------
	out := make(map[string]any)
	out["logId"] = msg.ID
	out["level"] = strings.ToLower(msg.LevelName)
	out["time"] = msg.Time.Format(time.RFC3339Nano)
	out["caller"] = fmt.Sprintf("%s:%d", msg.File, msg.Line)
	out["message"] = topMessage

	// attach trace fields if exist in metadata
	if t, ok := metadata["trace_id"]; ok {
		out["trace_id"] = t
		delete(metadata, "trace_id")
	}
	if s, ok := metadata["span_id"]; ok {
		out["span_id"] = s
		delete(metadata, "span_id")
	}

	delete(metadata, "log_type")
	out["metadata"] = metadata
	out["log_type"] = finalLogType

	if val, err := json.Marshal(out); err == nil {
		if print {
			c.print(string(val))
		}
		return val
	}
	return nil
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
			// also copy log_type if present inside message.metadata or top-level
			if val, ok := mm["log_type"]; ok {
				out["log_type"] = val
			}
			if md, ok := mm["metadata"].(map[string]any); ok {
				if lt, ok2 := md["log_type"]; ok2 {
					out["log_type"] = lt
				}
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

	setFieldInMsg(msg, "TraceID", sc.TraceID().String())
	setFieldInMsg(msg, "SpanID", sc.SpanID().String())

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

// LogWithMeta writes a normal application log with metadata and attaches trace info from ctx if present.
func (c *Modules) LogWithMeta(ctx context.Context, level interfaces.LogLevel, message string, meta map[string]any) {
	if meta == nil {
		meta = map[string]any{}
	}
	// only set default if not provided
	if _, ok := meta["log_type"]; !ok {
		meta["log_type"] = string(LogTypeLOG)
	} else {
		meta["log_type"] = NormalizeLogType(meta["log_type"])
	}

	msg := interfaces.LoggerMessage{
		ID:        hash.CreateRandomId(10),
		Time:      time.Now(),
		Level:     level,
		LevelName: interfaces.GetLogLevelString(level),
		File:      "",
		Line:      0,
		FuncName:  "",
		Message: map[string]any{
			"message":  message,
			"metadata": meta,
		},
	}

	attachTraceToMsg(ctx, &msg)
	c.output(msg)
}

// APM writes observability / trace-correlated logs
func (c *Modules) APM(ctx context.Context, key string, data any) {
	meta := map[string]any{
		"log_type": string(LogTypeAPM),
		"apm_key":  key,
		"data":     data,
	}
	msg := interfaces.LoggerMessage{
		ID:        hash.CreateRandomId(10),
		Time:      time.Now(),
		Level:     interfaces.LogLevelInfo,
		LevelName: interfaces.GetLogLevelString(interfaces.LogLevelInfo),
		Message: map[string]any{
			"message":  fmt.Sprintf("%s", key),
			"metadata": meta,
		},
	}
	attachTraceToMsg(ctx, &msg)
	c.output(msg)
}

// Access writes service-to-service logs — renamed to SERVICE_TO_SERVICE semantic (outgoing client)
func (c *Modules) Access(ctx context.Context, req *http.Request, res *http.Response, latencyMs int64, extra map[string]any) {
	meta := map[string]any{
		"log_type": string(LogTypeServiceToService),
		"http.method": func() string {
			if req != nil {
				return req.Method
			}
			return ""
		}(),
		"http.path": func() string {
			if req != nil && req.URL != nil {
				return req.URL.Path
			}
			return ""
		}(),
		"http.host": func() string {
			if req != nil && req.URL != nil {
				return req.URL.Host
			}
			return ""
		}(),
		"http.status": func() int {
			if res != nil {
				return res.StatusCode
			}
			return 0
		}(),
		"latency_ms": latencyMs,
	}
	for k, v := range extra {
		meta[k] = v
	}

	msg := interfaces.LoggerMessage{
		ID:        hash.CreateRandomId(10),
		Time:      time.Now(),
		Level:     interfaces.LogLevelInfo,
		LevelName: interfaces.GetLogLevelString(interfaces.LogLevelInfo),
		Message: map[string]any{
			"message":  fmt.Sprintf("%s %s -> %d", meta["http.method"], meta["http.path"], meta["http.status"]),
			"metadata": meta,
		},
	}

	attachTraceToMsg(ctx, &msg)
	c.output(msg)
}

// Audit writes audit logs (actor/action/details). Usually persisted with strict retention.
func (c *Modules) Audit(actor, action string, details map[string]any) {
	meta := map[string]any{
		"log_type": string(LogTypeAuditTrail),
		"actor":    actor,
		"action":   action,
	}
	for k, v := range details {
		meta[k] = v
	}

	msg := interfaces.LoggerMessage{
		ID:        hash.CreateRandomId(10),
		Time:      time.Now(),
		Level:     interfaces.LogLevelNotice,
		LevelName: interfaces.GetLogLevelString(interfaces.LogLevelNotice),
		Message: map[string]any{
			"message":  action,
			"metadata": meta,
		},
	}
	// by default we do not attach trace to audit logs
	c.output(msg)
}

// ApplicationLog writes a generic application log (log_type = "LOG").
// - ctx: to attach trace/span if any
// - level: interfaces.LogLevel (trace/debug/info/...)
// - message: short message
// - meta: additional metadata (map[string]any) - optional
func ApplicationLog(ctx context.Context, level interfaces.LogLevel, message string, meta map[string]any) {
	if meta == nil {
		meta = map[string]any{}
	}
	// ensure log_type present (createJsonMsg will promote it to root)
	meta["log_type"] = "LOG"
	// attach trace/span from ctx if present
	if ctx != nil {
		if span := trace.SpanFromContext(ctx); span != nil {
			if sc := span.SpanContext(); sc.IsValid() {
				meta["trace_id"] = sc.TraceID().String()
				meta["span_id"] = sc.SpanID().String()
				if dd := ConvertTraceIDToDatadogFormat(sc.TraceID().String()); dd != "" {
					meta["datadog_trace_id"] = dd
				}
			}
		}
	}

	// prefer concrete impl to reuse LogWithMeta (keeps consistent formatting)
	if impl, ok := globalLogger.(*Modules); ok && impl != nil {
		impl.LogWithMeta(ctx, level, message, meta)
		return
	}

	// fallback: call appropriate globalLogger level method
	if globalLogger == nil {
		return
	}
	payload := map[string]any{"message": message, "metadata": meta}
	switch level {
	case interfaces.LogLevelTrace:
		globalLogger.Trace(payload)
	case interfaces.LogLevelDebug:
		globalLogger.Debug(payload)
	case interfaces.LogLevelNotice:
		globalLogger.Notice(payload)
	case interfaces.LogLevelInfo:
		globalLogger.Info(payload)
	case interfaces.LogLevelWarning:
		globalLogger.Warning(payload)
	case interfaces.LogLevelSuccess:
		globalLogger.Success(payload)
	case interfaces.LogLevelError:
		globalLogger.Error(payload)
	default:
		globalLogger.Info(payload)
	}
}

// APMEvent writes an observability / APM structured log (log_type = "APM").
// data parameter should match your APM schema (host_name, ip, action, metadata, etc.)
func APMEvent(ctx context.Context, level interfaces.LogLevel, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	meta := map[string]any{
		"log_type": "APM",
		"data":     data,
	}
	// attach trace/span
	if ctx != nil {
		if span := trace.SpanFromContext(ctx); span != nil {
			if sc := span.SpanContext(); sc.IsValid() {
				meta["trace_id"] = sc.TraceID().String()
				meta["span_id"] = sc.SpanID().String()
				if dd := ConvertTraceIDToDatadogFormat(sc.TraceID().String()); dd != "" {
					meta["datadog_trace_id"] = dd
				}
			}
			// also add event to span
			span.AddEvent("apm.event")
		}
	}

	// prefer concrete impl
	if impl, ok := globalLogger.(*Modules); ok && impl != nil {
		impl.APM(ctx, "apm_event", data) // keep existing APM method behavior
		return
	}

	Logged(level, fmt.Sprintf("%v", data["action"]), meta)
}

// AuditTrailLog writes an audit trail record (log_type = "AUDIT_TRAIL").
// - actor: map with actor fields (e.g. {"user":"andi.budi"})
// - action: short action string
// - status: "SUCCESS"/"FAILED"/etc
// - deviceInfo: map with device info (user_agent, ip_address, app_version)
// - details: additional metadata
func AuditTrailLog(ctx context.Context, level interfaces.LogLevel, actor map[string]any, action string, status string, deviceInfo map[string]any, details map[string]any) {
	if actor == nil {
		actor = map[string]any{}
	}
	if deviceInfo == nil {
		deviceInfo = map[string]any{}
	}
	if details == nil {
		details = map[string]any{}
	}

	meta := map[string]any{
		"log_type":    "AUDIT_TRAIL",
		"actor":       actor,
		"status":      status,
		"msg":         action,
		"device_info": deviceInfo,
		"metadata":    details,
	}

	// attach trace/span if present
	if ctx != nil {
		if span := trace.SpanFromContext(ctx); span != nil {
			if sc := span.SpanContext(); sc.IsValid() {
				meta["trace_id"] = sc.TraceID().String()
				meta["span_id"] = sc.SpanID().String()
				if dd := ConvertTraceIDToDatadogFormat(sc.TraceID().String()); dd != "" {
					meta["datadog_trace_id"] = dd
				}
			}
		}
	}

	if impl, ok := globalLogger.(*Modules); ok && impl != nil {
		impl.LogWithMeta(ctx, level, action, meta)
		return
	}

	Logged(level, action, meta)
}

// AccessLogStructured writes an ACCESS_LOG record with HTTP access info.
// Use it for incoming requests (client -> service).
// Fields follow your ACCESS_LOG JSON schema.
// AccessLogStructured writes an ACCESS_LOG with a structured root-level JSON payload.
// It will prefer concrete *Modules impl (to allow impl.Info(payload)) and fallback to globalLogger.Info.
func AccessLogStructured(ctx context.Context, level interfaces.LogLevel,
	bytesIn int64, bytesOut int64,
	host string, latency int64,
	method string, msgStr string, path string, protocol string,
	referer string, remoteIP string, status int, userAgent string, extra map[string]any) {

	// base payload (root-level) — timestamp uses RFC3339Nano for high precision and timezone
	payload := map[string]any{
		"timestamp":  time.Now().Format(time.RFC3339Nano),
		"log_type":   "ACCESS_LOG",
		"level":      strings.ToLower(interfaces.GetLogLevelString(level)),
		"trace_id":   "",
		"bytes_in":   bytesIn,
		"bytes_out":  bytesOut,
		"host":       host,
		"latency":    latency,
		"method":     method,
		"msg":        msgStr,
		"path":       path,
		"protocol":   protocol,
		"referer":    referer,
		"remote_ip":  remoteIP,
		"status":     status,
		"user_agent": userAgent,
	}

	// attach trace/span if present
	if ctx != nil {
		if sp := trace.SpanFromContext(ctx); sp != nil {
			if sc := sp.SpanContext(); sc.IsValid() {
				payload["trace_id"] = sc.TraceID().String()
				payload["span_id"] = sc.SpanID().String()
				if dd := ConvertTraceIDToDatadogFormat(sc.TraceID().String()); dd != "" {
					payload["datadog_trace_id"] = dd
				}
			}
		}
	}

	// Ensure msg fallback if empty
	if payload["msg"] == "" {
		payload["msg"] = fmt.Sprintf("%s %s -> %d", method, path, status)
	}

	// Merge extra but DO NOT overwrite core keys unless core key is absent.

	for k, v := range extra {
		if _, exists := payload[k]; !exists {
			payload[k] = v
		} else {
			// If extra contains a nested metadata map, merge into "metadata" key
			// to avoid overwriting top-level fields; create metadata if needed.
			if k == "metadata" {
				if mm, ok := v.(map[string]any); ok {
					var meta map[string]any
					if existing, ok2 := payload["metadata"].(map[string]any); ok2 && existing != nil {
						meta = existing
					} else {
						meta = map[string]any{}
					}
					for kk, vv := range mm {
						// don't overwrite top-level keys accidentally
						if _, topExists := payload[kk]; !topExists {
							meta[kk] = vv
						}
					}
					payload["metadata"] = meta
				}
			}
		}
	}

	// Prefer concrete implementation so logger can emit top-level JSON as-is.
	if impl, ok := globalLogger.(*Modules); ok && impl != nil {
		impl.Info(payload)
		return
	}

	Logged(level, msgStr, nil)
}

// The logger's createJsonMsg will pull "log_type" out of metadata and place it at root level.
func ServiceToServiceLog(ctx context.Context,
	host, path, method string,
	requestBody any, requestHeader any,
	responseBody any, responseHeader any,
	level interfaces.LogLevel,
	extra map[string]any) {

	// Prepare metadata
	meta := map[string]any{
		"log_type": "SERVICE_TO_SERVICE",
		"host":     host,
		"path":     path,
		"method":   method,
	}

	// request
	if requestBody != nil {
		meta["request.body"] = requestBody
	}
	if requestHeader != nil {
		meta["request.header"] = requestHeader
	}

	// response
	if responseBody != nil {
		meta["response.body"] = responseBody
	}
	if responseHeader != nil {
		meta["response.header"] = responseHeader
	}

	maps.Copy(meta, extra)
	if ctx != nil {
		if span := trace.SpanFromContext(ctx); span != nil {
			if sc := span.SpanContext(); sc.IsValid() {
				meta["trace_id"] = sc.TraceID().String()
				meta["span_id"] = sc.SpanID().String()
				// also datadog lower-64bits if convertible
				if dd := ConvertTraceIDToDatadogFormat(sc.TraceID().String()); dd != "" {
					meta["datadog_trace_id"] = dd
				}
			}
		}
	}

	// build message string (succinct)
	msgStr := fmt.Sprintf("%s %s -> %v", method, path, meta["response.header"])
	// if status is present in extra or meta, make message more useful
	if s, ok := meta["http.status"]; ok {
		msgStr = fmt.Sprintf("%s -> %v", method+" "+path, s)
	}

	// If concrete Modules implementation available as globalLogger, prefer using its LogWithMeta helper
	if impl, ok := globalLogger.(*Modules); ok && impl != nil {
		impl.LogWithMeta(ctx, level, msgStr, meta)
		return
	}

	Logged(level, msgStr, meta)
}

func Logged(level interfaces.LogLevel, msgStr string, meta map[string]any) {
	if globalLogger == nil {
		return
	}

	payload := map[string]any{"message": msgStr, "metadata": meta}
	switch level {
	case interfaces.LogLevelTrace:
		globalLogger.Trace(payload)
	case interfaces.LogLevelDebug:
		globalLogger.Debug(payload)
	case interfaces.LogLevelNotice:
		globalLogger.Notice(payload)
	case interfaces.LogLevelInfo:
		globalLogger.Info(payload)
	case interfaces.LogLevelWarning:
		globalLogger.Warning(payload)
	case interfaces.LogLevelSuccess:
		globalLogger.Success(payload)
	case interfaces.LogLevelError:
		globalLogger.Error(payload)
	default:
		globalLogger.Info(payload)
	}
}
