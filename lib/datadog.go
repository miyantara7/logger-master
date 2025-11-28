// Package lib datadog exporter
package lib

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/miyantara7/logger-master/interfaces"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

func (c *Modules) runDatadogLogExporter() {
	if c.datadogExporterIsRun {
		return
	}
	go c.sendingDatadogLogs()
}

func (c *Modules) RunDatadogTraceLogExporter() {
	if c.datadogTracerExporterIsRun {
		return
	}
	go c.sendingDatadogTracerLogs()
}

func (c *Modules) closeDataDogPendingLogs(ctx context.Context) error {

	c.RLock()
	pendingLogs := len(c.pendingLogs)
	c.RUnlock()

	if pendingLogs != 0 {
		c.Warning("Waiting for %d logs to be sent to datadog", pendingLogs)
		if c.sendToDatadog {
			c.Lock()
			c.datadogChannel = make(chan int)
			c.Unlock()

			for remLog := range c.datadogChannel {
				if remLog == 0 {
					c.Info("All logs has been sent to datadog")
					return nil
				}
				c.Warning("Waiting for %d logs to be sent to datadog", remLog)
				if ctx.Err() != nil {
					c.Error("Failed to send remaining %d logs to datadog : %s", len(c.pendingLogs), ctx.Err())
					return nil
				}
			}
		}
	}

	return nil
}

func (c *Modules) closeDataDogPendingTraceLog(ctx context.Context) error {
	c.RLock()
	pendingTraceLogs := len(c.pendingTracerDatadogLogs)
	c.RUnlock()

	if pendingTraceLogs != 0 {
		c.Warning("Waiting for %d trace logs to be sent to datadog", pendingTraceLogs)
		c.Lock()
		c.datadogTraceChannel = make(chan int)
		c.Unlock()
		for remLog := range c.datadogTraceChannel {
			if remLog == 0 {
				c.Info("All trace logs has been sent to datadog")
				return nil
			}
			c.Warning("Waiting for %d trace logs to be sent to datadog", remLog)
			if ctx.Err() != nil {
				c.Error("Failed to send remaining %d trace logs to datadog : %s", len(c.pendingTracerDatadogLogs), ctx.Err())
				return nil
			}
		}
	}

	return nil
}

func (c *Modules) SendDataDogTraceLog(data *interfaces.DatadogTraceLog) {
	c.Lock()
	defer c.Unlock()
	if c.onClosing || !c.datadogTracerExporterIsRun {
		return
	}
	c.pendingTracerDatadogLogs = append(c.pendingTracerDatadogLogs, data)
}

func (c *Modules) sendingDatadogTracerLogs() {
	c.Lock()
	c.datadogTracerExporterIsRun = true
	c.Unlock()
	currentTry := 1
	for range time.NewTicker(time.Duration(c.config.LogSendInterval) * time.Millisecond).C {
		c.Lock()
		var listToInsert []*interfaces.DatadogTraceLog
		if len(c.pendingTracerDatadogLogs) >= c.config.LogNoOfChunk {
			listToInsert = c.pendingTracerDatadogLogs[:c.config.LogNoOfChunk]
		} else if len(c.pendingTracerDatadogLogs) > 0 {
			listToInsert = c.pendingTracerDatadogLogs
		}
		c.Unlock()

		var body []datadogV2.HTTPLogItem
		hostname, _ := os.Hostname()
		for _, s := range listToInsert {
			body = append(body, datadogV2.HTTPLogItem{
				Ddsource: datadog.PtrString(s.ServiceName + ":tracing"),
				Ddtags:   datadog.PtrString("env:" + s.Env),
				Hostname: datadog.PtrString(hostname),
				Message:  fmt.Sprintf("%v", s.Msg),
				Service:  datadog.PtrString(s.ServiceName),
				AdditionalProperties: map[string]any{
					"dd.span_id":  s.SpanID,
					"dd.trace_id": s.TraceID,
					"key":         fmt.Sprintf("%s:%s", s.Operation, s.Key),
					"caller":      s.Caller,
					"date":        time.Now().String(),
					"level":       interfaces.GetLogLevelString(s.Level),
				},
			})
		}

		var enableLogTraceFile bool
		if c.fileConfig != nil {
			if c.fileConfig.Enable {
				if c.logTraceWriter != nil {
					enableLogTraceFile = true
				}
			}
		}

		if c.datadogTraceLogExporterIsClose && enableLogTraceFile && len(listToInsert) != 0 && len(body) != 0 {
			for _, s := range body {
				if val, err := json.Marshal(s); err == nil {
					if len(val) != 0 {
						ssd := string(val) + "\n"
						if _, err := c.logTraceWriter.Write([]byte(ssd)); err != nil {
							c.internal(interfaces.LogLevelError, err)
						}
					}
				}
			}

			c.Lock()
			if len(listToInsert) != 0 {
				c.pendingTracerDatadogLogs = c.pendingTracerDatadogLogs[len(listToInsert):]
			}
			c.Unlock()
		}

		if c.datadogApi != nil && !c.datadogTraceLogExporterIsClose {

			if len(body) != 0 {
				if retry, err := c.sendToDatadogApi("trace", body); err == nil {
					currentTry = 1
					c.Lock()
					if len(listToInsert) != 0 {
						c.pendingTracerDatadogLogs = c.pendingTracerDatadogLogs[len(listToInsert):]
					}
					c.Unlock()
				} else {
					if retry {
						currentTry++
						tryIn := time.Duration(currentTry*2) * time.Second
						c.internal(interfaces.LogLevelWarning, "[log-trace] retry sending again in %s", tryIn.String())
						time.Sleep(tryIn)
					} else {
						c.Lock()
						c.datadogTraceLogExporterIsClose = true
						c.Unlock()
						c.Warning("Datadog trace log exporter is closed, we write trace logs to file")

					}
				}
			}
		}

		c.RLock()
		remLog := len(c.pendingTracerDatadogLogs)
		channel := c.datadogTraceChannel
		c.RUnlock()

		if !enableLogTraceFile && c.datadogTraceLogExporterIsClose {
			c.Lock()
			c.pendingTracerDatadogLogs = []*interfaces.DatadogTraceLog{}
			c.Unlock()
			if channel != nil {
				channel <- 0
			}
			return
		}

		if channel != nil {
			channel <- remLog
		}

	}
}

func (c *Modules) sendingDatadogLogs() {
	c.Lock()
	c.datadogExporterIsRun = true
	c.Unlock()
	currentTry := 1
	for range time.NewTicker(time.Duration(c.config.LogSendInterval) * time.Millisecond).C {

		c.Lock()
		var listToInsert []*interfaces.LoggerMessage
		if len(c.pendingLogs) >= c.config.LogNoOfChunk {
			listToInsert = c.pendingLogs[:c.config.LogNoOfChunk]
		} else if len(c.pendingLogs) > 0 {
			listToInsert = c.pendingLogs
		}
		c.Unlock()

		if c.datadogApi != nil && c.sendToDatadog && !c.datadogLogExporterIsClose {
			serviceName := strings.ReplaceAll(strings.ToLower(c.ServiceName()), " ", "-")
			hostname, _ := os.Hostname()
			env := os.Getenv("ENVIRONMENT")
			if env == "" {
				env = "development"
			}

			var body []datadogV2.HTTPLogItem
			for _, s := range listToInsert {
				body = append(body, datadogV2.HTTPLogItem{
					Ddsource: datadog.PtrString(serviceName),
					Ddtags:   datadog.PtrString("env:" + env),
					Hostname: datadog.PtrString(hostname),
					Message:  fmt.Sprintf("%v", s.Message),
					Service:  datadog.PtrString(c.ServiceName()),
				})
			}

			if len(body) != 0 {
				if retry, err := c.sendToDatadogApi("log", body); err == nil {
					currentTry = 1
					c.Lock()
					if len(listToInsert) != 0 {
						c.pendingLogs = c.pendingLogs[len(listToInsert):]
					}
					c.Unlock()
				} else {

					if retry {
						currentTry++
						tryIn := time.Duration(currentTry*2) * time.Second
						c.internal(interfaces.LogLevelWarning, "[log] retry sending again in %s", tryIn.String())
						time.Sleep(tryIn)
					} else {
						c.Lock()
						c.datadogLogExporterIsClose = true
						c.Unlock()
					}

				}
			}
		}

		c.RLock()
		remLog := len(c.pendingLogs)
		channel := c.datadogChannel
		c.RUnlock()

		if c.datadogLogExporterIsClose {
			c.Lock()
			c.pendingLogs = []*interfaces.LoggerMessage{}
			c.Unlock()
			if channel != nil {
				channel <- 0
			}
			return
		}

		if channel != nil {
			channel <- remLog
		}

	}
}

func (c *Modules) sendToDatadogApi(id string, body []datadogV2.HTTPLogItem) (retry bool, err error) {
	ctx := datadog.NewDefaultContext(context.Background())
	resp, r, err := c.datadogApi.SubmitLog(ctx, body, *datadogV2.NewSubmitLogOptionalParameters().
		WithContentEncoding(datadogV2.ContentEncoding(c.config.DatadogExporterContentEncoding)))
	if err != nil {
		c.internal(interfaces.LogLevelError, "[%s] error sending logs to datadog %s", id, err)
		if r != nil {
			switch r.StatusCode {
			case 401, 403:
				return false, err
			}
		}
		return true, err
	} else {
		switch r.StatusCode {
		case 200, 201, 202:
			return false, nil
		default:
			responseContent, _ := json.MarshalIndent(resp, "", "  ")
			c.internal(interfaces.LogLevelError, "[%s] error sending logs to datadog %s", id, string(responseContent))
			return true, fmt.Errorf("%s", string(responseContent))
		}

	}
}
