package interfaces

type LoggerConfig struct {
	Level                          string       `yaml:"level" default:"verbose" desc:"log:level" validate:"oneof=trace verbose info warning error"`
	Format                         string       `yaml:"format" default:"default" desc:"log:format" validate:"oneof=default json"`
	File                           *LoggingFile `yaml:"file"`
	SendToDataDog                  bool         `yaml:"sendToDataDog" desc:"log:sendToDataDog"`
	LogNoOfChunk                   int          `yaml:"logNoOfChunk" default:"10" desc:"log:logNoOfChunk" validate:"min=10,max=1000"`
	LogSendInterval                int          `yaml:"logSendInterval" default:"200" desc:"log:logSendInterval" validate:"min=100,max=3000"`
	DatadogExporterContentEncoding string       `yaml:"datadogExporterContentEncoding" default:"deflate" desc:"log:datadogExporterContentEncoding" validate:"oneof=identity deflate gzip"`
}

type LoggingFile struct {
	Enable   bool   `yaml:"enable" default:"false" desc:"log:file:enable"`
	Output   string `yaml:"output" default:"./logs/app.log" desc:"log:file:output"`
	MaxSize  int    `yaml:"maxsize" default:"100" desc:"log:file:maxsize"`
	MaxAge   int    `yaml:"maxage" default:"28" desc:"log:file:maxage"`
	Compress bool   `yaml:"compress"  desc:"log:file:compress"`
}

var LoggerConfigManual = map[string]string{
	"log:level": `logging level, valid value is
		- trace
		- verbose
		- info
		- warning
		- error
	`,
	"log:format": `logging output format, valid value is
		- default
		- json
	`,
	"log:file:enable":      `Enable writing log to file`,
	"log:hideTraceConsole": `Don't print trace log to console, if false, maybe performance impact`,
	"log:file:output":      `Directory log output location`,
	"log:file:maxsize":     `Max log file size (in megabytes) before retain`,
	"log:file:maxage":      `Max log file age (in days) before retain`,
	"log:file:compress":    `Compress the backup files (older than max age) in gzip format`,
	"log:sendToDataDog":    `Send every logs to datadog`,
	"log:logNoOfChunk":     `How to many logs send to exporter at once`,
	"log:logSendInterval":  `Time interval to send logs to exporter, in ms, default 200ms`,
	"log:datadogExporterContentEncoding": `datadog exporter content encoding, valid value is
		- gzip
		- deflate
		- identity
	`,
}

func SetManual(man map[string]string) map[string]string {
	for k, v := range LoggerConfigManual {
		man[k] = v
	}
	return man
}
