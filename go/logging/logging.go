package logging

import (
	"io"
	"os"

	joonix "github.com/joonix/log"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/writer"

	"github.com/malonaz/core/go/contexttag"
)

const (
	// PanicLevel level, highest level of severity. Logs and then calls panic with the message passed to Debug, Info, ...
	PanicLevel Level = iota
	// FatalLevel level. Logs and then calls `logger.Exit(1)`. It will exit even if the logging level is set to Panic.
	FatalLevel
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel
	// InfoLevel level. General operational entries about what's going on inside the application.
	InfoLevel
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel
	// TraceLevel level. Designates finer-grained informational events than the Debug.
	TraceLevel
)

// Level represents the logger's logging severity level.
type Level int

// ForceLogToStdErr is used by cli that require no pollution of stdout.
var ForceLogToStdErr = false

var levels = map[Level]logrus.Level{
	PanicLevel: logrus.PanicLevel,
	FatalLevel: logrus.FatalLevel,
	ErrorLevel: logrus.ErrorLevel,
	WarnLevel:  logrus.WarnLevel,
	InfoLevel:  logrus.InfoLevel,
	DebugLevel: logrus.DebugLevel,
	TraceLevel: logrus.TraceLevel,
}

// Logger is a wrapper around logrus. It is used by all micro-services for logging purposes.
type Logger struct {
	*logrus.Logger
}

// NewLogger returns a new logger
func NewLogger() *Logger {
	logrusLogger := &logrus.Logger{
		Out:          io.Discard,
		Formatter:    joonix.NewFormatter(),
		Level:        logrus.InfoLevel,
		Hooks:        make(logrus.LevelHooks),
		ReportCaller: true,
	}
	logrusLogger.Hooks.Add(new(ContextHook))
	logrusLogger.AddHook(&writer.Hook{
		Writer: os.Stderr,
		LogLevels: []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
		},
	})
	logrusLogger.AddHook(&writer.Hook{
		Writer: os.Stdout,
		LogLevels: []logrus.Level{
			logrus.InfoLevel,
			logrus.DebugLevel,
		},
	})
	return &Logger{logrusLogger}
}

// NewPrettyLogger returns a logger with human readable formatting.
func NewPrettyLogger() *Logger {
	logger := NewLogger()
	logger.Formatter = &Formatter{LogFormat: richLogFormat}
	return logger
}

// NewRawLogger returns as raw logger.
func NewRawLogger() *Logger {
	logger := NewLogger()
	logger.Hooks = make(logrus.LevelHooks)
	logger.Formatter = &Formatter{LogFormat: rawLogFormat}
	logger.Out = os.Stderr
	return logger
}

// SetVerbosity sets the Logger Level.
func (l *Logger) SetVerbosity(level Level) *Logger {
	l.SetLevel(levels[level])
	return l
}

// ContextHook is a logrus hook to add context tags to each log entry
type ContextHook struct{}

// Levels returns the logrus levels this hook is applied to.
func (hook *ContextHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire is called by logrus when a new log entry is created, it adds context tags to the entry.
func (hook *ContextHook) Fire(entry *logrus.Entry) error {
	if entry.Context == nil {
		return nil
	}
	if tags, ok := contexttag.GetLogTags(entry.Context); ok {
		for k, v := range tags.Values() {
			entry.Data[k] = v
		}
	}
	return nil
}
