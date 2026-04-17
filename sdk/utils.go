package sdk

import (
	"log"
	"os"
	"sync/atomic"
)

type LogLevel int32

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

var currentLevel atomic.Int32

func SetLogLevel(level LogLevel) {
	currentLevel.Store(int32(level))
}

func LogDebug(format string, v ...interface{}) {
	if currentLevel.Load() <= int32(LogLevelDebug) {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func LogInfo(format string, v ...interface{}) {
	if currentLevel.Load() <= int32(LogLevelInfo) {
		log.Printf("[INFO] "+format, v...)
	}
}

func LogWarn(format string, v ...interface{}) {
	if currentLevel.Load() <= int32(LogLevelWarn) {
		log.Printf("[WARN] "+format, v...)
	}
}

func LogError(format string, v ...interface{}) {
	if currentLevel.Load() <= int32(LogLevelError) {
		log.Printf("[ERROR] "+format, v...)
	}
}

func LogFatal(format string, v ...interface{}) {
	log.Printf("[FATAL] "+format, v...)
	os.Exit(1)
}
