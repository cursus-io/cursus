package util

import (
	"log"
	"math"
	"os"
	"sync/atomic"
)

var currentLevel atomic.Int32

func SetLevel(level LogLevel) {
	if level < 0 || level > LogLevel(math.MaxInt32) {
		return
	}
	currentLevel.Store(int32(level))
}

func Debug(format string, v ...interface{}) {
	if currentLevel.Load() <= int32(LogLevelDebug) {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func Info(format string, v ...interface{}) {
	if currentLevel.Load() <= int32(LogLevelInfo) {
		log.Printf("[INFO] "+format, v...)
	}
}

func Warn(format string, v ...interface{}) {
	if currentLevel.Load() <= int32(LogLevelWarn) {
		log.Printf("[WARN] "+format, v...)
	}
}

func Error(format string, v ...interface{}) {
	if currentLevel.Load() <= int32(LogLevelError) {
		log.Printf("[ERROR] "+format, v...)
	}
}

func Fatal(format string, v ...interface{}) {
	log.Printf("[FATAL] "+format, v...)
	os.Exit(1)
}
