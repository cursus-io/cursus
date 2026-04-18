package sdk

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func captureLog(fn func()) string {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(nil)
		log.SetFlags(log.LstdFlags)
	}()
	fn()
	return buf.String()
}

func TestSetLogLevel(t *testing.T) {
	SetLogLevel(LogLevelDebug)
	if currentLevel.Load() != int32(LogLevelDebug) {
		t.Errorf("expected level Debug, got %d", currentLevel.Load())
	}

	SetLogLevel(LogLevelError)
	if currentLevel.Load() != int32(LogLevelError) {
		t.Errorf("expected level Error, got %d", currentLevel.Load())
	}

	// Restore default
	SetLogLevel(LogLevelInfo)
}

func TestLogDebug_OutputWhenEnabled(t *testing.T) {
	SetLogLevel(LogLevelDebug)
	defer SetLogLevel(LogLevelInfo)

	out := captureLog(func() {
		LogDebug("test message %d", 42)
	})
	if !strings.Contains(out, "[DEBUG]") {
		t.Errorf("expected [DEBUG] prefix, got: %s", out)
	}
	if !strings.Contains(out, "test message 42") {
		t.Errorf("expected message content, got: %s", out)
	}
}

func TestLogDebug_SuppressedWhenHigherLevel(t *testing.T) {
	SetLogLevel(LogLevelInfo)

	out := captureLog(func() {
		LogDebug("should not appear")
	})
	if out != "" {
		t.Errorf("expected no output at Info level, got: %s", out)
	}
}

func TestLogInfo_Output(t *testing.T) {
	SetLogLevel(LogLevelInfo)

	out := captureLog(func() {
		LogInfo("info msg")
	})
	if !strings.Contains(out, "[INFO]") {
		t.Errorf("expected [INFO] prefix, got: %s", out)
	}
}

func TestLogInfo_SuppressedAtWarnLevel(t *testing.T) {
	SetLogLevel(LogLevelWarn)
	defer SetLogLevel(LogLevelInfo)

	out := captureLog(func() {
		LogInfo("should not appear")
	})
	if out != "" {
		t.Errorf("expected no output at Warn level, got: %s", out)
	}
}

func TestLogWarn_Output(t *testing.T) {
	SetLogLevel(LogLevelWarn)
	defer SetLogLevel(LogLevelInfo)

	out := captureLog(func() {
		LogWarn("warn msg")
	})
	if !strings.Contains(out, "[WARN]") {
		t.Errorf("expected [WARN] prefix, got: %s", out)
	}
}

func TestLogError_Output(t *testing.T) {
	SetLogLevel(LogLevelError)
	defer SetLogLevel(LogLevelInfo)

	out := captureLog(func() {
		LogError("error msg")
	})
	if !strings.Contains(out, "[ERROR]") {
		t.Errorf("expected [ERROR] prefix, got: %s", out)
	}
}

func TestLogError_AlwaysVisibleAtErrorLevel(t *testing.T) {
	SetLogLevel(LogLevelError)
	defer SetLogLevel(LogLevelInfo)

	out := captureLog(func() {
		LogError("critical")
	})
	if !strings.Contains(out, "critical") {
		t.Errorf("expected error message, got: %s", out)
	}
}
