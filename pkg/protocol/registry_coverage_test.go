package protocol

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestErrorRegistryCoversStaticBrokerEmissions(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	pkgRoot := filepath.Dir(filepath.Dir(filename))
	errorPattern := regexp.MustCompile(`ERROR: ([A-Za-z0-9_]+)`)
	missing := make(map[string]struct{})

	err := filepath.WalkDir(pkgRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, match := range errorPattern.FindAllSubmatch(data, -1) {
			code := string(match[1])
			if !IsKnownErrorCode(code) {
				missing[code] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) > 0 {
		codes := make([]string, 0, len(missing))
		for code := range missing {
			codes = append(codes, code)
		}
		sort.Strings(codes)
		t.Fatalf("static broker error codes missing from registry: %s", strings.Join(codes, ", "))
	}
}

func TestKnownErrorCodesReturnsSortedCopy(t *testing.T) {
	codes := KnownErrorCodes()
	if len(codes) < 50 {
		t.Fatalf("registry unexpectedly small: %d", len(codes))
	}
	if !sort.StringsAreSorted(codes) {
		t.Fatal("known error codes are not sorted")
	}
	codes[0] = "changed"
	if IsKnownErrorCode("changed") {
		t.Fatal("registry escaped by reference")
	}
}
