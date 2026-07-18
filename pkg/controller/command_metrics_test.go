package controller

import "testing"

func TestMetricCommandNameBoundsLabels(t *testing.T) {
	handler, _ := newTestHandler(t)
	tests := map[string]string{
		"CREATE topic=orders":   "CREATE",
		"consume topic=orders":  "CONSUME",
		"STREAM topic=orders":   "STREAM",
		"user-controlled-value": "UNKNOWN",
		"":                      "EMPTY",
	}
	for command, want := range tests {
		if got := handler.metricCommandName(command); got != want {
			t.Fatalf("metricCommandName(%q) = %q, want %q", command, got, want)
		}
	}
}
