package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/cursus-io/cursus/pkg/topic"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help" || args[0] == "help") {
		printUsage(stdout)
		return 0
	}
	if len(args) < 2 {
		printUsage(stderr)
		return 2
	}
	switch args[0] + " " + args[1] {
	case "manifest inspect":
		flags := flag.NewFlagSet("manifest inspect", flag.ContinueOnError)
		flags.SetOutput(stderr)
		logDir := flags.String("log-dir", "", "broker log directory")
		if code := parseCommandFlags(flags, args[2:]); code >= 0 {
			return code
		}
		if *logDir == "" || flags.NArg() != 0 {
			return usageError(stderr, "--log-dir is required and positional arguments are not accepted")
		}
		inventory, err := topic.InspectStandaloneStorage(*logDir)
		if err != nil {
			return operationError(stderr, err)
		}
		return writeJSON(stdout, stderr, inventory)

	case "consumer-metadata inspect":
		flags := flag.NewFlagSet("consumer-metadata inspect", flag.ContinueOnError)
		flags.SetOutput(stderr)
		logDir := flags.String("log-dir", "", "broker log directory")
		if code := parseCommandFlags(flags, args[2:]); code >= 0 {
			return code
		}
		if *logDir == "" || flags.NArg() != 0 {
			return usageError(stderr, "--log-dir is required and positional arguments are not accepted")
		}
		inventory, err := topic.InspectStandaloneStorage(*logDir)
		if err != nil {
			return operationError(stderr, err)
		}
		records := inventory.ConsumerMetadataRecords
		if records == nil {
			records = make([]topic.PersistedConsumerMetadataRecord, 0)
		}
		return writeJSON(stdout, stderr, struct {
			Records  []topic.PersistedConsumerMetadataRecord `json:"records"`
			Problems []topic.StorageProblem                  `json:"problems,omitempty"`
		}{Records: records, Problems: inventory.Problems})

	case "orphan inspect":
		flags := flag.NewFlagSet("orphan inspect", flag.ContinueOnError)
		flags.SetOutput(stderr)
		logDir := flags.String("log-dir", "", "broker log directory")
		if code := parseCommandFlags(flags, args[2:]); code >= 0 {
			return code
		}
		if *logDir == "" || flags.NArg() != 0 {
			return usageError(stderr, "--log-dir is required and positional arguments are not accepted")
		}
		inventory, err := topic.InspectStandaloneStorage(*logDir)
		if err != nil {
			return operationError(stderr, err)
		}
		if !inventory.ManifestPresent {
			return operationError(stderr, errors.New("cannot inspect orphans: no topic metadata manifest is present"))
		}
		declared := make(map[string]struct{}, len(inventory.ManifestTopics))
		for _, name := range inventory.ManifestTopics {
			declared[name] = struct{}{}
		}
		orphans := make([]topic.PersistedTopic, 0)
		for _, persisted := range inventory.Topics {
			if _, ok := declared[persisted.Name]; !ok {
				orphans = append(orphans, persisted)
			}
		}
		return writeJSON(stdout, stderr, struct {
			Topics   []topic.PersistedTopic `json:"topics"`
			Problems []topic.StorageProblem `json:"problems,omitempty"`
		}{Topics: orphans, Problems: inventory.Problems})

	case "orphan archive":
		flags := flag.NewFlagSet("orphan archive", flag.ContinueOnError)
		flags.SetOutput(stderr)
		logDir := flags.String("log-dir", "", "broker log directory")
		archiveDir := flags.String("archive-dir", "", "archive directory outside the broker log directory")
		topicName := flags.String("topic", "", "single orphan topic to archive")
		dryRun := flags.Bool("dry-run", false, "validate without moving")
		if code := parseCommandFlags(flags, args[2:]); code >= 0 {
			return code
		}
		if *logDir == "" || *archiveDir == "" || *topicName == "" || flags.NArg() != 0 {
			return usageError(stderr, "--log-dir, --archive-dir, and --topic are required; positional arguments are not accepted")
		}
		result, err := topic.ArchiveOrphanTopic(*logDir, *archiveDir, *topicName, *dryRun)
		if writeErr := writeJSONValue(stdout, result); writeErr != nil {
			return operationError(stderr, writeErr)
		}
		if err != nil {
			return operationError(stderr, err)
		}
		return 0
	default:
		printUsage(stderr)
		return 2
	}
}

func writeJSON(stdout, stderr io.Writer, value any) int {
	if err := writeJSONValue(stdout, value); err != nil {
		return operationError(stderr, err)
	}
	return 0
}

func writeJSONValue(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func operationError(stderr io.Writer, err error) int {
	_, _ = fmt.Fprintln(stderr, "error:", err)
	return 1
}

func parseCommandFlags(flags *flag.FlagSet, args []string) int {
	err := flags.Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		return 2
	}
	return -1
}

func usageError(stderr io.Writer, message string) int {
	_, _ = fmt.Fprintln(stderr, "error:", message)
	return 2
}

func printUsage(writer io.Writer) {
	_, _ = fmt.Fprintln(writer, "usage:")
	_, _ = fmt.Fprintln(writer, "  cursus-storage manifest inspect --log-dir DIR")
	_, _ = fmt.Fprintln(writer, "  cursus-storage consumer-metadata inspect --log-dir DIR")
	_, _ = fmt.Fprintln(writer, "  cursus-storage orphan inspect --log-dir DIR")
	_, _ = fmt.Fprintln(writer, "  cursus-storage orphan archive --log-dir DIR --archive-dir DIR --topic NAME [--dry-run]")
}
