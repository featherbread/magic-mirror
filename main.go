package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"github.com/spf13/pflag"

	"github.com/featherbread/magic-mirror/internal/image/copy"
	"github.com/featherbread/magic-mirror/internal/log"
)

var (
	flagConcurrency = pflag.Int("concurrency", 10, "Number of concurrent operations for each task")
	flagVerbose     = pflag.Bool("verbose", false, "Enable verbose logging of all operations")
)

func main() {
	pflag.Parse()

	if *flagConcurrency < 1 {
		log.Printf("[main] concurrency must be at least 1")
		os.Exit(2)
	}

	var specReader io.Reader
	if pflag.NArg() == 0 {
		specReader = newStdinWarningReader()
	} else {
		specFile, err := os.Open(pflag.Arg(0))
		if err != nil {
			log.Printf("[main] cannot open %s: %v", os.Args[1], err)
			os.Exit(2)
		}
		defer specFile.Close()
		specReader = specFile
	}
	copySpecs, err := readAllCopySpecs(specReader)
	if err != nil {
		log.Printf("[main] invalid copy spec: %v", err)
		os.Exit(2)
	}

	if *flagVerbose {
		log.EnableVerbose()
	}

	if err := copy.CopyAll(*flagConcurrency, copySpecs...); err != nil {
		log.Printf("[main] some copies failed:\n%v", err)
		os.Exit(1)
	}
}

func readAllCopySpecs(r io.Reader) ([]copy.Spec, error) {
	decoder := json.NewDecoder(r)
	var allSpecs []copy.Spec
	for {
		specs, err := readNextCopySpecs(decoder)
		switch {
		case errors.Is(err, io.EOF):
			return allSpecs, nil
		case err != nil:
			return nil, err
		default:
			allSpecs = append(allSpecs, specs...)
		}
	}
}

func readNextCopySpecs(decoder *json.Decoder) ([]copy.Spec, error) {
	var next json.RawMessage
	if err := decoder.Decode(&next); err != nil {
		return nil, err
	}

	var nextSlice []copy.Spec
	var sliceErr error
	if sliceErr = json.Unmarshal(next, &nextSlice); sliceErr == nil {
		return nextSlice, nil
	}

	var nextValue copy.Spec
	var valueErr error
	if valueErr = json.Unmarshal(next, &nextValue); valueErr == nil {
		return []copy.Spec{nextValue}, nil
	}

	var unmarshalTypeErr *json.UnmarshalTypeError
	switch {
	case errors.As(sliceErr, &unmarshalTypeErr):
		return nil, valueErr
	default:
		return nil, sliceErr
	}
}

type stdinWarningReader struct {
	*time.Timer
}

func newStdinWarningReader() *stdinWarningReader {
	return &stdinWarningReader{
		Timer: time.AfterFunc(2*time.Second, func() {
			log.Printf("[main] still waiting to read JSON copy specs from standard input")
		}),
	}
}

func (r *stdinWarningReader) Read(b []byte) (n int, err error) {
	n, err = os.Stdin.Read(b)
	r.Timer.Stop()
	return
}

func (r *stdinWarningReader) Close() error {
	return os.Stdin.Close()
}
