package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"time"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/image/copy"
	"go.alexhamlin.co/magic-mirror/internal/log"
)

var (
	flagCompareMode = flag.String("compare", "equal", `Mode for comparing manifests ("equal" or "annotated")`)
	flagConcurrency = flag.Int("concurrency", 10, "Number of simultaneous blob and manifest operations")
	flagVerbose     = flag.Bool("verbose", false, "Enable verbose logging of all operations")
)

func main() {
	flag.Parse()

	var specReader io.Reader
	if flag.NArg() == 0 {
		specReader = newStdinWarningReader()
	} else {
		specFile, err := os.Open(flag.Arg(0))
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

	var compareMode copy.CompareMode
	switch *flagCompareMode {
	case "equal":
		compareMode = copy.CompareModeEqual
	case "annotated":
		compareMode = copy.CompareModeAnnotation
	default:
		log.Printf("[main] invalid -compare mode %s", *flagCompareMode)
		os.Exit(2)
	}

	if *flagVerbose {
		log.EnableVerbose()
	}

	if err := copy.CopyAll(*flagConcurrency, compareMode, copySpecs...); err != nil {
		log.Printf("[main] some copies failed:\n%v", err)
		os.Exit(1)
	}
}

type requestSpec struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

func readAllCopySpecs(r io.Reader) ([]copy.Spec, error) {
	decoder := json.NewDecoder(r)
	var requests []copy.Spec
	for {
		specs, err := readNextCopySpecs(decoder)
		switch {
		case errors.Is(err, io.EOF):
			return requests, nil
		case err != nil:
			return nil, err
		}

		for _, spec := range specs {
			var req copy.Spec
			if req.Src, err = image.Parse(spec.Src); err != nil {
				return nil, err
			}
			if req.Dst, err = image.Parse(spec.Dst); err != nil {
				return nil, err
			}
			requests = append(requests, req)
		}
	}
}

func readNextCopySpecs(decoder *json.Decoder) ([]requestSpec, error) {
	var next json.RawMessage
	if err := decoder.Decode(&next); err != nil {
		return nil, err
	}

	var nextSlice []requestSpec
	var sliceErr error
	if sliceErr = json.Unmarshal(next, &nextSlice); sliceErr == nil {
		return nextSlice, nil
	}

	var nextValue requestSpec
	var valueErr error
	if valueErr = json.Unmarshal(next, &nextValue); valueErr == nil {
		return []requestSpec{nextValue}, nil
	}

	return nil, errors.Join(sliceErr, valueErr)
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
