package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/image/copy"
)

func main() {
	requests, err := readRequests(os.Stdin)
	if err != nil {
		log.Printf("[main] invalid copy spec: %v", err)
		os.Exit(2)
	}

	copier := copy.NewCopier(10, copy.CompareModeAnnotation)
	defer copier.CloseSubmit()

	if err := copier.CopyAll(requests...); err != nil {
		log.Printf("[main] some copies failed:\n%v", err)
		os.Exit(1)
	}
}

type requestSpec struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

func readRequests(r io.Reader) ([]copy.Request, error) {
	decoder := json.NewDecoder(r)
	var requests []copy.Request
	for {
		specs, err := readRequestSpecs(decoder)
		switch {
		case errors.Is(err, io.EOF):
			return requests, nil
		case err != nil:
			return nil, err
		}

		for _, spec := range specs {
			var req copy.Request
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

func readRequestSpecs(decoder *json.Decoder) ([]requestSpec, error) {
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
