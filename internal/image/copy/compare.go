package copy

import (
	"bytes"
	"encoding/json"

	"github.com/opencontainers/go-digest"
)

type CompareMode int

const (
	CompareModeEqual CompareMode = iota
	CompareModeAnnotation
)

var comparisons = map[CompareMode]func(src, dst manifest) (bool, error){
	CompareModeEqual:      compareEqual,
	CompareModeAnnotation: compareAnnotation,
}

func compareEqual(src, dst manifest) (bool, error) {
	return bytes.Equal(src.Body, dst.Body), nil
}

const annotationSourceDigest = "co.alexhamlin.magic-mirror.source-digest"

func compareAnnotation(src, dst manifest) (bool, error) {
	var parsedDestination struct {
		Annotations map[string]string `json:"annotations"`
	}
	if err := json.Unmarshal(dst.Body, &parsedDestination); err != nil {
		return false, err
	}

	rawWantDigest, ok := parsedDestination.Annotations[annotationSourceDigest]
	if !ok {
		return false, nil
	}

	wantDigest := digest.Digest(rawWantDigest)
	if err := wantDigest.Validate(); err != nil {
		return false, err
	}

	sourceDigest := wantDigest.Algorithm().FromBytes(src.Body)
	return sourceDigest == wantDigest, nil
}
