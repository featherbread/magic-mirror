package copy

import (
	"fmt"

	"github.com/opencontainers/go-digest"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

type CompareMode int

const (
	CompareModeEqual CompareMode = iota
	CompareModeAnnotation
)

var comparisons = map[CompareMode]func(src, dst image.ManifestKind) (bool, error){
	CompareModeEqual:      compareEqual,
	CompareModeAnnotation: compareAnnotation,
}

func compareEqual(src, dst image.ManifestKind) (bool, error) {
	return src.Descriptor().Digest == dst.Descriptor().Digest, nil
}

const annotationSourceDigest = "co.alexhamlin.magic-mirror.source-digest"

func compareAnnotation(src, dst image.ManifestKind) (bool, error) {
	var dstAnnotations map[string]string
	dstMediaType := dst.GetMediaType()
	switch {
	case dstMediaType.IsIndex():
		dstAnnotations = dst.(image.Index).Parsed().Annotations
	case dstMediaType.IsManifest():
		dstAnnotations = dst.(image.Manifest).Parsed().Annotations
	default:
		return false, fmt.Errorf("unknown manifest type %s", dstMediaType)
	}

	rawWantDigest, ok := dstAnnotations[annotationSourceDigest]
	if !ok {
		return false, nil
	}
	wantDigest := digest.Digest(rawWantDigest)
	if err := wantDigest.Validate(); err != nil {
		return false, err
	}

	sourceDigest := src.Descriptor().Digest
	return sourceDigest == wantDigest, nil
}
