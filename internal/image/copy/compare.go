package copy

import (
	"bytes"

	"github.com/opencontainers/go-digest"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

// CompareMode represents how a copy operation compares source and destination
// manifests to determine whether they are already in sync.
//
// This will go away as the functionality and interface of package copy improve.
type CompareMode int

const (
	CompareModeEqual CompareMode = iota
	CompareModeAnnotation
)

type comparer interface {
	// IsMirrored returns true if the destination manifest represents an
	// up-to-date mirroring of the image represented by the source manifest.
	IsMirrored(src, dst image.ManifestKind) bool

	// MarkSource returns a destination manifest that is marked as necessary to
	// indicate that it mirrors the manifest with the provided source digest.
	//
	// Implementations of MarkSource must not modify dst in place.
	MarkSource(dst image.ManifestKind, src digest.Digest) image.ManifestKind
}

var comparers = map[CompareMode]comparer{
	CompareModeEqual:      compareEqual{},
	CompareModeAnnotation: compareAnnotation{},
}

type compareEqual struct{}

func (compareEqual) IsMirrored(src, dst image.ManifestKind) bool {
	return bytes.Equal(src.Encoded(), dst.Encoded())
}

func (compareEqual) MarkSource(dst image.ManifestKind, _ digest.Digest) image.ManifestKind {
	return dst
}

type compareAnnotation struct{}

const annotationSourceDigest = "co.alexhamlin.magic-mirror.source-digest"

func (compareAnnotation) IsMirrored(src, dst image.ManifestKind) bool {
	var dstAnnotations map[string]string
	dstMediaType := dst.GetMediaType()
	switch {
	case dstMediaType.IsIndex():
		dstAnnotations = dst.(image.Index).Parsed().Annotations
	case dstMediaType.IsManifest():
		dstAnnotations = dst.(image.Manifest).Parsed().Annotations
	default:
		return false
	}

	rawWantDigest, ok := dstAnnotations[annotationSourceDigest]
	if !ok {
		return false
	}
	wantDigest := digest.Digest(rawWantDigest)
	if err := wantDigest.Validate(); err != nil {
		return false
	}

	sourceDigest := src.Descriptor().Digest
	return sourceDigest == wantDigest
}

func (c compareAnnotation) MarkSource(dst image.ManifestKind, src digest.Digest) image.ManifestKind {
	mediaType := dst.GetMediaType()
	switch {
	case mediaType.IsIndex():
		return c.markIndex(dst.(image.Index), src)
	case mediaType.IsManifest():
		return c.markManifest(dst.(image.Manifest), src)
	default:
		return dst
	}
}

func (compareAnnotation) markIndex(dst image.Index, src digest.Digest) image.ManifestKind {
	patched := image.DeepCopy(dst.Parsed()).(image.Index).Parsed()
	patched.MediaType = string(image.OCIIndexMediaType)
	if patched.Annotations == nil {
		patched.Annotations = make(map[string]string)
	}
	patched.Annotations[annotationSourceDigest] = src.String()
	return patched
}

func (compareAnnotation) markManifest(dst image.Manifest, src digest.Digest) image.ManifestKind {
	patched := image.DeepCopy(dst.Parsed()).(image.Manifest).Parsed()
	patched.MediaType = string(image.OCIManifestMediaType)
	if patched.Annotations == nil {
		patched.Annotations = make(map[string]string)
	}
	patched.Annotations[annotationSourceDigest] = src.String()
	return patched
}
