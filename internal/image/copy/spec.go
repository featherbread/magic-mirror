package copy

import (
	"errors"
	"fmt"

	mapset "github.com/deckarep/golang-set/v2"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

// Spec represents a single request to copy a particular source image to a
// particular destination.
type Spec struct {
	Src       image.Image
	Dst       image.Image
	Transform Transform
}

func (s Spec) toKey() specKey {
	return specKey{
		Src:       s.Src,
		Dst:       s.Dst,
		Transform: s.Transform.toKey(),
	}
}

// Transform represents an optional set of transformations to perform while
// mirroring a source image to a destination.
type Transform struct {
	SelectPlatforms struct{}
}

func (t Transform) toKey() transformKey {
	return transformKey{
		SelectPlatforms: stringListKey(""),
	}
}

type requireComparable[K comparable] struct{}

var _ requireComparable[specKey]

type specKey struct {
	Src       image.Image
	Dst       image.Image
	Transform transformKey
}

func (k specKey) ToSpec() Spec {
	return Spec{
		Src:       k.Src,
		Dst:       k.Dst,
		Transform: k.Transform.ToSpec(),
	}
}

type transformKey struct {
	SelectPlatforms stringListKey
}

func (k transformKey) ToSpec() Transform {
	return Transform{
		SelectPlatforms: struct{}{},
	}
}

type stringListKey string

func keyifyRequests(specs []Spec) ([]specKey, error) {
	// TODO: Validate that destinations do not contain digests in annotation
	// comparison mode.

	// TODO: Validate that source and destination digests do not mismatch.

	var errs []error

	srcs := mapset.NewThreadUnsafeSet[image.Image]()
	for _, req := range specs {
		srcs.Add(req.Src)
	}
	for _, req := range specs {
		if srcs.Contains(req.Dst) {
			errs = append(errs, fmt.Errorf("%s is both a source and a destination", req.Dst))
		}
	}

	coalesced := make([]specKey, 0, len(specs))
	requestsByDst := make(map[image.Image]specKey)
	for _, currentSpec := range specs {
		currentKey := currentSpec.toKey()
		previousKey, ok := requestsByDst[currentSpec.Dst]
		if !ok {
			coalesced = append(coalesced, currentKey)
			requestsByDst[currentSpec.Dst] = currentKey
			continue
		}
		if previousKey != currentKey {
			errs = append(errs, fmt.Errorf("%s requests inconsistent copies from %s and %s", currentSpec.Dst, currentSpec.Src, previousKey.Src))
		}
	}

	return coalesced, errors.Join(errs...)
}
