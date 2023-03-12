package copy

import (
	"errors"
	"fmt"

	mapset "github.com/deckarep/golang-set/v2"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/stringkeyed"
)

// Spec represents a single request to copy a particular source image to a
// particular destination.
type Spec struct {
	Src       image.Image `json:"src"`
	Dst       image.Image `json:"dst"`
	Transform Transform   `json:"transform,omitempty"`
}

// Transform represents an optional set of transformations to perform while
// mirroring a source image to a destination.
type Transform struct {
	// LimitPlatforms limits the set of platforms copied from a multi-platform
	// source image to those listed. If it is empty, all platforms from the source
	// image will be copied. If the source image is a single-platform image, this
	// setting will be ignored and the image will be copied as-is.
	LimitPlatforms stringkeyed.Set `json:"limitPlatforms,omitempty"`
}

func coalesceRequests(specs []Spec) ([]Spec, error) {
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

	coalesced := make([]Spec, 0, len(specs))
	requestsByDst := make(map[image.Image]Spec)
	for _, current := range specs {
		previous, ok := requestsByDst[current.Dst]
		if !ok {
			coalesced = append(coalesced, current)
			requestsByDst[current.Dst] = current
			continue
		}
		if previous != current {
			errs = append(errs, fmt.Errorf("%s requests inconsistent copies from %s and %s", current.Dst, current.Src, previous.Src))
		}
	}

	return coalesced, errors.Join(errs...)
}
