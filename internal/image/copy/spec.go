package copy

import (
	"errors"
	"fmt"

	mapset "github.com/deckarep/golang-set/v2"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

type Spec struct {
	Src image.Image
	Dst image.Image
}

func (s Spec) ToKey() specKey {
	return specKey(s)
}

type specKey struct {
	Src image.Image
	Dst image.Image
}

type requireComparable[K comparable] struct{}

var _ requireComparable[specKey]

func (k specKey) ToSpec() Spec {
	return Spec(k)
}

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
		currentKey := currentSpec.ToKey()
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
