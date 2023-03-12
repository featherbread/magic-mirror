package stringkeyed

import (
	"encoding/ascii85"
	"io"
	"strings"

	"golang.org/x/exp/slices"
)

const unitSeparator = "\u001f"

type assertComparable[T comparable] struct{}

var _ assertComparable[Set]

// Set is a set of strings that is encoded internally as a Go comparable value.
// The zero value is a valid and empty set.
type Set struct {
	joined string
}

func (s *Set) Add(elems ...string) {
	all := append(s.ToSlice(), elems...)
	slices.Sort(all)
	all = slices.Compact(all)
	encodeAll(all)
	s.joined = strings.Join(all, unitSeparator)
}

func (s Set) ToSlice() []string {
	if len(s.joined) == 0 {
		return nil
	}
	all := strings.Split(s.joined, unitSeparator)
	decodeAll(all)
	return all
}

func encodeAll(elems []string) {
	// TODO: There is probably a more efficient way to do this.
	var builder strings.Builder
	for i := range elems {
		enc := ascii85.NewEncoder(&builder)
		enc.Write([]byte(elems[i]))
		enc.Close()
		elems[i] = builder.String()
		builder.Reset()
	}
}

func decodeAll(elems []string) {
	var builder strings.Builder
	for i := range elems {
		dec := ascii85.NewDecoder(strings.NewReader(elems[i]))
		io.Copy(&builder, dec)
		elems[i] = builder.String()
		builder.Reset()
	}
}
