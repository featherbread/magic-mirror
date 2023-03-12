package stringkeyed

import (
	"encoding/ascii85"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"golang.org/x/exp/slices"
)

const (
	shiftOut      = "\u000e"
	unitSeparator = "\u001f"
)

type assertComparable[T comparable] struct{}

var _ assertComparable[Set]

// Set is a set of strings that is comparable with == and !=. The zero value is
// a valid and empty set.
type Set struct {
	// The internal representation of a Set is formed by sorting its raw elements,
	// encoding each one, and concatenating them with the ASCII Unit Separator
	// character U+001F.
	//
	// The per-element encoding has two forms. If the encoded element begins with
	// the ASCII Shift Out character U+000E, the remaining characters of the
	// encoded element are the Ascii85 encoding of the original raw element.
	// Otherwise, the encoded element is equivalent to the original raw element.
	joined string
}

// Add adds the provided elements to s if it does not already contain them. In
// other words, it makes s the union of the elements already in s and the
// elements provided.
func (s *Set) Add(elems ...string) {
	all := append(s.ToSlice(), elems...)
	slices.Sort(all)
	all = slices.Compact(all)
	encodeAll(all)
	s.joined = strings.Join(all, unitSeparator)
}

// ToSlice returns a sorted slice of the elements in s.
func (s Set) ToSlice() []string {
	if len(s.joined) == 0 {
		return nil
	}
	all := strings.Split(s.joined, unitSeparator)
	decodeAll(all)
	return all
}

func (s Set) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.ToSlice())
}

func (s *Set) UnmarshalJSON(b []byte) error {
	var elems []string
	if err := json.Unmarshal(b, &elems); err != nil {
		return err
	}
	slices.Sort(elems)
	if len(slices.Compact(elems)) < len(elems) {
		return errors.New("cannot unmarshal duplicate elements in a set")
	}
	*s = Set{}
	s.Add(elems...)
	return nil
}

func encodeAll(elems []string) {
	var builder strings.Builder
	for i, elem := range elems {
		if strings.Contains(elem, unitSeparator) || strings.HasPrefix(elem, shiftOut) {
			builder.WriteString(shiftOut)
			enc := ascii85.NewEncoder(&builder)
			enc.Write([]byte(elem))
			enc.Close()
			elems[i] = builder.String()
			builder.Reset()
		}
	}
}

func decodeAll(elems []string) {
	var builder strings.Builder
	for i, elem := range elems {
		if strings.HasPrefix(elem, shiftOut) {
			dec := ascii85.NewDecoder(strings.NewReader(elem[1:]))
			io.Copy(&builder, dec)
			elems[i] = builder.String()
			builder.Reset()
		}
	}
}
