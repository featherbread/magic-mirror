package stringkeyed

import (
	"encoding/ascii85"
	"encoding/json"
	"errors"
	"fmt"
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
	// encoded element are the Ascii85 encoding of the original raw element with
	// no additional control or whitespace characters. Otherwise, the encoded
	// element is equivalent to the original raw element.
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
	for i, elem := range elems {
		// Note that the per-element encoding is defined only in terms of the final
		// encoded form, so we can be flexible about when we choose to encode. For
		// now, we only encode in the two cases where the properties of the encoding
		// absolutely require it: when the raw element contains a Unit Separator
		// (which conflicts with the higher-level Set representation) or starts with
		// a Shift Out (which conflicts with the Ascii85 marker in the encoding).
		if strings.Contains(elem, unitSeparator) || strings.HasPrefix(elem, shiftOut) {
			elems[i] = encodeAscii85Element(elem)
		}
	}
}

func encodeAscii85Element(elem string) string {
	out := make([]byte, 1+ascii85.MaxEncodedLen(len(elem)))
	out[0] = shiftOut[0]
	outlen := ascii85.Encode(out[1:], []byte(elem))
	return string(out[:1+outlen])
}

func decodeAll(elems []string) {
	for i, elem := range elems {
		if strings.HasPrefix(elem, shiftOut) {
			elems[i] = decodeAscii85Element(elem)
		}
	}
}

func decodeAscii85Element(elem string) string {
	// We must strip off the Shift Out byte used to mark the Ascii85 encoding.
	encoded := elem[1:]

	// Ascii85 produces 5 bytes of output for every 4 bytes of input, with two
	// exceptions: the last block of 5 bytes may be truncated, and runs of 4 zero
	// bytes are encoded with a single "z". The decoder needs enough space in the
	// destination slice to handle every input block, including all 4 of the bytes
	// corresponding to a truncated block at the end. This formula will allocate a
	// large enough slice as long as the encoding does not contain any extraneous
	// whitespace characters. These are allowed by Ascii85 itself, but prohibited
	// by our own specification of the element encoding.
	zeroExpandedLen := len(encoded) + 4*strings.Count(encoded, "z")
	blocks := 1 + zeroExpandedLen/5
	decoded := make([]byte, 4*blocks)

	decodelen, srclen, err := ascii85.Decode(decoded, []byte(encoded), true)
	if err != nil {
		panic(fmt.Errorf("invalid stringkeyed.Set encoding: %q: %v", elem, err))
	}
	if srclen < len(encoded) {
		panic(fmt.Errorf("stringkeyed.Set element not fully decoded: %q", elem))
	}
	return string(decoded[:decodelen])
}
