package stringkeyed

import (
	"encoding/json"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	testCases := []struct {
		Description string
		Elements    []string
	}{
		{
			Description: "empty set",
			Elements:    nil,
		},
		{
			Description: "empty string",
			Elements:    []string{""},
		},
		{
			Description: "one normal element",
			Elements:    []string{"one"},
		},
		{
			Description: "unit separator",
			Elements:    []string{unitSeparator},
		},
		{
			Description: "shift out",
			Elements:    []string{shiftOut},
		},
		{
			Description: "multiple normal elements",
			Elements:    []string{"one", "three", "two"},
		},
		{
			Description: "multiple elements including control chars",
			Elements: []string{
				"",
				"\x0e x",
				"\x0e y",
				"\x1f",
				"\x1fabc\x00\x00\x00\x00x",
				"a",
				"a \x0e b",
				"p \x1f ",
				"p \x1f q",
				"p \x1f qq",
				"z",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Description, func(t *testing.T) {
			elements := slices.Clone(tc.Elements)
			shuffle(elements)

			s := SetOf(elements...)
			t.Logf("encoded set: %q", s.joined)

			assert.Equal(t, len(elements), s.Cardinality())

			got := s.ToSlice()
			assert.Equal(t, tc.Elements, got)

			x := SetOf(elements...)
			shuffle(elements)
			x.Add(elements...)
			if s != x {
				t.Errorf("sets with the same content compared unequal: %q vs. %q", s.joined, x.joined)
			}
		})
	}
}

func shuffle[T any, S ~[]T](ts S) {
	rand.Shuffle(len(ts), func(i, j int) { ts[i], ts[j] = ts[j], ts[i] })
}

func TestSetIterateEmpty(t *testing.T) {
	var s Set
	for elem := range s.All() {
		t.Fatalf("iteration over empty Set yielded %q", elem)
	}
}

func TestSetIterateBreak(t *testing.T) {
	s := SetOf("one", "two", "three")
	for range s.All() {
		break // Should not panic.
	}
}

func TestSetMarshalJSON(t *testing.T) {
	s := SetOf("one", "two", "three")
	want := []byte(`["one","three","two"]`)
	got, err := json.Marshal(s)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestSetUnmarshalJSON(t *testing.T) {
	var s Set
	if err := json.Unmarshal([]byte(`["one","two","three"]`), &s); err != nil {
		t.Fatalf("error unmarshaling valid Set from JSON: %v", err)
	}
	want := []string{"one", "three", "two"}
	got := s.ToSlice()
	assert.Equal(t, want, got)
}

func TestSetUnmarshalJSONInvalid(t *testing.T) {
	var s Set
	err := json.Unmarshal([]byte(`["one","two","one"]`), &s)
	if err == nil {
		t.Error("successfully unmarshaled an invalid Set from JSON")
	} else {
		t.Logf("unmarshal error was: %v", err)
	}
}

func FuzzSetChunkedBytes(f *testing.F) {
	f.Fuzz(func(t *testing.T, chunkSize uint, input string) {
		if chunkSize == 0 {
			t.SkipNow()
		}

		var chunks []string
		for chunk := range slices.Chunk([]byte(input), int(chunkSize)) {
			chunks = append(chunks, string(chunk))
		}

		s := SetOf(chunks...)
		t.Logf("encoded set: %q", s.joined)

		slices.Sort(chunks)
		chunks = slices.Compact(chunks)

		gotChunks := s.ToSlice()
		assert.Equal(t, len(gotChunks), s.Cardinality())
		if len(gotChunks) > 0 {
			assert.Equal(t, chunks, gotChunks)
		}
	})
}
