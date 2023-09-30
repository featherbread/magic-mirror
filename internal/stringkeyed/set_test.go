package stringkeyed

import (
	"encoding/json"
	"math/rand"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/samber/lo"
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
			Description: "set of the empty string",
			Elements:    []string{""},
		},
		{
			Description: "one element",
			Elements:    []string{"one"},
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
				"a \x0e b",
				"p \x1f ",
				"p \x1f q",
				"p \x1f qq",
				"p \x1f qqq",
				"p \x1f qqqq",
				"z",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Description, func(t *testing.T) {
			elements := make([]string, len(tc.Elements))
			copy(elements, tc.Elements)
			shuffleElements := func() {
				rand.Shuffle(len(elements), func(i, j int) { elements[i], elements[j] = elements[j], elements[i] })
			}
			shuffleElements()

			var s Set
			s.Add(elements...)
			t.Logf("encoded set: %q", s.joined)

			if s.Cardinality() != len(elements) {
				t.Errorf("incorrect cardinality: got %d, want %d", s.Cardinality(), len(elements))
			}

			got := s.ToSlice()
			if diff := cmp.Diff(tc.Elements, got); diff != "" {
				t.Errorf("got back different elements than put in (-want +got):\n%s", diff)
			}

			var x Set
			shuffleElements()
			x.Add(elements...)
			shuffleElements()
			x.Add(elements...)
			if s != x {
				t.Errorf("sets with the same content compared unequal: %q vs. %q", s.joined, x.joined)
			}
		})
	}
}

func TestSetMarshalJSON(t *testing.T) {
	var s Set
	s.Add("one", "two", "three")

	want := []byte(`["one","three","two"]`)
	got, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("error marshaling to JSON: %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected JSON marshaling (-want +got):\n%s", diff)
	}
}

func TestSetUnmarshalJSON(t *testing.T) {
	var s Set
	if err := json.Unmarshal([]byte(`["one","two","three"]`), &s); err != nil {
		t.Fatalf("error unmarshaling valid Set from JSON: %v", err)
	}
	want := []string{"one", "three", "two"}
	got := s.ToSlice()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected JSON unmarshaling (-want +got):\n%s", diff)
	}
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

func FuzzSetChunkedString(f *testing.F) {
	f.Fuzz(func(t *testing.T, chunkSize uint, input string) {
		if chunkSize == 0 {
			t.SkipNow()
		}

		chunks := lo.ChunkString(input, int(chunkSize))
		uniqChunks := lo.Uniq(chunks)
		slices.Sort(uniqChunks)

		var s Set
		s.Add(chunks...)
		t.Logf("encoded set: %q", s.joined)

		gotChunks := s.ToSlice()
		slices.Sort(gotChunks)
		if diff := cmp.Diff(uniqChunks, gotChunks, cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("ToSlice() did not return expected elements (-want +got):\n%s", diff)
		}

		if len(gotChunks) != s.Cardinality() {
			t.Errorf("Cardinality() == %d, want %d", s.Cardinality(), len(gotChunks))
		}
	})
}
