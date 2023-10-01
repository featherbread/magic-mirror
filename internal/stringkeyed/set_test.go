package stringkeyed

import (
	"encoding/json"
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
			lo.Shuffle(elements)

			s := SetOf(elements...)
			t.Logf("encoded set: %q", s.joined)

			if s.Cardinality() != len(elements) {
				t.Errorf("incorrect cardinality: got %d, want %d", s.Cardinality(), len(elements))
			}

			got := s.ToSlice()
			if diff := cmp.Diff(tc.Elements, got); diff != "" {
				t.Errorf("got back different elements than put in (-want +got):\n%s", diff)
			}

			x := SetOf(elements...)
			lo.Shuffle(elements)
			x.Add(elements...)
			if s != x {
				t.Errorf("sets with the same content compared unequal: %q vs. %q", s.joined, x.joined)
			}
		})
	}
}

func TestSetMarshalJSON(t *testing.T) {
	s := SetOf("one", "two", "three")
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
		s := SetOf(chunks...)
		t.Logf("encoded set: %q", s.joined)

		uniqChunks := lo.Uniq(chunks)
		slices.Sort(uniqChunks)

		gotChunks := s.ToSlice()
		if diff := cmp.Diff(uniqChunks, gotChunks, cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("ToSlice() did not return expected elements (-want +got):\n%s", diff)
		}

		if len(gotChunks) != s.Cardinality() {
			t.Errorf("Cardinality() == %d, want %d", s.Cardinality(), len(gotChunks))
		}
	})
}
