package stringkeyed

import (
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
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
			Description: "one element",
			Elements:    []string{"one"},
		},
		{
			Description: "multiple normal elements",
			Elements:    []string{"one", "three", "two"},
		},
		{
			Description: "multiple elements including controls",
			Elements:    []string{"\u000e x", "\u000e y", "a \u000e b", "p \u001f q", "z"},
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

			got := s.ToSlice()
			if diff := cmp.Diff(tc.Elements, got); diff != "" {
				t.Errorf("got back different elements than put in (-want +got):\n%s", diff)
			}

			var x Set
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
