package stringkeyed

import (
	"encoding/json"
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
			Description: "multiple elements including separator",
			Elements:    []string{"a", "one \u001f test", "z"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Description, func(t *testing.T) {
			var s Set
			s.Add(tc.Elements...)

			if s.Len() != len(tc.Elements) {
				t.Errorf("incorrect length: got %d, want %d", s.Len(), len(tc.Elements))
			}

			got := s.ToSlice()
			if diff := cmp.Diff(tc.Elements, got); diff != "" {
				t.Errorf("got back different elements than put in (-want +got):\n%s", diff)
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
