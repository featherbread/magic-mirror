package stringkeyed

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSet(t *testing.T) {
	want := []string{
		"a",
		"one \u001f test",
		"z",
	}

	var s Set
	s.Add(want...)
	got := s.ToSlice()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("got back a different slice than was put in (-want +got):\n%s", diff)
	}
}

func TestEmptySet(t *testing.T) {
	var s Set
	got := s.ToSlice()
	if len(got) > 0 {
		t.Errorf("got elements from an empty set: %#v", got)
	}
}
