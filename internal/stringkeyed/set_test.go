package stringkeyed

import (
	"encoding/json"
	"iter"
	"math/rand/v2"
	"reflect"
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
			rand.Shuffle(len(elements), swapper(elements))

			s := SetOf(elements...)
			t.Logf("encoded set: %q", s.joined)

			assert.Equal(t, len(elements), s.Cardinality())

			got := s.ToSlice()
			assert.Equal(t, tc.Elements, got)

			x := SetOf(elements...)
			rand.Shuffle(len(elements), swapper(elements))
			x.Add(elements...)
			if s != x {
				t.Errorf("sets with the same content compared unequal: %q vs. %q", s.joined, x.joined)
			}
		})
	}
}

func swapper[T any, S ~[]T](slice S) func(i, j int) {
	return func(i, j int) { slice[i], slice[j] = slice[j], slice[i] }
}

func TestTestSwapper(t *testing.T) { // That's right: testing the tests.
	s := []string{"a", "b", "c"}
	swapper(s)(0, 1)
	assert.Equal(t, []string{"b", "a", "c"}, s)
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

func TestSetUnmarshalBadJSON(t *testing.T) {
	testCases := []struct {
		Description string
		JSON        string
	}{
		{
			Description: "duplicate values",
			JSON:        `["one","two","one"]`,
		},
		{
			Description: "object at top level",
			JSON:        `{"test":true}`,
		},
		{
			Description: "number in array",
			JSON:        `["one","two",3,"four"]`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Description, func(t *testing.T) {
			var s Set
			err := json.Unmarshal([]byte(tc.JSON), &s)
			if assert.Error(t, err) {
				t.Logf("error was: %v", err)
			}
		})
	}
}

func TestSetUnmarshalJSONInner(t *testing.T) {
	testCases := []struct {
		Description string
		JSON        string
		Value       any
		Want        []Set
	}{
		{
			Description: "value of an object",
			JSON:        `{"ignore": true, "set": ["first", "second"], "alsoIgnore": true}`,
			Value: &struct {
				Ignore     bool `json:"ignore"`
				AlsoIgnore bool `json:"alsoIgnore"`
				Set        Set  `json:"set"`
			}{},
			Want: []Set{
				SetOf("first", "second"),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Description, func(t *testing.T) {
			value := reflect.New(reflect.ValueOf(tc.Value).Elem().Type())
			err := json.Unmarshal([]byte(tc.JSON), value.Interface())
			assert.NoError(t, err)

			got := slices.Collect(digForSets(value))
			assert.Equal(t, tc.Want, got)
		})
	}
}

func digForSets(value reflect.Value) iter.Seq[Set] {
	return func(yield func(Set) bool) {
		reallyDigForSets(value, yield)
	}
}

func reallyDigForSets(value reflect.Value, yield func(Set) bool) bool {
	switch value.Kind() {
	case reflect.Struct:
		if value.Type() == reflect.TypeFor[Set]() {
			return yield(value.Interface().(Set))
		}
		for i := range value.NumField() {
			if !reallyDigForSets(value.Field(i), yield) {
				return false
			}
		}

	case reflect.Pointer:
		return reallyDigForSets(value.Elem(), yield)

	case reflect.Array, reflect.Slice:
		for i := range value.Len() {
			if !reallyDigForSets(value.Index(i), yield) {
				return false
			}
		}

	case reflect.Map:
		for _, key := range value.MapKeys() {
			if !reallyDigForSets(value.MapIndex(key), yield) {
				return false
			}
		}

	}
	return true
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
