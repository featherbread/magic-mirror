//go:build goexperiment.jsonv2

package stringkeyed

// Note the intentional use of the v1 encoding/json package. For now, I just
// want to see how the new (un)marshaling implementations work with the older
// API and semantics. Maybe I'll change this later.
import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// jsonv1Set hides MarshalJSONTo and UnmarshalJSONFrom to prevent encoding/json
// from preferring them over the old interfaces.
type jsonv1Set struct{ inner Set }

func (s jsonv1Set) MarshalJSON() ([]byte, error) {
	return s.inner.MarshalJSON()
}

func (s *jsonv1Set) UnmarshalJSON(b []byte) error {
	return s.inner.UnmarshalJSON(b)
}

func FuzzSetJSONVersions(f *testing.F) {
	f.Add(`["one","two","three"]`)
	f.Add(`["one","two","one"]`)
	f.Add(`{"test":true}`)
	f.Add(`"hello"`)
	f.Add(`["one",2,"three"]`)
	f.Fuzz(func(t *testing.T, inputJSON string) {
		var set1 jsonv1Set
		err1 := json.Unmarshal([]byte(inputJSON), &set1)

		var set2 Set
		err2 := json.Unmarshal([]byte(inputJSON), &set2)

		isErr1 := (err1 != nil)
		isErr2 := (err2 != nil)
		if isErr1 != isErr2 {
			t.Fatalf("inconsistent error status:\n\tv1: %v\n\tv2:%v", err1, err2)
		}

		assert.Equal(t, set1.inner.ToSlice(), set2.ToSlice())
	})
}

func BenchmarkSetMarshalJSON(b *testing.B) {
	set := SetOf("one", "two", "three", "four", "five")

	b.Run("iface=Marshaler", func(b *testing.B) {
		setv1 := jsonv1Set{set}
		for b.Loop() {
			json.Marshal(setv1)
		}
	})

	b.Run("iface=MarshalerTo", func(b *testing.B) {
		for b.Loop() {
			json.Marshal(set)
		}
	})
}

func BenchmarkSetUnmarshalJSON(b *testing.B) {
	input := []byte(`["one","two","three","four","five"]`)

	b.Run("iface=Unmarshaler", func(b *testing.B) {
		for b.Loop() {
			var set jsonv1Set
			json.Unmarshal(input, &set)
		}
	})

	b.Run("iface=UnmarshalerFrom", func(b *testing.B) {
		for b.Loop() {
			var set Set
			json.Unmarshal(input, &set)
		}
	})
}
