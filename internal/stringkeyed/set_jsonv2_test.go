//go:build goexperiment.jsonv2

package stringkeyed

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type jsonv1Set struct{ inner Set }

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
