//go:build !goexperiment.jsonv2

package stringkeyed

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
