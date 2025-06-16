//go:build goexperiment.jsonv2

package stringkeyed

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetMarshalJSONTo(t *testing.T) {
	s := SetOf("one", "two", "three")
	_, err := json.Marshal(s)
	assert.ErrorIs(t, err, errors.ErrUnsupported)
}

func TestSetUnmarshalJSONFrom(t *testing.T) {
	var s Set
	err := json.Unmarshal([]byte(`["one","two","three"]`), &s)
	assert.ErrorIs(t, err, errors.ErrUnsupported)
}
