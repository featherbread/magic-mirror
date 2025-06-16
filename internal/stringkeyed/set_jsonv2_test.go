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
	got, err := json.Marshal(s)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`["one","three","two"]`), got)
}

func TestSetUnmarshalJSONFrom(t *testing.T) {
	var s Set
	err := json.Unmarshal([]byte(`["one","two","three"]`), &s)
	assert.ErrorIs(t, err, errors.ErrUnsupported)
}
