//go:build goexperiment.jsonv2

package stringkeyed

import (
	"encoding/json/jsontext"
	"errors"
)

func (s Set) MarshalJSONTo(*jsontext.Encoder) error {
	return errors.ErrUnsupported
}

func (s *Set) UnmarshalJSONFrom(*jsontext.Decoder) error {
	return errors.ErrUnsupported
}
