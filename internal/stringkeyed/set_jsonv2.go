//go:build goexperiment.jsonv2

package stringkeyed

import (
	"encoding/json/jsontext"
	"errors"
)

func (s Set) MarshalJSONTo(e *jsontext.Encoder) error {
	if err := e.WriteToken(jsontext.BeginArray); err != nil {
		return err
	}
	for element := range s.All() {
		if err := e.WriteToken(jsontext.String(element)); err != nil {
			return err
		}
	}
	if err := e.WriteToken(jsontext.EndArray); err != nil {
		return err
	}
	return nil
}

func (s *Set) UnmarshalJSONFrom(*jsontext.Decoder) error {
	return errors.ErrUnsupported
}
