//go:build goexperiment.jsonv2

package stringkeyed

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
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

func (s *Set) UnmarshalJSONFrom(d *jsontext.Decoder) error {
	var elems []string
	if err := json.UnmarshalDecode(d, &elems); err != nil {
		return err
	}

	set := SetOf(elems...)
	if set.Cardinality() < len(elems) {
		return errors.New("cannot unmarshal duplicate elements in a set")
	}

	*s = set
	return nil
}
