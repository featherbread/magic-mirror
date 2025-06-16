//go:build goexperiment.jsonv2

package stringkeyed

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"maps"
	"reflect"
	"slices"
)

func (s Set) MarshalJSONTo(e *jsontext.Encoder) (err error) {
	emit := func(token jsontext.Token) {
		if err == nil {
			err = e.WriteToken(token)
		}
	}

	emit(jsontext.BeginArray)
	for element := range s.All() {
		emit(jsontext.String(element))
	}
	emit(jsontext.EndArray)
	return
}

func (s *Set) UnmarshalJSONFrom(d *jsontext.Decoder) error {
	at := d.InputOffset()
	tok, err := d.ReadToken()
	if err != nil {
		return err
	}
	if tok.Kind() != '[' {
		return &json.SemanticError{
			ByteOffset:  at,
			JSONPointer: d.StackPointer(),
			JSONKind:    tok.Kind(),
			GoType:      reflect.TypeFor[Set](),
			Err:         errors.New("array of strings required"),
		}
	}

	elems := make(map[string]struct{})

tokenLoop:
	for {
		at := d.InputOffset()
		tok, err := d.ReadToken()
		if err != nil {
			return err
		}

		switch tok.Kind() {
		case ']':
			break tokenLoop

		case '"':
			oldLen := len(elems)
			elems[tok.String()] = struct{}{}
			if len(elems) == oldLen {
				return &json.SemanticError{
					ByteOffset:  at,
					JSONPointer: d.StackPointer(),
					JSONKind:    tok.Kind(),
					GoType:      reflect.TypeFor[string](),
					Err:         errors.New("cannot unmarshal duplicate elements in a set"),
				}
			}

		default:
			return &json.SemanticError{
				ByteOffset:  at,
				JSONPointer: d.StackPointer(),
				JSONKind:    tok.Kind(),
				GoType:      reflect.TypeFor[string](),
			}
		}
	}

	allElems := slices.Collect(maps.Keys(elems))
	*s = SetOf(allElems...)
	return nil
}
