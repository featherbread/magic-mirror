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
	for {
		at := d.InputOffset()
		tok, err := d.ReadToken()
		if err != nil {
			return err
		}

		switch tok.Kind() {
		case ']':
			break

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
