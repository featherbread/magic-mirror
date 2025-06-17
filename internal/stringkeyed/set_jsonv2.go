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
	emit := func(token jsontext.Token) bool {
		if err == nil {
			err = e.WriteToken(token)
		}
		return err == nil
	}

	emit(jsontext.BeginArray)
	s.All()(func(element string) bool {
		return emit(jsontext.String(element))
	})
	emit(jsontext.EndArray)
	return
}

func (s *Set) UnmarshalJSONFrom(d *jsontext.Decoder) error {
	t, err := d.ReadToken()
	if err != nil {
		return err
	}
	if t.Kind() != '[' {
		return &json.SemanticError{
			JSONPointer: d.StackPointer(),
			JSONKind:    t.Kind(),
			GoType:      reflect.TypeFor[Set](),
			Err:         errors.New("array of strings required"),
		}
	}

	elements := make(map[string]struct{})

tokenLoop:
	for {
		t, err := d.ReadToken()
		if err != nil {
			return err
		}

		switch t.Kind() {
		case ']':
			break tokenLoop

		case '"':
			oldLen := len(elements)
			elements[t.String()] = struct{}{}
			if len(elements) == oldLen {
				return &json.SemanticError{
					JSONPointer: d.StackPointer(),
					JSONKind:    t.Kind(),
					GoType:      reflect.TypeFor[string](),
					Err:         errors.New("cannot unmarshal duplicate elements in a set"),
				}
			}

		default:
			return &json.SemanticError{
				JSONPointer: d.StackPointer(),
				JSONKind:    t.Kind(),
				GoType:      reflect.TypeFor[string](),
			}
		}
	}

	allElements := slices.Collect(maps.Keys(elements))
	*s = SetOf(allElements...)
	return nil
}
