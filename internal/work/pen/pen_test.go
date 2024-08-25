//go:debug panicnil=1
package pen_test

import (
	"errors"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ahamlinman/magic-mirror/internal/work/pen"
)

// someNilValue is intentionally never set to anything other than nil.
// Using it to test nil panics silences analyzers like gopls.
var someNilValue any

func TestPenResults(t *testing.T) {
	testCases := []struct {
		Description string
		Do          func() (int, error)
		Want        pen.Result[int]
	}{
		{
			Description: "normal non-error return",
			Do:          func() (int, error) { return 42, nil },
			Want:        pen.Return[int]{Value: 42},
		},
		{
			Description: "normal error return",
			Do:          func() (int, error) { return 0, errors.New("pen error") },
			Want:        pen.Return[int]{Err: errors.New("pen error")},
		},
		{
			Description: "non-nil panic",
			Do:          func() (int, error) { panic("not nil") },
			Want:        pen.Panic[int]{Value: "not nil"},
		},
		{
			Description: "nil panic",
			Do:          func() (int, error) { panic(someNilValue) },
			Want:        pen.Panic[int]{Value: nil},
		},
		{
			Description: "runtime.Goexit",
			Do:          func() (int, error) { runtime.Goexit(); return 0, nil },
			Want:        pen.Goexit[int]{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Description, func(t *testing.T) {
			got := pen.Do(tc.Do)
			assert.Equal(t, tc.Want, got)
		})
	}
}
