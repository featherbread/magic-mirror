package image

import (
	"encoding/json"
	"testing"
)

func TestDigestFromJSON(t *testing.T) {
	testCases := []struct {
		Description string
		JSON        string
		Valid       bool
	}{
		{
			Description: "valid sha256",
			JSON:        `{"digest": "sha256:0ff03e684b238b3cb6150490f0ed7080b85498c7924eee586885baf8b48b50a3"}`,
			Valid:       true,
		},
		{
			Description: "invalid sha256",
			JSON:        `{"digest": "sha256:09f911029d74e35bd84156c5635688c1"}`,
			Valid:       false,
		},
		{
			Description: "invalid algorithm",
			JSON:        `{"digest": "md4:ef85b08000be3134ac57a4e81c02b1ce"}`,
			Valid:       false,
		},
		{
			Description: "totally invalid",
			JSON:        `{"digest": "latest"}`,
			Valid:       false,
		},
		{
			Description: "blank",
			JSON:        `{"digest": ""}`,
			Valid:       false,
		},
		{
			Description: "never touched",
			JSON:        `{"unrelated": true}`,
			Valid:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Description, func(t *testing.T) {
			var content struct {
				Digest Digest `json:"digest"`
			}
			err := json.Unmarshal([]byte(tc.JSON), &content)
			valid := (err == nil)
			if valid != tc.Valid {
				t.Errorf("unexpected result: %v", err)
			} else if err != nil {
				t.Logf("error was: %v", err)
			}
		})
	}
}
