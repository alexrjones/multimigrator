package schemata_driver

import (
	"regexp"
	"testing"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/stretchr/testify/assert"
)

func TestParser_NewParser(t *testing.T) {

	schemata := []string{
		"customer",
		"manifest",
		"indexing",
	}

	p, err := newParser(schemata)
	assert.Nil(t, err)

	expected := []*regexp.Regexp{
		regexp.MustCompile("^([0-9]+)_([0-9]+)_(customer)_(.*)\\.(" + (string(source.Down) + "|" + string(source.Up)) + ")\\.(.*)$"),
		regexp.MustCompile("^([0-9]+)_([0-9]+)_(manifest)_(.*)\\.(" + (string(source.Down) + "|" + string(source.Up)) + ")\\.(.*)$"),
		regexp.MustCompile("^([0-9]+)_([0-9]+)_(indexing)_(.*)\\.(" + (string(source.Down) + "|" + string(source.Up)) + ")\\.(.*)$"),
	}

	for i, r := range p.regexes {
		assert.Equal(t, expected[i].String(), r.String())
	}
}

func TestParser_Parse(t *testing.T) {

	schemata := []string{
		"customer",
		"manifest",
		"indexing",
	}

	p, err := newParser(schemata)
	assert.Nil(t, err)

	type testCase struct {
		input    string
		expected *source.Migration
		err      error
	}

	tcs := []testCase{
		{
			input: "0001_01_customer_Create.up.sql",
			expected: &source.Migration{
				Version:    101,
				Identifier: "Create",
				Direction:  source.Up,
				Raw:        "0001_01_customer_Create.up.sql",
			},
		},
		{
			input: "0001_02_manifest_Create_new.up.sql",
			expected: &source.Migration{
				Version:    102,
				Identifier: "Create_new",
				Direction:  source.Up,
				Raw:        "0001_02_manifest_Create_new.up.sql",
			},
		},
		{
			input: "0001_02_indexing_Create_new.up.sql",
			err:   ErrWrongSchemaIndex,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.input, func(t *testing.T) {
			parse, err := p.Parse(tc.input)
			if tc.err != nil {
				assert.NotNil(t, err)
				assert.ErrorIs(t, err, tc.err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tc.expected, parse)
			}
		})
	}
}
