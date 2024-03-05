package schemata_driver

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/golang-migrate/migrate/v4/source"
)

var (
	ErrParse            = errors.New("no match")
	ErrWrongSchemaIndex = errors.New("incorrect schema index")
)

// Regex matches the following pattern:
//
//	123_name.up.ext
//	123_name.down.ext
var regexTemplate = "^([0-9]+)_([0-9]+)_({{.SchemaName}})_(.*)\\.(" + (string(source.Down) + "|" + string(source.Up)) + ")\\.(.*)$"
var tmpl = template.Must(template.New("fname_template").Parse(regexTemplate))

type parser struct {
	regexes []*regexp.Regexp
}

func newParser(schemata []string) (*parser, error) {

	regexes := make([]*regexp.Regexp, len(schemata))
	for i, s := range schemata {
		var sb strings.Builder
		err := tmpl.Execute(&sb, struct{ SchemaName string }{s})
		if err != nil {
			return nil, err
		}
		compile, err := regexp.Compile(sb.String())
		if err != nil {
			return nil, err
		}
		regexes[i] = compile
	}

	return &parser{regexes}, nil
}

// Parse returns Migration for matching Regex pattern.
func (p *parser) Parse(raw string) (*source.Migration, error) {
	for i, r := range p.regexes {
		m := r.FindStringSubmatch(raw)
		if len(m) == 7 {
			schemaIndex, err := strconv.ParseInt(m[2], 10, 64)
			if err != nil {
				return nil, err
			}
			if schemaIndex != int64(i+1) {
				return nil, ErrWrongSchemaIndex
			}
			versionUint64, err := strconv.ParseUint(m[1]+m[2], 10, 64)
			if err != nil {
				return nil, err
			}
			return &source.Migration{
				Version:    uint(versionUint64),
				Identifier: m[4],
				Direction:  source.Direction(m[5]),
				Raw:        raw,
			}, nil
		}
	}
	return nil, ErrParse
}
