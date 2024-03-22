package internal

import (
	"go/format"
	"strings"
	"text/template"
	"unicode"
)

type TemplateArgs struct {
	PackageName string
	Schemata    []string
}

var funcMap = template.FuncMap{
	"inc": func(i int) int {
		return i + 1
	},
	"fmt": func(s string) string {

		parts := strings.Split(s, "_")
		var sb strings.Builder
		for _, p := range parts {
			sb.WriteRune(unicode.ToUpper(rune(p[0])))
			if len(p) > 1 {
				sb.WriteString(p[1:])
			}
		}
		return sb.String()
	},
	"last_index": func(index int, c []string) bool {

		return index == len(c)-1
	},
	"join_strings": func(c []string) string {

		return strings.Join(c, `", "`)
	},
}

var tmplStr = `package {{.PackageName}}

import (
	"fmt"
	"errors"
)

var ErrInvalidSchemaLevel = errors.New("invalid schema level")
var SchemaNames = []string{ "{{join_strings .Schemata}}" }

type SchemaLevel uint

const (
	{{$schemata := .Schemata}}
	{{- range $index, $schemaName := $schemata}}
	SchemaLevel{{fmt $schemaName}} SchemaLevel = {{inc $index}}
	{{- if (last_index $index $schemata)}}
	MaximumSchemaLevel SchemaLevel = SchemaLevel{{fmt $schemaName}}
	{{- end}}
	{{- end}}
)

func (s SchemaLevel) SchemaName() (string, error) {

	var ret string

	switch s {
	{{- range $index, $schemaName := .Schemata}}
	case SchemaLevel{{fmt $schemaName}}: ret = "{{$schemaName}}"
	{{- end}}
	}

	if ret == "" {
		return "", ErrInvalidSchemaLevel
	}

	return ret, nil
}

func (s SchemaLevel) String() string {

	ret, err := s.SchemaName()
	if err != nil {
		return fmt.Sprintf("SchemaLevel(%d)", s)
	}
	return ret
}
`
var tmpl = template.Must(template.New("enumTemplate").Funcs(funcMap).Parse(tmplStr))

func ProcessTemplate(args TemplateArgs) (string, error) {

	var sb strings.Builder
	err := tmpl.Execute(&sb, args)
	if err != nil {
		return "", err
	}
	ret, err := format.Source([]byte(sb.String()))
	if err != nil {
		return "", err
	}
	return string(ret), nil
}
