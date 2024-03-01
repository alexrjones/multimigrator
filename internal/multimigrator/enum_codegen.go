package multimigrator

import (
	"go/format"
	"html/template"
	"strings"
	"unicode"
)

type TemplateArgs struct {
	PackageName string
	Schemata    []string
}

var funcMap = template.FuncMap{
	// The name "inc" is what the function will be called in the template text.
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
}

var tmplStr = `package {{.PackageName}}

import "fmt"

type SchemaLevel int

const (
	{{- range $index, $schemaName := .Schemata}}
	SchemaLevel{{fmt $schemaName}} SchemaLevel = {{inc $index}}
	{{- end}}
)

func (s SchemaLevel) String() string {

	switch s {
	{{- range $index, $schemaName := .Schemata}}
	case SchemaLevel{{fmt $schemaName}}: return "{{$schemaName}}"
	{{- end}}
	}

	return fmt.Sprintf("SchemaLevel(%d)", s)
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
