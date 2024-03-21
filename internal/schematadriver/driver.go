package schematadriver

import (
	"fmt"
	"io/fs"
	nurl "net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"text/template"

	"github.com/alexrjones/multimigrator/util"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

func init() {
	source.Register("schematadriver", &SchemataDriver{})
}

type SchemataDriver struct {
	iofs.PartialDriver
	url  string
	path string
}

func (f *SchemataDriver) Open(url string) (source.Driver, error) {
	p, q, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	nf := &SchemataDriver{
		url:  url,
		path: p,
	}
	fs, err := util.PathsFS(p, q)
	if err != nil {
		return nil, err
	}
	if err := nf.Init(fs, "."); err != nil {
		return nil, err
	}
	return nf, nil
}

func parseURL(url string) (string, []string, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return "", nil, err
	}
	// concat host and path to restore full path
	// host might be `.`
	p := u.Opaque
	if len(p) == 0 {
		p = u.Host + u.Path
	}

	if len(p) == 0 {
		// default to current directory if no path
		wd, err := os.Getwd()
		if err != nil {
			return "", nil, err
		}
		p = wd

	} else if p[0:1] == "." || p[0:1] != "/" {
		// make path absolute if relative
		abs, err := filepath.Abs(p)
		if err != nil {
			return "", nil, err
		}
		p = abs
	}

	return p, u.Query()["path"], nil
}

// This is a template for a regex that matches a path like (0001)_(01)_(Schema)_(Create).up.sql
// where the bracketed parts are:
// - version number
// - schema index
// - schema name
// - migration identifier
var regexTemplate = "\\d+_\\d+_{{.SchemaName}}_[^.]+\\.(?:up|down)\\.sql"
var tmpl = template.Must(template.New("regex_template").Parse(regexTemplate))

func ExpandPaths(rootDir string, schemata []string) (map[string][]string, error) {

	type regexEntry struct {
		re  *regexp.Regexp
		sch string
	}
	byLengthDesc := slices.Clone(schemata)
	slices.SortFunc(byLengthDesc, func(a, b string) int {
		return len(b) - len(a)
	})
	// Build a list of regexes that will match migrations for a particular schema.
	// Schemata with shorter names are earlier in the list so that the most specific
	// name will match first when iterating the schema files.
	regexes := make([]regexEntry, len(schemata))
	for i, s := range byLengthDesc {
		var sb strings.Builder
		err := tmpl.Execute(&sb, struct {
			SchemaName string
		}{s})
		if err != nil {
			return nil, err
		}
		r, err := regexp.Compile(sb.String())
		if err != nil {
			return nil, err
		}
		regexes[i] = regexEntry{r, s}
	}

	ret := make(map[string][]string)
	// Walk the directory and match migration files to schemata
	// The output is a map like:
	// {"first": ["0001_01_first_Create.up.sql"], "second: ["0001_02_second_Create.up.sql"]}
	err := filepath.Walk(rootDir, func(path string, info fs.FileInfo, err error) error {

		if info.IsDir() {
			return nil
		}

		for _, r := range regexes {
			if r.re.MatchString(path) {

				if _, ok := ret[r.sch]; !ok {
					ret[r.sch] = make([]string, 0)
				}
				// Use the file name, as the `path` variable is absolute
				ret[r.sch] = append(ret[r.sch], info.Name())
				return nil
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// BuildURL creates a URL that can be used to open the driver.
// This allows the driver to conform to the Open(url string) interface
func BuildURL(rootDir string, paths []string) string {

	q := nurl.Values{"path": paths}.Encode()
	return fmt.Sprintf("schematadriver://%s?%s", rootDir, q)
}
