package schemata_driver_two

import (
	"multimigrator/util"
	nurl "net/url"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

func init() {
	source.Register("schemata_driver", &SchemataDriver{})
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
