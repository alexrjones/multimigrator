package schemata_driver

import (
	"errors"
	"io"
	"io/fs"
	"multimigrator/internal"
	nurl "net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/golang-migrate/migrate/v4/source"
)

type SchemataFile struct {
	migrations *source.Migrations
	fsys       fs.FS
	url        string
	path       string
}

// Init prepares not initialized IoFS instance to read migrations from a
// io/fs#FS instance and a relative path.
func (sf *SchemataFile) Init(description internal.DatabaseDescription, fsys fs.FS, path string) error {
	entries, err := fs.ReadDir(fsys, path)
	if err != nil {
		return err
	}

	p, err := newParser(description.Ordering)
	if err != nil {
		return err
	}

	ms := source.NewMigrations()
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m, err := p.Parse(e.Name())
		if err != nil {
			continue
		}
		file, err := e.Info()
		if err != nil {
			return err
		}
		if !ms.Append(m) {
			return source.ErrDuplicateMigration{
				Migration: *m,
				FileInfo:  file,
			}
		}
	}

	sf.fsys = fsys
	sf.path = path
	sf.migrations = ms
	return nil
}

// Close is part of source.Driver interface implementation.
// Closes the file system if possible.
func (sf *SchemataFile) Close() error {
	c, ok := sf.fsys.(io.Closer)
	if !ok {
		return nil
	}
	return c.Close()
}

// First is part of source.Driver interface implementation.
func (sf *SchemataFile) First() (version uint, err error) {
	if version, ok := sf.migrations.First(); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "first",
		Path: sf.path,
		Err:  fs.ErrNotExist,
	}
}

// Prev is part of source.Driver interface implementation.
func (sf *SchemataFile) Prev(version uint) (prevVersion uint, err error) {
	if version, ok := sf.migrations.Prev(version); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "prev for version " + strconv.FormatUint(uint64(version), 10),
		Path: sf.path,
		Err:  fs.ErrNotExist,
	}
}

// Next is part of source.Driver interface implementation.
func (sf *SchemataFile) Next(version uint) (nextVersion uint, err error) {
	if version, ok := sf.migrations.Next(version); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "next for version " + strconv.FormatUint(uint64(version), 10),
		Path: sf.path,
		Err:  fs.ErrNotExist,
	}
}

// ReadUp is part of source.Driver interface implementation.
func (sf *SchemataFile) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := sf.migrations.Up(version); ok {
		body, err := sf.open(path.Join(sf.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &fs.PathError{
		Op:   "read up for version " + strconv.FormatUint(uint64(version), 10),
		Path: sf.path,
		Err:  fs.ErrNotExist,
	}
}

// ReadDown is part of source.Driver interface implementation.
func (sf *SchemataFile) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := sf.migrations.Down(version); ok {
		body, err := sf.open(path.Join(sf.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &fs.PathError{
		Op:   "read down for version " + strconv.FormatUint(uint64(version), 10),
		Path: sf.path,
		Err:  fs.ErrNotExist,
	}
}

func (sf *SchemataFile) open(path string) (fs.File, error) {
	f, err := sf.fsys.Open(path)
	if err == nil {
		return f, nil
	}
	// Some non-standard file systems may return errors that don't include the path, that
	// makes debugging harder.
	if !errors.As(err, new(*fs.PathError)) {
		err = &fs.PathError{
			Op:   "open",
			Path: path,
			Err:  err,
		}
	}
	return nil, err
}

func (sf *SchemataFile) Open(url string) (source.Driver, error) {

	return nil, errors.New("not supported")
}

func (sf *SchemataFile) OpenWithDescription(url string, description internal.DatabaseDescription) (source.Driver, error) {
	p, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	nf := &SchemataFile{
		url:  url,
		path: p,
	}
	if err := nf.Init(description, os.DirFS(p), "."); err != nil {
		return nil, err
	}
	return nf, nil
}

func parseURL(url string) (string, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return "", err
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
			return "", err
		}
		p = wd

	} else if p[0:1] == "." || p[0:1] != "/" {
		// make path absolute if relative
		abs, err := filepath.Abs(p)
		if err != nil {
			return "", err
		}
		p = abs
	}
	return p, nil
}
