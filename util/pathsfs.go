package util

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

var ErrInvalidRoot = errors.New("invalid root directory")

type pathsFS struct {
	root       string
	names      map[string]fs.DirEntry
	namesSlice []fs.DirEntry
}

func PathsFS(root string, paths []string) (fs.ReadDirFS, error) {

	if root == "" {
		return nil, ErrInvalidRoot
	}
	canonicalRoot := root
	if !filepath.IsAbs(root) {
		var err error
		canonicalRoot, err = filepath.Abs(canonicalRoot)
		if err != nil {
			return nil, err
		}
	}
	names := make(map[string]fs.DirEntry)
	namesSlice := make([]fs.DirEntry, 0)
	for _, p := range paths {
		path := filepath.Join(canonicalRoot, p)
		stat, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		finfo := &finfoWrapper{stat}
		names[path] = finfo
		namesSlice = append(namesSlice, finfo)
	}

	return &pathsFS{root: canonicalRoot, names: names, namesSlice: namesSlice}, nil
}

type finfoWrapper struct {
	stat os.FileInfo
}

func (f *finfoWrapper) Name() string {
	return f.stat.Name()
}

func (f *finfoWrapper) IsDir() bool {
	return f.stat.IsDir()
}

func (f *finfoWrapper) Type() fs.FileMode {
	return f.stat.Mode()
}

func (f *finfoWrapper) Info() (fs.FileInfo, error) {
	return f.stat, nil
}

// Open opens the named file.
//
// When Open returns an error, it should be of type *PathError
// with the Op field set to "open", the Path field set to name,
// and the Err field describing the problem.
//
// Open should reject attempts to open names that do not satisfy
// ValidPath(name), returning a *PathError with Err set to
// ErrInvalid or ErrNotExist.
func (p *pathsFS) Open(name string) (fs.File, error) {
	name, err := p.validate(name)
	if err != nil {
		return nil, err
	}

	return os.Open(name)
}

// ReadDir reads the named directory
// and returns a list of directory entries sorted by filename.
func (p *pathsFS) ReadDir(name string) ([]fs.DirEntry, error) {

	name, err := p.validate(name)
	if err != nil {
		return nil, err
	}

	if name == p.root {
		return p.namesSlice, nil
	}

	return os.ReadDir(name)
}

func (p *pathsFS) validate(name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}

	if !filepath.IsAbs(name) {
		var err error
		name, err = filepath.Abs(filepath.Join(p.root, name))
		if err != nil {
			return "", err
		}
	}

	if _, ok := p.names[name]; !ok {
		if name != p.root {
			return "", &fs.PathError{
				Op:   "open",
				Path: name,
				Err:  fs.ErrNotExist,
			}
		}
	}

	return name, nil
}
