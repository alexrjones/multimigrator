package util

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

var ErrInvalidRoot = errors.New("invalid root directory")

type pathsFS struct {
	root  string
	names map[string]fs.DirEntry
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
	for _, p := range paths {
		path := filepath.Join(canonicalRoot, p)
		stat, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		names[p] = &finfoWrapper{stat}
	}

	return &pathsFS{root: canonicalRoot, names: names}, nil
}

type finfoWrapper struct {
	stat os.FileInfo
}

func (f *finfoWrapper) Name() string {
	return f.Name()
}

func (f *finfoWrapper) IsDir() bool {
	return f.IsDir()
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
	if err := checkOk(name, p.names); err != nil {
		return nil, err
	}

	return os.Open(filepath.Join(p.root, name))
}

// ReadDir reads the named directory
// and returns a list of directory entries sorted by filename.
func (p *pathsFS) ReadDir(name string) ([]fs.DirEntry, error) {

	if err := checkOk(name, p.names); err != nil {
		return nil, err
	}

	return os.ReadDir(filepath.Join(p.root, name))
}

func checkOk(name string, names map[string]fs.DirEntry) error {
	if !fs.ValidPath(name) {
		return &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}
	if _, ok := names[name]; !ok {
		return &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fs.ErrNotExist,
		}
	}

	return nil
}
