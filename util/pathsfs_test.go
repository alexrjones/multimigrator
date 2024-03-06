package util

import (
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	assert "github.com/stretchr/testify/require"
)

func TestPathsFS_ReadFile_Ok(t *testing.T) {

	allowedFiles := []string{"1.txt", "2.txt"}
	fs, err := PathsFS("../testdata/util/pathsfs", allowedFiles)
	assert.Nil(t, err)
	checkOkFile := func(name string) {
		f, err := fs.Open(name)
		assert.Nil(t, err)
		defer f.Close()
		b, err := io.ReadAll(f)
		assert.Nil(t, err)

		expectedText, _, found := strings.Cut(name, ".")
		assert.True(t, found)
		assert.Equal(t, expectedText+"\n", string(b))
	}
	for _, n := range allowedFiles {
		checkOkFile(n)
	}
}

func TestPathsFS_ReadFile_Error(t *testing.T) {

	var allowedFiles []string
	fs, err := PathsFS("../testdata/util/pathsfs", allowedFiles)
	assert.Nil(t, err)
	_, err = fs.Open("1.txt")
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestPathsFS_ReadDir_Relative(t *testing.T) {

	allowedFiles := []string{"1.txt"}
	fs, err := PathsFS("../testdata/util/pathsfs", allowedFiles)
	assert.Nil(t, err)

	type testCase struct{ path string }
	tcs := []testCase{
		{"."},
	}
	for _, tc := range tcs {
		t.Run(tc.path, func(t *testing.T) {
			dir, err := fs.ReadDir(tc.path)
			assert.Nil(t, err)
			assert.Len(t, dir, 1)
			assert.Equal(t, "1.txt", dir[0].Name())
		})
	}
}

func TestPathsFS_ReadDir_Ok(t *testing.T) {

	allowedFiles := []string{"nested"}
	fs, err := PathsFS("../testdata/util/pathsfs", allowedFiles)
	assert.Nil(t, err)
	dir, err := fs.ReadDir("nested")
	assert.Nil(t, err)
	for i, e := range dir {
		assert.Equal(t, strconv.Itoa(i+1)+".txt", e.Name())
	}
}

func TestPathsFS_ReadDir_Error(t *testing.T) {

	var allowedFiles []string
	fs, err := PathsFS("../testdata/util/pathsfs", allowedFiles)
	assert.Nil(t, err)
	_, err = fs.ReadDir("nested_disallowed")
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)
}
