package multimigrator

import (
	"fmt"
	"os"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/assert"
)

type mockMigrator struct {
	indexInParent int
	cursor        int
	versions      []uint
	downstream    func(iv identifiedVersion)
}

func (mv *mockMigrator) Next(version uint) (nextVersion uint, err error) {

	for _, v := range mv.versions {
		if v > version {
			nextVersion = v
			return
		}
	}
	err = fmt.Errorf("no version is greater than %d (indexInParent: %d), %w", version, mv.indexInParent, os.ErrNotExist)
	return
}

func (mv *mockMigrator) Version() (version uint, dirty bool, err error) {
	if mv.cursor == -1 {
		return 0, false, migrate.ErrNilVersion
	}

	return mv.versions[mv.cursor], false, nil
}

func (mv *mockMigrator) Steps(n int) error {

	for range n {
		mv.cursor++
		mv.downstream(identifiedVersion{
			indexInParent: mv.indexInParent,
			version:       mv.versions[mv.cursor],
		})
	}
	return nil
}

type identifiedVersion struct {
	indexInParent int
	version       uint
}

type collector struct {
	identifiedVersions []identifiedVersion
}

func (c *collector) collect(iv identifiedVersion) {
	c.identifiedVersions = append(c.identifiedVersions, iv)
}

func newMockMigratorParts(versions [][]uint) (migratorParts, *collector) {

	c := &collector{identifiedVersions: make([]identifiedVersion, 0)}
	mp := migratorParts{}
	for i, v := range versions {
		mm := &mockMigrator{indexInParent: i, versions: v, cursor: -1, downstream: c.collect}
		mp = append(mp, &migratorPart{
			sourceDrv:    mm,
			instance:     mm,
			firstVersion: v[0],
		})
	}
	return mp, c
}

func TestApplyMigrations(t *testing.T) {

	type testCase struct {
		name string
		// The input versions (a numeric series)
		versions [][]uint
		expected []identifiedVersion
	}
	tcs := []testCase{
		{
			name:     "Dependent schema that begins with a later version is applied later",
			versions: [][]uint{{1, 2, 3}, {2, 3, 4}},
			expected: []identifiedVersion{
				{0, 1},
				{0, 2},
				{1, 2},
				{0, 3},
				{1, 3},
				{1, 4},
			},
		},
		{
			name:     "Dependent schema that begins after all earlier versions is applied correctly",
			versions: [][]uint{{1, 2, 3}, {4, 5, 6}, {4, 5, 6}},
			expected: []identifiedVersion{
				{0, 1},
				{0, 2},
				{0, 3},
				{1, 4},
				{2, 4},
				{1, 5},
				{2, 5},
				{1, 6},
				{2, 6},
			},
		},
		{
			name:     "Dependent schema with equal versions alternates",
			versions: [][]uint{{1, 2, 3}, {1, 2, 3}},
			expected: []identifiedVersion{
				{0, 1},
				{1, 1},
				{0, 2},
				{1, 2},
				{0, 3},
				{1, 3},
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			mp, c := newMockMigratorParts(tc.versions)
			err := mp.applyMigrations()
			assert.Nil(t, err)
			assert.Equal(t, tc.expected, c.identifiedVersions)
		})
	}
}
