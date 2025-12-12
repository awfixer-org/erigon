package app

import (
	"fmt"
	"io/fs"
	"iter"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	g "github.com/anacrolix/generics"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/go-quicktest/qt"
)

func TestResetChaindata(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		testResetLocalFs(t, []fsEntry{
			{Name: "chaindata/mdbx.dat"},
			{Name: "chaindata/mdbx.lck"},
		}, nil, qt.IsNil)
	})
	t.Run("DanglingSymlink", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata", Mode: fs.ModeSymlink, Data: "symlink"},
		}
		testResetLocalFs(t, startEntries, haveEntries(slices.Clone(startEntries)...), qt.IsNil)
	})
	t.Run("Symlinked", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata", Mode: fs.ModeSymlink, Data: "symlink"},
			{Name: "symlink", Mode: fs.ModeDir},
			{Name: "symlink/mdbx.dat"},
			{Name: "symlink/mdbx.lck"},
		}
		testResetLocalFs(t, startEntries, haveEntries(slices.Clone(startEntries[:2])...), qt.IsNil)
	})
	t.Run("LinkLoop", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata/a", Mode: fs.ModeSymlink, Data: "b"},
			{Name: "chaindata/b", Mode: fs.ModeSymlink, Data: "a"},
		}
		testResetLocalFs(t, startEntries, haveEntries(startEntries...), qt.IsNotNil)
	})
	t.Run("CheapDiskExample", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata", Mode: fs.ModeSymlink, Data: "fastdisk"},
			{Name: "snapshots", Mode: fs.ModeDir},
			{Name: "symlink/mdbx.dat"},
			{Name: "symlink/mdbx.lck"},
		}
		testResetLocalFs(t, startEntries, haveEntries(slices.Clone(startEntries[:2])...), qt.IsNil)
	})
}

func TestResetEscapeRootAbsoluteSymlink(t *testing.T) {
	const fastDiskName = "fastdisk"
	startEntries := []fsEntry{
		{Name: "bystander"},
		{Name: "datadir/chaindata", Mode: fs.ModeSymlink, Data: path.Join("..", fastDiskName)},
		{Name: fastDiskName, Mode: fs.ModeDir},
		{Name: "datadir/snapshots", Mode: fs.ModeDir},
		{Name: "datadir/snapshots/domain", Mode: fs.ModeSymlink, Data: "/fastdisk"},
		{Name: "datadir/snapshots/history", Mode: fs.ModeDir},
		{Name: "datadir/heimdall/mystuff"},
	}
	endEntries := append(
		slices.Clone(startEntries[:5]),
		fsEntry{Name: ".", Mode: fs.ModeDir})
	withOsRoot(t, func(osRoot *os.Root) {
		makeEntries(t, startEntries, osRoot)
		rootFS := osRoot.FS()
		printFs(t, rootFS)
		r := makeTestingReset(t)
		r.datadirPath = "datadir"
		r.fs = rootFS
		r.removeFunc = osRoot.Remove
		qt.Assert(t, qt.ErrorMatches(r.run(), pathEscapesFromParent))
		checkFs(t, rootFS, haveEntries(endEntries...)...)
	})
}
func TestResetCheapDiskExample(t *testing.T) {
	withOsRoot(t, func(osRoot *os.Root) {
		absFastDiskLink, err := filepath.Abs(filepath.Join(osRoot.Name(), "fastdisk"))
		qt.Assert(t, qt.IsNil(err))
		t.Log("absolute fastdisk path:", absFastDiskLink)
		startEntries := []fsEntry{
			{Name: "bystander"},
			{Name: "datadir/chaindata", Mode: fs.ModeSymlink, Data: "../fastdisk"},
			{Name: "fastdisk", Mode: fs.ModeDir},
			{Name: "datadir/snapshots", Mode: fs.ModeDir},
			{Name: "datadir/snapshots/domain", Mode: fs.ModeSymlink, Data: absFastDiskLink},
			{Name: "datadir/snapshots/history", Mode: fs.ModeDir},
			{Name: "datadir/heimdall/mystuff"},
		}
		endEntries := append(
			slices.Clone(startEntries[:5]),
			fsEntry{Name: ".", Mode: fs.ModeDir})
		makeEntries(t, startEntries, osRoot)
		rootFS := osRoot.FS()
		printFs(t, rootFS)
		r := makeTestingReset(t)
		r.datadirPath = "datadir"
		r.fs = rootFS
		r.removeFunc = osRoot.Remove
		qt.Assert(t, qt.IsNil(r.run()))
		checkFs(t, rootFS, haveEntries(endEntries...)...)
	})
}

func TestResetSymlinkEscapes(t *testing.T) {
	startEntries := []fsEntry{
		{Name: "jail/snapshots/badlink", Mode: fs.ModeSymlink, Data: "../../escape"},
		{Name: "escape"},
	}
	endEntries := slices.Clone(startEntries)
	withOsRoot(t, func(osRoot *os.Root) {
		makeEntries(t, startEntries, osRoot)
		resetJail, err := osRoot.OpenRoot("jail")
		qt.Assert(t, qt.IsNil(err))
		rootFS := osRoot.FS()
		printFs(t, rootFS)
		r := makeTestingReset(t)
		r.datadirPath = "."
		r.fs = resetJail.FS()
		r.removeFunc = resetJail.Remove
		err = r.run()
		qt.Check(t, qt.ErrorMatches(err, pathEscapesFromParent))
		checkFs(t, rootFS, haveEntries(endEntries...)...)
	})
}

var pathEscapesFromParent = regexp.MustCompile("path escapes from parent$")

type fsEntry struct {
	Name string
	Data string
	Mode fs.FileMode
}

func (me fsEntry) readData(fsys fs.FS) (string, error) {
	switch mt := me.Mode.Type(); mt {
	case fs.ModeSymlink:
		return fs.ReadLink(fsys, me.Name)
	case 0:
		b, err := fs.ReadFile(fsys, me.Name)
		return string(b), err
	case fs.ModeDir:
		return "", nil
	default:
		return "", fmt.Errorf("unhandled file type %v", mt)
	}
}

func withOsRoot(t *testing.T, with func(root *os.Root)) {
	osRoot, err := os.OpenRoot(t.TempDir())
	qt.Assert(t, qt.IsNil(err))
	t.Log("rootfs is at", osRoot.Name())
	defer func() {
		if t.Failed() {
			t.Log("fs state at failure")
			printFs(t, os.DirFS(osRoot.Name()))
		}
	}()
	with(osRoot)
}

func testResetLocalFs(t *testing.T, startEntries []fsEntry, checkers []fsChecker, runChecker func(error) qt.Checker) {
	withOsRoot(t, func(osRoot *os.Root) {
		makeEntries(t, startEntries, osRoot)
		rootFS := os.DirFS(osRoot.Name())
		printFs(t, rootFS)
		r := makeTestingReset(t)
		r.fs = rootFS
		r.removeFunc = osRoot.Remove
		qt.Assert(t, runChecker(r.run()))
		checkFs(t, rootFS, append(
			checkers,
			// We're running on a temp dir that always exists. Reset doesn't remove the top-level
			// datadir.
			haveEntry(fsEntry{Name: ".", Mode: fs.ModeDir}))...)
	})
}

func parentNames(name string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for {
			name = path.Dir(name)
			if name == "" {
				return
			}
			if !yield(name) {
				return
			}
			if name == "." {
				return
			}
		}
	}
}

func haveEntries(entries ...fsEntry) (ret []fsChecker) {
	byName := make(map[string]fsChecker)
	for _, e := range entries {
		g.MapMustAssignNew(byName, e.Name, haveEntry(e))
	}
	for _, e := range entries {
		for name := range parentNames(e.Name) {
			if g.MapContains(byName, name) {
				continue
			}
			g.MapMustAssignNew(byName, name, haveEntry(fsEntry{Name: name, Mode: fs.ModeDir}))
		}
	}
	return slices.Collect(maps.Values(byName))
}

func haveEntry(entry fsEntry) fsChecker {
	name := entry.Name
	checkErr := fs.ErrNotExist
	return fsCheckerFuncs{
		onWalkDir: func(input fsCheckerWalkInput) (stop bool, err error) {
			if input.name != name {
				return
			}
			stop = true
			data, err := entry.readData(input.fs)
			if err != nil {
				return
			}
			if data == entry.Data {
				checkErr = nil
			} else {
				checkErr = fmt.Errorf("wrong data, expected %q, got %q", entry.Data, data)
			}
			return
		},
		check: func(t *testing.T) {
			if checkErr != nil {
				t.Errorf("entry %q failed check: %v", name, checkErr)
			}
		},
	}
}

func printFs(t *testing.T, rootFS fs.FS) {
	qt.Assert(t, qt.IsNil(fs.WalkDir(rootFS, ".", func(path string, d fs.DirEntry, err error) error {
		t.Logf("path: %q, mode: %v, err: %v\n", path, d.Type(), err)
		return nil
	})))
}

type fsCheckerFuncs struct {
	onWalkDir fsCheckerWalkFunc
	check     func(t *testing.T)
}

func (f fsCheckerFuncs) OnWalkDir(input fsCheckerWalkInput) (bool, error) {
	return f.onWalkDir(input)
}

func (f fsCheckerFuncs) Check(t *testing.T) {
	f.check(t)
}

type fsCheckerWalkFunc = func(input fsCheckerWalkInput) (bool, error)

type fsChecker interface {
	// Return early with error if return false
	OnWalkDir(input fsCheckerWalkInput) (bool, error)
	// Tell us if you pass
	Check(t *testing.T)
}

type fsCheckerWalkInput struct {
	name string
	d    fs.DirEntry
	err  error
	fs   fs.FS
}

func checkFs(t *testing.T, fsRoot fs.FS, checkers ...fsChecker) {
	qt.Assert(t, qt.IsNil(fs.WalkDir(
		fsRoot,
		".",
		func(path string, d fs.DirEntry, err error) error {
			println("checkFs", path, d, err)
			for _, c := range checkers {
				stop, err := c.OnWalkDir(fsCheckerWalkInput{
					name: path,
					d:    d,
					err:  err,
					fs:   fsRoot,
				})
				// Maybe this is t.Errorf is stop is false?
				if err != nil {
					return err
				}
				if stop {
					return nil
				}
			}
			t.Errorf("unexpected path %q", path)
			return nil
		},
	)))
	for _, checker := range checkers {
		checker.Check(t)
	}
}

func makeEntries(t *testing.T, entries []fsEntry, root *os.Root) {
	assertNoErr := func(err error) {
		qt.Assert(t, qt.IsNil(err))
	}
	for _, entry := range entries {
		localName, err := filepath.Localize(entry.Name)
		qt.Assert(t, qt.IsNil(err), qt.Commentf("localizing entry name %q", entry.Name))
		root.MkdirAll(filepath.Dir(localName), dir.DirPerm)
		if entry.Mode&fs.ModeSymlink != 0 {
			assertNoErr(root.Symlink(entry.Data, localName))
		} else if entry.Mode&fs.ModeDir != 0 {
			assertNoErr(root.Mkdir(localName, dir.DirPerm))
		} else {
			f, err := root.Create(localName)
			assertNoErr(err)
			f.Close()
		}
	}
	return
}

func makeTestingReset(t *testing.T) reset {
	logger := log.New( /*"test", t.Name()*/ )
	logger.SetHandler(log.StdoutHandler)
	return reset{
		logger:               logger,
		preverifiedSnapshots: nil,
		removeUnknown:        true,
		removeLocal:          true,
		linkLimit:            3,
	}
}
