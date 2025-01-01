package main

import (
	"crypto"
	"embed"
	"encoding/hex"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mpetavy/common"
	"github.com/mpetavy/common/sqldb"
	"io"
	"os"
	"sync"
)

var (
	flagDbFilename *string
	flagTruncate   = flag.Bool("t", false, "truncate database")
	flagSource     = flag.String("s", "", "Source")
	flagQuery      = flag.String("q", "", "SQL query")
	flagInclude    = flag.String("i", "", "include a directory scan to database")

	db *sqldb.SqlDb
)

//go:embed go.mod
var resources embed.FS

func init() {
	common.Init("", "", "", "", "picsort", "", "", "", &resources, nil, nil, run, 0)

	common.Events.AddListener(common.EventInit{}, func(ev common.Event) {
		flagDbFilename = flag.String("db", common.AppFilename(".db"), "database name")
	})
}

func md5(filename string) (string, error) {
	common.DebugFunc(filename)

	md5 := crypto.MD5.New()

	f, err := os.Open(filename)
	if common.Error(err) {
		return "", err
	}

	defer func() {
		common.Error(f.Close())
	}()

	_, err = io.Copy(md5, f)
	if common.Error(err) {
		return "", err
	}

	fingerprint := md5.Sum(nil)

	hash := hex.EncodeToString(fingerprint)

	common.DebugFunc("%s: %s", filename, hash)

	return hash, nil
}

func processFile(source string, fileName string, fileInfo os.FileInfo) error {
	common.DebugFunc(fileName)

	if fileName == *flagDbFilename {
		return nil
	}

	hash, err := md5(fileName)
	if common.Error(err) {
		return err
	}

	_, err = db.Execute("insert into files (source,path,hash) values (?,?,?)", source, fileName, hash)
	if common.Error(err) {
		return err
	}

	return nil
}

func processDir(wg *sync.WaitGroup, source string, dir string) error {
	err := common.WalkFiles(dir, true, true, func(filename string, fi os.FileInfo) error {
		if fi.IsDir() {
			return nil
		}

		wg.Add(1)

		go func() {
			defer common.UnregisterGoRoutine(common.RegisterGoRoutine(1))
			defer func() {
				wg.Done()
			}()

			common.Error(processFile(source, filename, fi))
		}()

		return nil
	})
	if common.Error(err) {
		return err
	}

	return nil
}

func createDb(filename string) (*sqldb.SqlDb, error) {
	isNew := !common.FileExists(filename)
	if !isNew && *flagTruncate {
		err := common.FileDelete(filename)
		if common.Error(err) {
			return nil, err
		}

		isNew = true
	}

	db, err := sqldb.NewSqlDb("sqlite3", fmt.Sprintf("file:%s?cache=shared", *flagDbFilename))
	if common.Error(err) {
		return nil, err
	}

	if isNew {
		_, err := db.Execute("create table if not exists files (id integer not null primary key, source text,path text, hash text)")
		if common.Error(err) {
			return nil, err
		}
	}

	return db, err
}

func query() error {
	rs, err := db.Query(*flagQuery)

	if common.Error(err) {
		return err
	}

	st := &common.StringTable{}

	st.AddCols(rs.ColumnNames...)

	for row := range rs.RowCount {
		fieldValues := []string{}
		for col := range len(rs.ColumnNames) {
			fieldValues = append(fieldValues, rs.FieldByIndex(row, col).String())
		}

		st.AddCols(fieldValues...)
	}

	fmt.Printf("%s\n", st.Table())

	return nil
}

func run() error {
	var err error

	db, err = createDb(*flagDbFilename)
	if common.Error(err) {
		return err
	}

	defer func() {
		common.Error(db.Close())
	}()

	if *flagQuery != "" {
		err := query()
		if common.Error(err) {
			return err
		}

		return nil
	}

	if *flagInclude != "" {
		if !common.FileExists(*flagInclude) {
			return fmt.Errorf("file not found: %s", *flagInclude)
		}

		if *flagSource == "" {
			return fmt.Errorf("undefined source: %s", *flagSource)
		}

		wg := sync.WaitGroup{}

		err := processDir(&wg, *flagSource, *flagInclude)
		if common.Error(err) {
			return err
		}

		wg.Wait()
	}

	return nil
}

func main() {
	common.Run([]string{"db", "i|q"})
}
