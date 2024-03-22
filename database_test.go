package sqlctrl

import (
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"
)

func TestNewDatabase(t *testing.T) {
	sqlDriver := "sqlite"
	sqlSource := "./test.db"
	sqlScheme := "./test.json"

	if _, err := os.Stat(sqlScheme); os.IsExist(err) { // if sqlScheme file already exist
		err = os.Remove(sqlScheme)
		if err != nil {
			t.Errorf("os.Remove(sqlScheme) error: %v", err)
			t.FailNow()
		}
	}

	db, err := NewDatabase(sqlDriver, sqlSource, sqlScheme)
	if err != nil {
		t.Errorf("Error to create NewDatabase: %v", err)
		t.FailNow()
	}

	if db.sqlDriver != sqlDriver {
		t.Errorf("db.sqlDriver != sqlDriver")
	}

	if db.sqlSource != sqlSource {
		t.Errorf("db.sqlSource != sqlSource")
	}

	if db.sqlScheme != sqlScheme {
		t.Errorf("db.sqlScheme != sqlScheme")
	}

	if _, err := os.Stat(sqlScheme); os.IsNotExist(err) { // if sqlScheme file does not exist
		t.Errorf("sqlScheme file is not created: %v", err)
		t.FailNow()
	}
}

func TestCheckExistTable(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"
	sqlScheme := "./test.json"

	db, err := NewDatabase(sqlDriver, sqlSource, sqlScheme)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if db.CheckExistTable(table) {
		t.Errorf("table already exist")
		t.FailNow()
	}

	err = db.CreateTable(table)
	if err != nil {
		t.Errorf("db.CreateTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		t.Errorf("table does not exist")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestCreateTable(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"
	sqlScheme := "./test.json"

	db, err := NewDatabase(sqlDriver, sqlSource, sqlScheme)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if db.CheckExistTable(table) {
		t.Errorf("table already exist")
		t.FailNow()
	}

	err = db.CreateTable(table)
	if err != nil {
		t.Errorf("db.CreateTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		t.Errorf("table does not exist")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}
