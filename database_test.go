package sqlctrl

import (
	"database/sql"
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

func TestDropTable(t *testing.T) {
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

	if db.CheckExistTable(table) {
		t.Errorf("table already exist")
		t.FailNow()
	}
}

func TestGetCount(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	count, err := db.GetCount(table)
	if err != nil {
		t.Errorf("db.GetCount error: %v", err)
		t.FailNow()
	}

	if count != 3 {
		t.Errorf("wrong count")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestGetLastId(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	lastId, err := db.GetLastId(table)
	if err != nil {
		t.Errorf("db.GetLastId error: %v", err)
		t.FailNow()
	}

	if lastId != 3 {
		t.Errorf("wrong lastId")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestSelectValue(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValue(table, "ParamA = 3")
	if err != nil {
		t.Errorf("db.SelectValue error: %v", err)
		t.FailNow()
	}

	responseArray, ok := responseIface.([]TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to []TestTable")
		t.FailNow()
	}

	if responseArray[0].ParamA != 3 {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestSelectValueSingle(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValueSingle(table, "ParamA = 3")
	if err != nil {
		t.Errorf("db.SelectValueSingle error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestSelectValueById(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValueById(table, 3)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestInsertValue(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	_, err = db.InsertValue(table, TestTable{
		ParamB: "d",
	})
	if err != nil {
		t.Errorf("db.InsertValue single error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValueById(table, 4)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 4 {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestReplaceValue(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	err = db.ReplaceValue(table, []TestTable{
		{ParamA: 1, ParamB: "a1"},
		{ParamA: 2, ParamB: "b1"},
		{ParamA: 3, ParamB: "c1"},
	})
	if err != nil {
		t.Errorf("db.ReplaceValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValueById(table, 3)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 || response.ParamB != "c1" {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestUpdateValue(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	err = db.UpdateValue(table, []TestTable{
		{ParamA: 1, ParamB: "a1"},
		{ParamA: 2, ParamB: "b1"},
		{ParamA: 3, ParamB: "c1"},
	})
	if err != nil {
		t.Errorf("db.UpdateValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValueById(table, 3)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 || response.ParamB != "c1" {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestDeleteValue(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	err = db.DeleteValue(table, []TestTable{
		{ParamA: 1, ParamB: "a1"},
		{ParamA: 2, ParamB: "b1"},
		{ParamA: 3, ParamB: "c1"},
	})
	if err != nil {
		t.Errorf("db.DeleteValue error: %v", err)
		t.FailNow()
	}

	count, err := db.GetCount(table)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	if count != 0 {
		t.Errorf("count != 0")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestQuery(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.Query(table, "select * from test_table where paramA = 3")
	if err != nil {
		t.Errorf("db.Query error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.([]TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to []TestTable")
		t.FailNow()
	}

	if response[0].ParamA != 3 || response[0].ParamB != "c" {
		t.Errorf("wrong item selected")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestQuerySingle(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.QuerySingle(table, "select * from test_table where paramA = 3")
	if err != nil {
		t.Errorf("db.QuerySingle error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 || response.ParamB != "c" {
		t.Errorf("wrong item selected")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestQueryWithTable(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.QueryWithTable(table, "select * from test_table where paramA = 3")
	if err != nil {
		t.Errorf("db.QueryWithTable error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.([]TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to []TestTable")
		t.FailNow()
	}

	if response[0].ParamA != 3 || response[0].ParamB != "c" {
		t.Errorf("wrong item selected")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestQuerySingleWithTable(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.QuerySingleWithTable(table, "select * from test_table where paramA = 3")
	if err != nil {
		t.Errorf("db.QuerySingleWithTable error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 || response.ParamB != "c" {
		t.Errorf("wrong item selected")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestExec(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	err = db.Exec(func(db *DataBase, tx *sql.Tx) error {
		rows, err := tx.Query("select * from test_table where paramA = 3")
		if err != nil {
			return err
		}

		for rows.Next() {
			test := TestTable{}
			err = rows.Scan(&test.ParamA, &test.ParamB)
			if err != nil {
				return err
			}

			if test.ParamA != 3 || test.ParamB != "c" {
				t.Errorf("wrong value received")
				t.FailNow()
			}
		}

		return nil
	})
	if err != nil {
		t.Errorf("db.Exec error: %v", err)
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestExecWithTable(t *testing.T) {
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

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	err = db.ExecWithTable(table, func(db *DataBase, tx *sql.Tx, table *Table) error {
		rows, err := tx.Query("select * from test_table where paramA = 3")
		if err != nil {
			return err
		}

		for rows.Next() {
			test := TestTable{}
			err = rows.Scan(&test.ParamA, &test.ParamB)
			if err != nil {
				return err
			}

			if test.ParamA != 3 || test.ParamB != "c" {
				t.Errorf("wrong value received")
				t.FailNow()
			}
		}

		return nil
	})
	if err != nil {
		t.Errorf("db.ExecWithTable error: %v", err)
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}
