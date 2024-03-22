package sqlctrl

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"reflect"
	"regexp"
	"strings"
)

//--------------------------------------------------------------------------------//

type DataBase struct {
	mutex chan interface{}

	sqlDriver string
	sqlSource string
	sqlScheme string
	sqlRaw    *sql.DB

	schemeMutex chan interface{}
	scheme      map[string]*Table
}

//--------------------------------------------------------------------------------//
// scheme control
//--------------------------------------------------------------------------------//

func (db *DataBase) schemeImport() error {
	db.schemeMutex <- true
	defer func() {
		<-db.schemeMutex
	}()

	schemeByte, err := os.ReadFile(db.sqlScheme)
	if err != nil {
		return err
	}

	scheme := map[string]*Table{}
	err = json.Unmarshal(schemeByte, &scheme)
	if err != nil {
		return err
	}

	db.scheme = scheme
	return nil
}

func (db *DataBase) schemeExport() error {
	db.schemeMutex <- true
	defer func() {
		<-db.schemeMutex
	}()

	schemeByte, err := json.MarshalIndent(db.scheme, "", "\t")
	if err != nil {
		return err
	}

	err = os.WriteFile(db.sqlScheme, schemeByte, 0666)
	if err != nil {
		return err
	}

	return nil
}

func (db *DataBase) CheckExistTable(table *Table) bool {
	db.schemeMutex <- true
	schemeTable := db.scheme[table.GoName]
	<-db.schemeMutex

	return schemeTable != nil
}

func (db *DataBase) CheckHashTable(table *Table) bool {
	db.schemeMutex <- true
	schemeTable := db.scheme[table.GoName]
	<-db.schemeMutex

	return schemeTable.GetHash() != table.GetHash()
}

//--------------------------------------------------------------------------------//
// DataBase control
//--------------------------------------------------------------------------------//

func (db *DataBase) Query(table *Table, request string) (response interface{}, err error) {
	var (
		sqlTx         *sql.Tx
		sqlRows       *sql.Rows
		responseArray reflect.Value

		structPtr     interface{}
		fieldArrayPtr []interface{}
	)

	db.mutex <- true
	defer func() {
		<-db.mutex
	}()

	sqlTx, err = db.sqlRaw.Begin()
	if err != nil {
		return
	}
	defer sqlTx.Rollback()

	sqlRows, err = sqlTx.Query(request)
	if err != nil {
		return
	}

	responseArray = reflect.MakeSlice(reflect.SliceOf(table.GoType), 0, 0)

	for sqlRows.Next() {
		structPtr, fieldArrayPtr, err = table.GetStruct(nil)
		if err != nil {
			return
		}

		err = sqlRows.Scan(fieldArrayPtr...)
		if err != nil {
			return
		}

		responseArray = reflect.Append(responseArray, reflect.ValueOf(structPtr).Elem())
	}

	err = sqlTx.Commit()
	if err != nil {
		return
	}

	response = responseArray.Interface()

	return
}

func (db *DataBase) QuerySingle(table *Table, request string) (response interface{}, err error) {
	var (
		responseArray             interface{}
		responseArrayReflectValue reflect.Value
	)

	responseArray, err = db.Query(table, request)
	if err != nil {
		return
	}

	responseArrayReflectValue = reflect.ValueOf(responseArray).Convert(reflect.SliceOf(table.GoType))

	switch responseArrayReflectValue.Len() {
	case 0:
		err = ErrResponseLessThanRequested
		return
	case 1:
	default:
		err = ErrResponseMoreThanRequested
		return
	}

	response = responseArrayReflectValue.Index(0).Interface()

	return
}

func (db *DataBase) QueryWithTable(table *Table, request string) (response interface{}, err error) {
	if !db.CheckExistTable(table) {
		return nil, ErrTableDoesNotExists
	}

	if db.CheckHashTable(table) {
		return nil, ErrTableDoesNotMigtated
	}

	return db.Query(table, request)
}

func (db *DataBase) QuerySingleWithTable(table *Table, request string) (response interface{}, err error) {
	if !db.CheckExistTable(table) {
		return nil, ErrTableDoesNotExists
	}

	if db.CheckHashTable(table) {
		return nil, ErrTableDoesNotMigtated
	}

	return db.QuerySingle(table, request)
}

func (db *DataBase) Exec(handler func(*DataBase, *sql.Tx) error) (err error) {
	var (
		sqlTx *sql.Tx
	)

	db.mutex <- true
	defer func() {
		<-db.mutex
	}()

	sqlTx, err = db.sqlRaw.Begin()
	if err != nil {
		return
	}
	defer sqlTx.Rollback()

	err = handler(db, sqlTx)
	if err != nil {
		return
	}

	err = sqlTx.Commit()
	if err != nil {
		return
	}

	return
}

func (db *DataBase) ExecWithTable(table *Table, handler func(*DataBase, *sql.Tx, *Table) error) (err error) {
	if !db.CheckExistTable(table) {
		return ErrTableDoesNotExists
	}

	if db.CheckHashTable(table) {
		return ErrTableDoesNotMigtated
	}

	err = db.Exec(func(db *DataBase, sqlTx *sql.Tx) error {
		return handler(db, sqlTx, table)
	})

	return
}

//--------------------------------------------------------------------------------//
// Table control
//--------------------------------------------------------------------------------//

func (db *DataBase) NewTable(tableName string, tableStruct interface{}) (*Table, error) {
	var (
		table *Table
		err   error
	)

	table, err = NewTable(tableName, tableStruct)
	if err != nil {
		return nil, err
	}

	err = db.MigrationTable(table, nil)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (db *DataBase) sqlCreateTable(table *Table) (request []string, err error) {
	request, err = table.sqlCreateTable()
	if err != nil {
		return
	}

	for i, v := range request {
		switch db.sqlDriver {
		case "sqlite", "sqlite3":
			v = regexp.MustCompile("INTEGER.*? PRIMARY_KEY AUTO_INCREMENT").ReplaceAllString(v, "INTEGER PRIMARY_KEY AUTO_INCREMENT")

			v = strings.Replace(v, "PRIMARY_KEY", "PRIMARY KEY", -1)
			v = strings.Replace(v, "AUTO_INCREMENT", "AUTOINCREMENT", -1)
			v = strings.Replace(v, "NOT_NULL", "NOT NULL", -1)
		case "mysql":
			v = strings.Replace(v, "PRIMARY_KEY", "PRIMARY KEY", -1)
			v = strings.Replace(v, "AUTO_INCREMENT", "AUTO_INCREMENT", -1)
			v = strings.Replace(v, "NOT_NULL", "NOT NULL", -1)
		}

		request[i] = v
	}

	return
}

func (db *DataBase) CreateTable(table *Table) error {
	if db.CheckExistTable(table) {
		return ErrTableAlreadyExists
	}

	requestArray, err := db.sqlCreateTable(table)
	if err != nil {
		return err
	}

	err = db.Exec(func(db *DataBase, sqlTx *sql.Tx) (err error) {
		for _, request := range requestArray {
			_, err = sqlTx.Exec(request)
			if err != nil {
				return
			}
		}

		return
	})

	if err != nil {
		return err
	}

	db.schemeMutex <- true
	db.scheme[table.GoName] = table
	<-db.schemeMutex

	return db.schemeExport()
}

func (db *DataBase) DropTable(table *Table) error {
	if !db.CheckExistTable(table) {
		return ErrTableDoesNotExists
	}

	requestArray := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", table.SqlName),
	}

	err := db.Exec(func(db *DataBase, sqlTx *sql.Tx) (err error) {
		for _, request := range requestArray {
			_, err = sqlTx.Exec(request)
			if err != nil {
				return
			}
		}

		return
	})

	if err != nil {
		return err
	}

	db.schemeMutex <- true
	delete(db.scheme, table.GoName)
	<-db.schemeMutex

	return db.schemeExport()
}

func MigrationTableAuto(tableA, tableB *Table) (string, error) {
	fieldNameMap := map[string]int{}

	for _, fieldName := range tableA.FieldNameArray {
		fieldNameMap[fieldName]++
	}

	for _, fieldName := range tableB.FieldNameArray {
		fieldNameMap[fieldName]++
	}

	fieldANameArray := []string{}
	fieldBNameArray := []string{}

	for fieldName, fieldCount := range fieldNameMap {
		if fieldCount == 2 {
			tableAField := tableA.FieldMap[fieldName]
			tableBField := tableB.FieldMap[fieldName]

			fieldANameArray = append(fieldANameArray, fmt.Sprintf("%s AS %s", tableAField.SqlName, tableBField.SqlName))
			fieldBNameArray = append(fieldBNameArray, tableBField.SqlName)
		}
	}

	if len(fieldANameArray) == 0 {
		return "", fmt.Errorf("auto migration unsupported")
	}

	return fmt.Sprintf("INSERT INTO `%s` (%s) SELECT %s FROM `%s`;",
		tableB.SqlName,
		strings.Join(fieldBNameArray, ", "),
		strings.Join(fieldANameArray, ", "),
		tableA.SqlName,
	), nil
}

func (db *DataBase) MigrationTable(table *Table, handler func(*Table, *Table) (string, error)) error {
	var (
		requestUnit  string
		requestArray []string
		err          error
	)

	if !db.CheckExistTable(table) {
		return db.CreateTable(table)
	}

	if !db.CheckHashTable(table) {
		return nil
	}

	if handler == nil {
		handler = MigrationTableAuto
	}

	db.schemeMutex <- true
	tableA := db.scheme[table.GoName]
	<-db.schemeMutex

	tableB := table
	tableB_Migration_SqlName := fmt.Sprintf("_%s_migration", tableB.SqlName)
	tableB_Rename_SqlName := tableB.SqlName
	tableB.SqlName = tableB_Migration_SqlName

	requestArray, err = db.sqlCreateTable(tableB)
	if err != nil {
		return err
	}

	requestUnit, err = handler(tableA, tableB)
	if err != nil {
		return err
	}
	requestArray = append(requestArray, requestUnit)

	requestArray = append(requestArray, fmt.Sprintf("DROP TABLE `%s`;", tableA.SqlName))

	requestArray = append(requestArray, fmt.Sprintf("ALTER TABLE `%s` RENAME TO `%s`;", tableB_Migration_SqlName, tableB_Rename_SqlName))

	err = db.Exec(func(db *DataBase, sqlTx *sql.Tx) (err error) {
		for _, request := range requestArray {
			_, err = sqlTx.Exec(request)
			if err != nil {
				return
			}
		}

		return
	})

	if err != nil {
		return err
	}

	db.schemeMutex <- true
	tableB.SqlName = tableB_Rename_SqlName
	db.scheme[tableB.GoName] = tableB
	<-db.schemeMutex

	return db.schemeExport()
}

//--------------------------------------------------------------------------------//
// Value control
//--------------------------------------------------------------------------------//

func (db *DataBase) GetCount(table *Table) (count int64, err error) {
	if table == nil {
		err = ErrTableIsNull
		return
	}

	if table.AutoIncrement == nil {
		err = ErrTableDoesNotHaveAutoIncrement
		return
	}

	dbRow := db.sqlRaw.QueryRow(fmt.Sprintf("SELECT COUNT(`%s`) FROM `%s`;", table.AutoIncrement.SqlName, table.SqlName))

	err = dbRow.Err()
	if err != nil {
		return
	}

	err = dbRow.Scan(&count)
	if err != nil {
		return
	}

	return
}

func (db *DataBase) GetLastId(table *Table) (id int64, err error) {
	if table == nil {
		err = ErrTableIsNull
		return
	}

	if table.AutoIncrement == nil {
		err = ErrTableDoesNotHaveAutoIncrement
		return
	}

	dbRow := db.sqlRaw.QueryRow(fmt.Sprintf("SELECT MAX(`%s`) FROM `%s`;", table.AutoIncrement.SqlName, table.SqlName))

	err = dbRow.Err()
	if err != nil {
		return
	}

	dbRow.Scan(&id)
	return
}

func (db *DataBase) SelectValue(table *Table, where string) (response interface{}, err error) {
	response, err = db.QueryWithTable(table, fmt.Sprintf("SELECT * FROM `%s` WHERE %s;", table.SqlName, where))
	return
}

func (db *DataBase) SelectValueSingle(table *Table, where string) (response interface{}, err error) {
	response, err = db.QuerySingleWithTable(table, fmt.Sprintf("SELECT * FROM `%s` WHERE %s;", table.SqlName, where))
	return
}

func (db *DataBase) SelectValueById(table *Table, id int64) (response interface{}, err error) {
	if table.AutoIncrement == nil {
		err = ErrTableDoesNotHaveAutoIncrement
		return
	}

	response, err = db.SelectValueSingle(table, fmt.Sprintf("`%s` = %d;", table.AutoIncrement.SqlName, id))
	if err != nil {
		return
	}

	return
}

func (db *DataBase) InsertValue(table *Table, value interface{}) (lastId int64, err error) {
	err = db.ExecWithTable(table, func(db *DataBase, sqlTx *sql.Tx, table *Table) (err error) {
		var (
			tableLastId    int64
			insertedLastId int64
			valueArray     []interface{}
			requestArray   []string
			sqlResult      sql.Result
		)

		valueArray, err = table.convertInterfaceToInterfaceArray(value)
		if err != nil {
			return
		}

		requestArray, err = table.sqlInsertValue(valueArray)
		if err != nil {
			return
		}

		tableLastId, err = db.GetLastId(table)
		if err != nil {
			return
		}

		for _, request := range requestArray {
			sqlResult, err = sqlTx.Exec(request)
			if err != nil {
				return
			}

			insertedLastId, err = sqlResult.LastInsertId()
			if err != nil {
				return
			}
		}

		if (insertedLastId - tableLastId) < int64(len(valueArray)) {
			err = ErrTableDidNotInsertTheValue
			return
		}

		lastId = insertedLastId
		return err
	})

	return
}

func (db *DataBase) ReplaceValue(table *Table, value interface{}) error {
	return db.ExecWithTable(table, func(db *DataBase, sqlTx *sql.Tx, table *Table) (err error) {
		var (
			replacedCount int64
			replacedValue int64
			valueArray    []interface{}
			requestArray  []string
			sqlResult     sql.Result
		)

		valueArray, err = table.convertInterfaceToInterfaceArray(value)
		if err != nil {
			return
		}

		requestArray, err = table.sqlReplaceValue(valueArray)
		if err != nil {
			return
		}

		for _, request := range requestArray {
			sqlResult, err = sqlTx.Exec(request)
			if err != nil {
				return
			}

			replacedValue, err = sqlResult.RowsAffected()
			if err != nil {
				return
			}

			replacedCount += replacedValue
		}

		if replacedCount < int64(len(valueArray)) {
			err = ErrTableDidNotReplaceTheValue
			return
		}

		return err
	})
}

func (db *DataBase) UpdateValue(table *Table, value interface{}) error {
	return db.ExecWithTable(table, func(db *DataBase, sqlTx *sql.Tx, table *Table) (err error) {
		var (
			updatedCount int64
			updatedValue int64
			valueArray   []interface{}
			requestArray []string
			sqlResult    sql.Result
		)

		valueArray, err = table.convertInterfaceToInterfaceArray(value)
		if err != nil {
			return
		}

		requestArray, err = table.sqlUpdateValue(valueArray)
		if err != nil {
			return
		}

		for _, request := range requestArray {
			sqlResult, err = sqlTx.Exec(request)
			if err != nil {
				fmt.Println(request)
				return
			}

			updatedValue, err = sqlResult.RowsAffected()
			if err != nil {
				return
			}

			updatedCount += updatedValue
		}

		if updatedCount != int64(len(valueArray)) {
			err = ErrTableDidNotUpdateTheValue
			return
		}

		return err
	})
}

func (db *DataBase) DeleteValue(table *Table, value interface{}) error {
	return db.ExecWithTable(table, func(db *DataBase, sqlTx *sql.Tx, table *Table) (err error) {
		var (
			deletedCount int64
			deletedValue int64
			valueArray   []interface{}
			requestArray []string
			sqlResult    sql.Result
		)

		valueArray, err = table.convertInterfaceToInterfaceArray(value)
		if err != nil {
			return
		}

		requestArray, err = table.sqlDeleteValue(valueArray)
		if err != nil {
			return
		}

		for _, request := range requestArray {
			sqlResult, err = sqlTx.Exec(request)
			if err != nil {
				return
			}

			deletedValue, err = sqlResult.RowsAffected()
			if err != nil {
				return
			}

			deletedCount += deletedValue
		}

		if deletedCount != int64(len(valueArray)) {
			err = ErrTableDidNotDeleteTheValue
			return
		}

		return err
	})
}

//--------------------------------------------------------------------------------//

// Creates a Database object specified by its database driver name
// and a driver-specific data source name. If sqlScheme file path exists
// imports provided database schema otherwise exports it.
func NewDatabase(sqlDriver, sqlSource, sqlScheme string) (*DataBase, error) {
	if len(sqlDriver) == 0 {
		return nil, errors.New("NewDatabase: empty sqlDriver param string")
	}

	if len(sqlSource) == 0 {
		return nil, errors.New("NewDatabase: empty sqlSource param string")
	}

	if len(sqlScheme) == 0 {
		return nil, errors.New("NewDatabase: empty sqlScheme param string")
	}

	var (
		db = &DataBase{
			mutex: make(chan interface{}, 1),

			sqlDriver: sqlDriver,
			sqlSource: sqlSource,
			sqlScheme: sqlScheme,

			schemeMutex: make(chan interface{}, 1),
			scheme:      map[string]*Table{},
		}
		err error
	)

	db.sqlRaw, err = sql.Open(sqlDriver, sqlSource)
	if err != nil {
		return nil, err
	}

	err = db.schemeImport()
	if err != nil {
		ierr, ok := err.(*fs.PathError)
		if !ok {
			return nil, err
		}

		if ierr.Op == "open" && os.IsNotExist(ierr) {
			err = db.schemeExport()
			if err != nil {
				return nil, err
			}
		}
	}

	return db, nil
}

//--------------------------------------------------------------------------------//
