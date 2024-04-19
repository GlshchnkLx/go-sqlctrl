package sqlctrl

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
)

//--------------------------------------------------------------------------------//
// constants
//--------------------------------------------------------------------------------//

const (
	schemaTableName      string = "sql_schema_table" // table to store tables for migration
	schemaFieldTableName string = "sql_schema_field" // table to store table fileds for migration
)

//--------------------------------------------------------------------------------//

type DataBase struct {
	mutex chan interface{}

	sqlDriver string
	sqlSource string
	sqlRaw    *sql.DB

	schemeMutex chan interface{}
	scheme      map[string]*Table
}

// Adapter type for export-import sql schema. Saves table data
type sqlSchemaTable struct {
	Id              int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
	GoName          string `sql:"NAME=goName, TYPE=TEXT(64), NOT_NULL"`
	SqlName         string `sql:"NAME=sqlName, TYPE=TEXT(64), NOT_NULL"`
	MigrationNumber int64  `sql:"NAME=migrationNumber, TYPE=INTEGER, NOT_NULL"`
	Hash            string `sql:"NAME=hash, TYPE=TEXT(256), NOT_NULL"`
}

// Adapter type for export-import sql schema. Saves table field data
type sqlSchemaField struct {
	Id              int64   `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
	TableId         int64   `sql:"NAME=tableId, TYPE=INTEGER, NOT_NULL"`
	GoName          string  `sql:"NAME=goName, TYPE=TEXT(64), NOT_NULL"`
	SqlName         string  `sql:"NAME=sqlName, TYPE=TEXT(64), NOT_NULL"`
	SqlType         string  `sql:"NAME=sqlType, TYPE=TEXT(64), NOT_NULL"`
	IsPrimaryKey    bool    `sql:"NAME=isPrimaryKey, TYPE=BOOL, NOT_NULL"`
	IsAutoIncrement bool    `sql:"NAME=isAutoIncrement, TYPE=BOOL, NOT_NULL"`
	IsUnique        bool    `sql:"NAME=isUnique, TYPE=BOOL, NOT_NULL"`
	IsNotNull       bool    `sql:"NAME=isNotNull, TYPE=BOOL, NOT_NULL"`
	ValueDefault    *string `sql:"NAME=valueDefault, TYPE=TEXT(64)"`
	ValueCheck      *string `sql:"NAME=valueCheck, TYPE=TEXT(64)"`
}

//--------------------------------------------------------------------------------//
// scheme control
//--------------------------------------------------------------------------------//

func (db *DataBase) SchemeImportJson(sqlSchemeFilePath string) error {
	db.schemeMutex <- true
	defer func() {
		<-db.schemeMutex
	}()

	if len(sqlSchemeFilePath) == 0 {
		return ErrInvalidArgument
	}

	schemeByte, err := os.ReadFile(sqlSchemeFilePath)
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

func (db *DataBase) schemeImportFromDataBase() error {
	// db.schemeMutex <- true
	// defer func() {
	// 	<-db.schemeMutex
	// }()

	schemaTable, err := NewTable(schemaTableName, sqlSchemaTable{})
	if err != nil {
		return err
	}

	db.schemeMutex <- true
	db.scheme[schemaTable.GoName] = schemaTable
	<-db.schemeMutex

	schemaTableArrayIface, err := db.SelectAll(schemaTable)
	if err != nil {
		delete(db.scheme, schemaTable.GoName) // should be removed for empty database. otherwise -- will be error on export
		return err
	}

	schemaTableArray, ok := schemaTableArrayIface.([]sqlSchemaTable)
	if !ok {
		return errors.New("wrong cast schemaTableArrayIface to []SqlSchemaTable")
	}

	if len(schemaTableArray) == 0 {
		return ErrMigrationTableIsEmpty
	}

	schemaFieldTable, err := NewTable(schemaFieldTableName, sqlSchemaField{})
	if err != nil {
		return err
	}

	db.schemeMutex <- true
	db.scheme[schemaFieldTable.GoName] = schemaFieldTable
	<-db.schemeMutex

	if !db.CheckExistTable(schemaFieldTable) {
		return ErrTableDoesNotExists
	}

	scheme := map[string]*Table{}

	for _, schemaTable := range schemaTableArray {

		schemaFieldArrayIface, err := db.SelectValue(schemaFieldTable, fmt.Sprintf("tableId = %d", schemaTable.Id))
		if err != nil {
			return err
		}

		schemaFieldArray, ok := schemaFieldArrayIface.([]sqlSchemaField)
		if !ok {
			return errors.New("wrong cast schemaFieldArrayIface to []sqlSchemaField")
		}

		fieldNameArray := []string{}
		fieldMap := map[string]*TableField{}
		autoIncrement := TableField{}

		for _, schemaField := range schemaFieldArray {
			fieldNameArray = append(fieldNameArray, schemaField.GoName)

			tableField := TableField{
				GoName:          schemaField.GoName,
				SqlName:         schemaField.SqlName,
				SqlType:         schemaField.SqlType,
				IsPrimaryKey:    schemaField.IsPrimaryKey,
				IsAutoIncrement: schemaField.IsAutoIncrement,
				IsUnique:        schemaField.IsUnique,
				IsNotNull:       schemaField.IsNotNull,
				ValueDefault:    schemaField.ValueDefault,
				ValueCheck:      schemaField.ValueCheck,
			}

			fieldMap[schemaField.GoName] = &tableField

			if schemaField.IsAutoIncrement {
				autoIncrement = tableField
			}
		}

		table := Table{
			GoName:          schemaTable.GoName,
			SqlName:         schemaTable.SqlName,
			FieldNameArray:  fieldNameArray,
			FieldMap:        fieldMap,
			MigrationNumber: schemaTable.MigrationNumber,
			AutoIncrement:   &autoIncrement,
		}

		scheme[table.GoName] = &table
	}

	db.schemeMutex <- true
	for k, v := range scheme {
		db.scheme[k] = v
	}
	<-db.schemeMutex

	return nil
}

func (db *DataBase) SchemeExportJson(sqlSchemeFilePath string) error {
	db.schemeMutex <- true
	defer func() {
		<-db.schemeMutex
	}()

	if len(sqlSchemeFilePath) == 0 {
		return ErrInvalidArgument
	}

	schemeByte, err := json.MarshalIndent(db.scheme, "", "\t")
	if err != nil {
		return err
	}

	err = os.WriteFile(sqlSchemeFilePath, schemeByte, 0666)
	if err != nil {
		return err
	}

	return nil
}

func (db *DataBase) schemeExportToDatabase() error {
	// db.schemeMutex <- true
	// defer func() {
	// 	<-db.schemeMutex
	// }()

	schemaTable, err := NewTable(schemaTableName, sqlSchemaTable{})
	if err != nil {
		return err
	}

	exist := db.CheckExistTable(schemaTable)
	if !exist {
		err = db.CreateTable(schemaTable)
		if err != nil {
			return err
		}
	}

	db.schemeMutex <- true
	schemaTable = db.scheme[schemaTable.GoName] // update schemaTable with new or old value
	<-db.schemeMutex

	err = db.TruncateTable(schemaTable)
	if err != nil {
		return err
	}

	schemaFieldTable, err := db.NewTable(0, schemaFieldTableName, sqlSchemaField{})
	if err != nil {
		return err
	}

	exist = db.CheckExistTable(schemaFieldTable)
	if !exist {
		err = db.CreateTable(schemaFieldTable)
		if err != nil {
			return err
		}
	}

	err = db.TruncateTable(schemaFieldTable)
	if err != nil {
		return err
	}

	for _, table := range db.scheme {
		if table.SqlName == schemaTableName || table.SqlName == schemaFieldTableName { // if migration table -- do not export
			continue
		}

		id, err := db.InsertValue(schemaTable, sqlSchemaTable{
			GoName:          table.GoName,
			SqlName:         table.SqlName,
			MigrationNumber: table.MigrationNumber,
			Hash:            table.GetHash(),
		})
		if err != nil {
			return err
		}

		for _, fieldName := range table.FieldNameArray {
			field := table.FieldMap[fieldName]

			_, err = db.InsertValue(schemaFieldTable, sqlSchemaField{
				TableId:         id,
				GoName:          field.GoName,
				SqlName:         field.SqlName,
				SqlType:         field.SqlType,
				IsPrimaryKey:    field.IsPrimaryKey,
				IsAutoIncrement: field.IsAutoIncrement,
				IsUnique:        field.IsUnique,
				IsNotNull:       field.IsNotNull,
				ValueDefault:    field.ValueDefault,
				ValueCheck:      field.ValueCheck,
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Checks if received @table exist in database object
func (db *DataBase) CheckExistTable(table *Table) bool {
	if table == nil {
		panic("table == nil")
	}

	db.schemeMutex <- true
	schemeTable := db.scheme[table.GoName]
	<-db.schemeMutex

	return schemeTable != nil
}

// Compares hashes of argument table object and table object from database map.
// Returns result of "not equal".
func (db *DataBase) CheckHashTable(table *Table) bool {
	db.schemeMutex <- true
	schemeTable := db.scheme[table.GoName]
	<-db.schemeMutex

	return schemeTable.GetHash() != table.GetHash()
}

//--------------------------------------------------------------------------------//
// DataBase control
//--------------------------------------------------------------------------------//

// Executes provided @request query for specified @table.
// Does not check presence of @table in database.
// Returns slice of values in interface{} object.
func (db *DataBase) Query(table *Table, request string) (response interface{}, err error) {
	var (
		sqlTx         *sql.Tx
		sqlRows       *sql.Rows
		responseArray reflect.Value

		structPtr     interface{}
		fieldArrayPtr []interface{}
	)

	if table == nil || len(request) == 0 {
		err = ErrInvalidArgument
		return
	}

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

// Executes provided @request query for specified @table.
// Does not check presence of @table in database.
// Returns single object in interface{} object.
func (db *DataBase) QuerySingle(table *Table, request string) (response interface{}, err error) {
	var (
		responseArray             interface{}
		responseArrayReflectValue reflect.Value
	)

	if table == nil || len(request) == 0 {
		err = ErrInvalidArgument
		return
	}

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

// Executes provided @request query for specified @table.
// Checks presence of @table in database.
// Returns slice of objects in interface{} object.
func (db *DataBase) QueryWithTable(table *Table, request string) (response interface{}, err error) {
	if table == nil || len(request) == 0 {
		return nil, ErrInvalidArgument
	}

	if !db.CheckExistTable(table) {
		return nil, ErrTableDoesNotExists
	}

	if db.CheckHashTable(table) {
		return nil, ErrTableDoesNotMigtated
	}

	return db.Query(table, request)
}

// Executes provided @request query for specified @table.
// Checks presence of @table in database.
// Returns single object in interface{} object.
func (db *DataBase) QuerySingleWithTable(table *Table, request string) (response interface{}, err error) {
	if table == nil || len(request) == 0 {
		return nil, ErrInvalidArgument
	}

	if !db.CheckExistTable(table) {
		return nil, ErrTableDoesNotExists
	}

	if db.CheckHashTable(table) {
		return nil, ErrTableDoesNotMigtated
	}

	return db.QuerySingle(table, request)
}

// Executes provided @handler closure with transaction control.
// If @handler returns non-nil error transaction rollback is executed.
func (db *DataBase) Exec(handler func(*DataBase, *sql.Tx) error) (err error) {
	var (
		sqlTx *sql.Tx
	)

	if handler == nil {
		err = ErrInvalidArgument
		return
	}

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

// Executes provided @handler closure with transaction control.
// Checks existence of @table in database.
// If @handler returns non-nil error transaction rollback is executed.
func (db *DataBase) ExecWithTable(table *Table, handler func(*DataBase, *sql.Tx, *Table) error) (err error) {
	if table == nil || handler == nil {
		return ErrInvalidArgument
	}

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

func (db *DataBase) NewTable(migrationNumber int64, tableName string, tableStruct interface{}) (*Table, error) {
	var (
		table *Table
		err   error
	)

	if len(tableName) == 0 || tableStruct == nil || migrationNumber < 0 {
		return nil, ErrInvalidArgument
	}

	table, err = NewTable(tableName, tableStruct)
	if err != nil {
		return nil, err
	}
	table.MigrationNumber = migrationNumber

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			return nil, err
		}
	}

	// not need to migrate schema table itself
	if tableName == schemaTableName || tableName == schemaFieldTableName {
		return table, nil
	}

	// no migration required for client application
	if migrationNumber == 0 {
		return table, nil
	}

	dbMigrationNumber, err := db.getTableMigrationNumber(table)
	if err != nil {
		return nil, err
	}

	// fmt.Println("migrationNumber: ", migrationNumber)
	// fmt.Println("dbMigrationNumber: ", dbMigrationNumber)

	if dbMigrationNumber > migrationNumber {
		return nil, errors.New("client is outdated")
	}

	if dbMigrationNumber == migrationNumber {
		equal, err := db.checkDBHashTable(table)
		if err != nil {
			return nil, err
		}

		if !equal {
			return nil, errors.New("database and table hash are diffrent")
		}

		if db.CheckHashTable(table) {
			return nil, errors.New("wrong migration number received. table must migrate")
		}

		// fmt.Println("dbMigrationNumber == migrationNumber")

		return table, nil // no need to migrate
	}

	err = db.MigrationTable(table, nil, migrationNumber)
	if err != nil {
		fmt.Printf("table: %s - db.MigrationTable err: %v\n", tableName, err)
		return nil, err
	}

	return table, nil
}

// Returns migration number of schema from db object (imported from database) for given @table.
// @table and table from db.schema may be not equal (first one from user, second - from db import)
func (db *DataBase) getTableMigrationNumber(table *Table) (int64, error) {
	if table == nil {
		return 0, ErrInvalidArgument
	}

	t, ok := db.scheme[table.GoName]
	if !ok {
		return 0, ErrTableDoesNotExists
	}

	return t.MigrationNumber, nil
}

// Check @table and database hash. Returns result of equal
func (db *DataBase) checkDBHashTable(table *Table) (bool, error) {
	if table == nil {
		return false, ErrInvalidArgument
	}

	dbHash, err := db.getTableDatabaseHash(table)
	if err != nil {
		return false, err
	}

	tableHash := table.GetHash()

	return dbHash == tableHash, nil
}

// Returns hash from database for given @table.
func (db *DataBase) getTableDatabaseHash(table *Table) (string, error) {
	if table == nil {
		return "", ErrInvalidArgument
	}

	schemaTable, err := NewTable(schemaTableName, sqlSchemaTable{})
	if err != nil {
		return "", err
	}

	db.schemeMutex <- true
	schemaTable = db.scheme[schemaTable.GoName]
	<-db.schemeMutex

	if schemaTable == nil {
		return "", errors.New("schemaTable is nil")
	}

	respIface, err := db.SelectValueSingle(schemaTable, fmt.Sprintf("goName = \"%s\"", table.GoName))
	if err != nil {
		fmt.Println("db.SelectValueSingle err: ", err)
		return "", err
	}

	resp, ok := respIface.(sqlSchemaTable)
	if !ok {
		return "", errors.New("wrong type assert respIface to sqlSchemaTable")
	}

	return resp.Hash, nil
}

func (db *DataBase) sqlCreateTable(table *Table) (request []string, err error) {
	if table == nil {
		err = ErrInvalidArgument
		return
	}

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

// Creates @table argument in current database.
// Saves table object in database scheme map.
func (db *DataBase) CreateTable(table *Table) error {
	if table == nil {
		return ErrInvalidArgument
	}

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

	return db.schemeExportToDatabase()
}

// Drops specified @table in database and removes from database object.
func (db *DataBase) DropTable(table *Table) error {
	if table == nil {
		return ErrInvalidArgument
	}

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

	return db.schemeExportToDatabase()
}

// Truncates specified @table in database (clear all rows).
func (db *DataBase) TruncateTable(table *Table) error {
	if table == nil {
		return ErrInvalidArgument
	}

	if !db.CheckExistTable(table) {
		return ErrTableDoesNotExists
	}

	requestArray := []string{
		fmt.Sprintf("DELETE FROM `%s`;", table.SqlName),
	}

	return db.Exec(func(db *DataBase, sqlTx *sql.Tx) (err error) {
		for _, request := range requestArray {
			_, err = sqlTx.Exec(request)
			if err != nil {
				return
			}
		}

		return
	})
}

func MigrationTableAuto(tableA, tableB *Table) (string, error) {
	if tableA == nil || tableB == nil {
		return "", ErrInvalidArgument
	}

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

func (db *DataBase) MigrationTable(table *Table, handler func(*Table, *Table) (string, error), migrationNumber int64) error {
	var (
		requestUnit  string
		requestArray []string
		err          error
	)

	if table == nil || migrationNumber < 0 {
		return ErrInvalidArgument
	}

	if !db.CheckExistTable(table) {
		return db.CreateTable(table)
	}

	if !db.CheckHashTable(table) {
		// fmt.Println(table.GoName, " - no need to migrate")
		return nil
	}

	// fmt.Println("migration!!!")

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
	tableB.MigrationNumber = migrationNumber

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

	return db.schemeExportToDatabase()
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

// Selects from given @table all data as slice of objects.
// Result of select is returned as interface{} object
// Be careful with large dataset (not sure that driver will limit data chunk)
func (db *DataBase) SelectAll(table *Table) (response interface{}, err error) {
	if table == nil {
		err = ErrInvalidArgument
		return
	}

	return db.QueryWithTable(table, fmt.Sprintf("SELECT * FROM `%s`;", table.SqlName))
}

// Selects from given @table all data as slice of objects.
// Data is limited by index @from and @portion size.
// Result of select is returned as interface{} object
func (db *DataBase) SelectAllWithLimit(table *Table, from, portion int64) (response interface{}, err error) {
	if table == nil || from < 0 || portion == 0 {
		err = ErrInvalidArgument
		return
	}

	return db.QueryWithTable(table, fmt.Sprintf("SELECT * FROM `%s` LIMIT %d, %d;", table.SqlName, from, portion))
}

// Selects from given @table slice of objects with specified @where conditional string.
// Result of select is returned as interface{} object.
func (db *DataBase) SelectValue(table *Table, where string) (response interface{}, err error) {
	if table == nil || len(where) == 0 {
		err = ErrInvalidArgument
		return
	}

	return db.QueryWithTable(table, fmt.Sprintf("SELECT * FROM `%s` WHERE %s;", table.SqlName, where))
}

// Selects from given @table single object with specified @where conditional string.
func (db *DataBase) SelectValueSingle(table *Table, where string) (response interface{}, err error) {
	if table == nil || len(where) == 0 {
		err = ErrInvalidArgument
		return
	}

	return db.QuerySingleWithTable(table, fmt.Sprintf("SELECT * FROM `%s` WHERE %s;", table.SqlName, where))
}

// Selects from given @table single object with specified @id value.
func (db *DataBase) SelectValueById(table *Table, id int64) (response interface{}, err error) {
	if table == nil || id < 0 {
		err = ErrInvalidArgument
		return
	}

	if table.AutoIncrement == nil {
		err = ErrTableDoesNotHaveAutoIncrement
		return
	}

	return db.SelectValueSingle(table, fmt.Sprintf("`%s` = %d;", table.AutoIncrement.SqlName, id))
}

// Inserts provided @value object (single or slice) into @table.
// Returns last inserted object id.
func (db *DataBase) InsertValue(table *Table, value interface{}) (lastId int64, err error) {
	if table == nil || value == nil {
		err = ErrInvalidArgument
		return
	}

	err = db.ExecWithTable(table, func(db *DataBase, sqlTx *sql.Tx, table *Table) (err error) {
		var (
			tableLastId    int64
			insertedLastId int64
			valueArray     []interface{}
			requestArray   []string
			sqlResult      sql.Result
		)

		if db == nil || sqlTx == nil || table == nil {
			err = ErrInvalidArgument
			return
		}

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

// Replaces provided @value object (single or slice) in @table.
func (db *DataBase) ReplaceValue(table *Table, value interface{}) error {
	if table == nil || value == nil {
		return ErrInvalidArgument
	}

	return db.ExecWithTable(table, func(db *DataBase, sqlTx *sql.Tx, table *Table) (err error) {
		var (
			replacedCount int64
			replacedValue int64
			valueArray    []interface{}
			requestArray  []string
			sqlResult     sql.Result
		)

		if db == nil || sqlTx == nil || table == nil {
			err = ErrInvalidArgument
			return
		}

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

// Updates provided @value object (single or slice) in @table.
func (db *DataBase) UpdateValue(table *Table, value interface{}) error {
	if table == nil || value == nil {
		return ErrInvalidArgument
	}

	return db.ExecWithTable(table, func(db *DataBase, sqlTx *sql.Tx, table *Table) (err error) {
		var (
			updatedCount int64
			updatedValue int64
			valueArray   []interface{}
			requestArray []string
			sqlResult    sql.Result
		)

		if db == nil || sqlTx == nil || table == nil {
			err = ErrInvalidArgument
			return
		}

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

// Delete provided @value object (single or slice) from @table.
func (db *DataBase) DeleteValue(table *Table, value interface{}) error {
	if table == nil || value == nil {
		return ErrInvalidArgument
	}

	return db.ExecWithTable(table, func(db *DataBase, sqlTx *sql.Tx, table *Table) (err error) {
		var (
			deletedCount int64
			deletedValue int64
			valueArray   []interface{}
			requestArray []string
			sqlResult    sql.Result
		)

		if db == nil || sqlTx == nil || table == nil {
			err = ErrInvalidArgument
			return
		}

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

// Iterates over each row of @table and executes @hanler func.
// @table must be non-empty table and be present in database.
// @portion limits sizes of data chunk stored in memory.
// Be careful with @portion -- may cause troubles on big @portion values and large data object size.
// This method does not have any transaction control.
// @index might be not equal @table Id and used only as iteration index.
// @value is a current iteration object.
//
// Inspired by js/ts ForEach. Probably is a case of internal iterator pattern.
// https://wiki.c2.com/?InternalIterator
// If handler code returns non nil error - iteration stops and same error is returned.
func (db *DataBase) ForEach(table *Table, handler func(index int64, value interface{}) error, portion int64) error {
	if table == nil || handler == nil || portion == 0 {
		return ErrInvalidArgument
	}

	if !db.CheckExistTable(table) {
		return ErrTableDoesNotExists
	}

	count, err := db.GetCount(table)
	if err != nil {
		return err
	}

	if count == 0 {
		return errors.New("zero objects found")
	}

	for p := int64(0); p < count; p += portion {
		err := func() error {
			value, err := db.SelectAllWithLimit(table, p, portion)
			if err != nil {
				return err
			}

			valueArray, err := table.convertInterfaceToInterfaceArray(value)
			if err != nil {
				return err
			}

			for i, v := range valueArray {
				err := handler(p+int64(i), v)
				if err != nil {
					return fmt.Errorf(
						"error at range[%d: %d], table index: %d, object: %v error: %v",
						p, p+portion, i, v, err,
					)
				}
			}

			return nil
		}()

		if err != nil {
			return err
		}
	}

	return nil
}

//--------------------------------------------------------------------------------//

// Creates a Database object specified by its database driver @sqlDriver
// and a driver-specific data source @sqlSource.
func NewDatabase(sqlDriver, sqlSource string) (*DataBase, error) {
	if len(sqlDriver) == 0 || len(sqlSource) == 0 {
		return nil, ErrInvalidArgument
	}

	var (
		db = &DataBase{
			mutex: make(chan interface{}, 1),

			sqlDriver: sqlDriver,
			sqlSource: sqlSource,

			schemeMutex: make(chan interface{}, 1),
			scheme:      map[string]*Table{},
		}
		err error
	)

	db.sqlRaw, err = sql.Open(sqlDriver, sqlSource)
	if err != nil {
		return nil, err
	}

	_ = db.schemeImportFromDataBase()
	// if err != nil {
	// 	fmt.Println("db.schemeImportFromDataBase err: ", err)
	// }

	return db, nil
}

//--------------------------------------------------------------------------------//
