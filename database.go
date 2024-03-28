package sqlctrl

import (
	"context"
	"database/sql"
	"encoding/json"
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

// Checks if received @table exist in database object
func (db *DataBase) CheckExistTable(table *Table) bool {
	if table == nil {
		return false
	}

	db.schemeMutex <- true
	schemeTable := db.scheme[table.GoName]
	<-db.schemeMutex

	return schemeTable != nil
}

// Compares hashes of argument table object and table object from database map
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

// Executes provided @request query for specified @table with @ctx context.
// Does not check presence of @table in database.
// Returns slice of values in interface{} object.
func (db *DataBase) QueryContext(ctx context.Context, table *Table, request string) (response interface{}, err error) {
	err = doWithContext(ctx, func() error {
		response, err = db.Query(table, request)
		return err
	})

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

// Executes provided @request query for specified @table with @ctx context.
// Does not check presence of @table in database.
// Returns single object in interface{} object.
func (db *DataBase) QuerySingleContext(ctx context.Context, table *Table, request string) (response interface{}, err error) {
	err = doWithContext(ctx, func() error {
		response, err = db.QuerySingle(table, request)
		return err
	})

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

// Executes provided @request query for specified @table with @ctx context.
// Checks presence of @table in database.
// Returns slice of objects in interface{} object.
func (db *DataBase) QueryWithTableContext(ctx context.Context, table *Table, request string) (response interface{}, err error) {
	err = doWithContext(ctx, func() error {
		response, err = db.QueryWithTable(table, request)
		return err
	})

	return
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

// Executes provided @request query for specified @table with @ctx context.
// Checks presence of @table in database.
// Returns single object in interface{} object.
func (db *DataBase) QuerySingleWithTableContext(ctx context.Context, table *Table, request string) (response interface{}, err error) {
	err = doWithContext(ctx, func() error {
		response, err = db.QuerySingleWithTable(table, request)
		return err
	})

	return
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

// Executes provided @handler closure with @ctx context. Has transaction control.
// If @handler returns non-nil error transaction rollback is executed.
func (db *DataBase) ExecContext(ctx context.Context, handler func(*DataBase, *sql.Tx) error) (err error) {
	return doWithContext(ctx, func() error {
		return db.Exec(handler)
	})
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

// Executes provided @handler closure with @ctx context. Has transaction control.
// Checks existence of @table in database.
// If @handler returns non-nil error transaction rollback is executed.
func (db *DataBase) ExecWithTableContext(ctx context.Context, table *Table, handler func(*DataBase, *sql.Tx, *Table) error) (err error) {
	return doWithContext(ctx, func() error {
		return db.ExecWithTable(table, handler)
	})
}

//--------------------------------------------------------------------------------//
// Table control
//--------------------------------------------------------------------------------//

func (db *DataBase) NewTable(tableName string, tableStruct interface{}) (*Table, error) {
	var (
		table *Table
		err   error
	)

	if len(tableName) == 0 || tableStruct == nil {
		return nil, ErrInvalidArgument
	}

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

	return db.schemeExport()
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

	return db.schemeExport()
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

func (db *DataBase) MigrationTable(table *Table, handler func(*Table, *Table) (string, error)) error {
	var (
		requestUnit  string
		requestArray []string
		err          error
	)

	if table == nil {
		return ErrInvalidArgument
	}

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

// Selects from given @table slice of objects with specified @where conditional string.
// Result of select is returned as interface{} object
func (db *DataBase) SelectValue(table *Table, where string) (response interface{}, err error) {
	if table == nil || len(where) == 0 {
		err = ErrInvalidArgument
		return
	}

	return db.QueryWithTable(table, fmt.Sprintf("SELECT * FROM `%s` WHERE %s;", table.SqlName, where))
}

// Selects from given @table slice of objects with provided @ctx context and @where conditional string.
// Result of select is returned as interface{} object
func (db *DataBase) SelectValueContext(ctx context.Context, table *Table, where string) (response interface{}, err error) {
	err = doWithContext(ctx, func() error {
		response, err = db.SelectValue(table, where)
		return err
	})

	return
}

// Selects from given @table single object with specified @where conditional string.
func (db *DataBase) SelectValueSingle(table *Table, where string) (response interface{}, err error) {
	if table == nil || len(where) == 0 {
		err = ErrInvalidArgument
		return
	}

	return db.QuerySingleWithTable(table, fmt.Sprintf("SELECT * FROM `%s` WHERE %s;", table.SqlName, where))
}

// Selects from given @table single object with specified @ctx context and @where conditional string.
func (db *DataBase) SelectValueSingleContext(ctx context.Context, table *Table, where string) (response interface{}, err error) {
	err = doWithContext(ctx, func() error {
		response, err = db.SelectValueSingle(table, where)
		return err
	})

	return
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

// Selects from given @table single object with specified @id value.
func (db *DataBase) SelectValueByIdContext(ctx context.Context, table *Table, id int64) (response interface{}, err error) {
	err = doWithContext(ctx, func() error {
		response, err = db.SelectValueById(table, id)
		return err
	})

	return
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

// Inserts provided @value object (single or slice) into @table with @ctx context.
// Returns last inserted object id.
func (db *DataBase) InsertValueContext(ctx context.Context, table *Table, value interface{}) (lastId int64, err error) {
	err = doWithContext(ctx, func() error {
		lastId, err = db.InsertValue(table, value)
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

// Replaces provided @value object (single or slice) in @table with @ctx context.
func (db *DataBase) ReplaceValueContext(ctx context.Context, table *Table, value interface{}) error {
	return doWithContext(ctx, func() error {
		return db.ReplaceValue(table, value)
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

// Updates provided @value object (single or slice) in @table with @ctx context.
func (db *DataBase) UpdateValueContext(ctx context.Context, table *Table, value interface{}) error {
	return doWithContext(ctx, func() error {
		return db.UpdateValue(table, value)
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

// Delete provided @value object (single or slice) from @table with @ctx context.
func (db *DataBase) DeleteValueContext(ctx context.Context, table *Table, value interface{}) error {
	return doWithContext(ctx, func() error {
		return db.DeleteValue(table, value)
	})
}

//--------------------------------------------------------------------------------//

// Creates a Database object specified by its database driver @sqlDriver
// and a driver-specific data source @sqlSource. If @sqlScheme file path exists
// imports provided database schema otherwise exports it.
func NewDatabase(sqlDriver, sqlSource, sqlScheme string) (*DataBase, error) {
	if len(sqlDriver) == 0 || len(sqlSource) == 0 || len(sqlScheme) == 0 {
		return nil, ErrInvalidArgument
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
