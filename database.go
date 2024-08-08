package sqlctrl

import (
	"fmt"
	"reflect"
)

//--------------------------------------------------------------------------------//

type Database struct {
	Transport
	scheme Scheme

	mutex chan interface{}
}

//--------------------------------------------------------------------------------//

func (database *Database) RegisterTable(tableName string, tableStruct interface{}) (table *Table, err error) {
	return database.scheme.RegisterTable(tableName, tableStruct)
}

//--------------------------------------------------------------------------------//

func (database *Database) QueryValue(request BuilderWithResponse) (response interface{}, err error) {
	var (
		responseArray             interface{}
		responseUnitTable         *Table
		responseArrayReflectValue reflect.Value
	)

	responseArray, err = database.Query(request)
	if err != nil {
		return
	}

	responseUnitTable = request.GetResponseTable()

	responseArrayReflectValue = reflect.ValueOf(responseArray).Convert(reflect.SliceOf(responseUnitTable.GetGoType()))

	switch responseArrayReflectValue.Len() {
	case 0:
		err = ErrorResponseLessThanRequested
		return
	case 1:
	default:
		err = ErrorResponseMoreThanRequested
		return
	}

	response = responseArrayReflectValue.Index(0).Interface()

	return
}

//--------------------------------------------------------------------------------//

func (database *Database) ExecuteCreateTable(createTable *Table) error {
	return database.Execute(NewBuilderCreate(createTable).IfNotExists(false))
}

func (database *Database) ExecuteDropTable(dropTable *Table) (err error) {
	if dropTable == nil {
		return ErrorTableIsNil
	}

	sqlDb := database.LockSqlDb()
	defer database.Unlock()

	if sqlDb == nil {
		return ErrorTransportIsAlreadyClosed
	}

	_, err = sqlDb.Exec(fmt.Sprintf("DROP TABLE `%s`", dropTable.GetSqlName()))
	if err != nil {
		return
	}

	return
}

func (database *Database) ExecuteDeleteTable(deleteTable *Table) error {
	return database.Execute(NewBuilderDelete(deleteTable))
}

//--------------------------------------------------------------------------------//

func (database *Database) QueryTableIndexLast(table *Table) (indexLast int64, err error) {
	if table == nil {
		err = ErrorTableIsNil
		return
	}

	if table.GetAutoIncrement() == nil {
		err = ErrorTableMustHaveAutoincrement
		return
	}

	sqlDb := database.LockSqlDb()
	defer database.Unlock()

	if sqlDb == nil {
		err = ErrorTransportIsAlreadyClosed
		return
	}

	dbRow := sqlDb.QueryRow(fmt.Sprintf("SELECT MAX(`%s`) FROM `%s`", table.GetAutoIncrement().GetSqlName(), table.GetSqlName()))

	err = dbRow.Err()
	if err != nil {
		return
	}

	dbRow.Scan(&indexLast)
	return
}

func (database *Database) QueryTableIndexCount(table *Table) (indexLast int64, err error) {
	if table == nil {
		err = ErrorTableIsNil
		return
	}

	if table.GetAutoIncrement() == nil {
		err = ErrorTableMustHaveAutoincrement
		return
	}

	sqlDb := database.LockSqlDb()
	defer database.Unlock()

	if sqlDb == nil {
		err = ErrorTransportIsAlreadyClosed
		return
	}

	dbRow := sqlDb.QueryRow(fmt.Sprintf("SELECT COUNT(`%s`) FROM `%s`", table.GetAutoIncrement().GetSqlName(), table.GetSqlName()))

	err = dbRow.Err()
	if err != nil {
		return
	}

	dbRow.Scan(&indexLast)
	return
}

//--------------------------------------------------------------------------------//

func NewDatabase(transport Transport, scheme Scheme) (*Database, error) {
	var (
		database *Database
		err      error
	)

	if transport == nil {
		err = ErrorTransportIsNil
		return nil, err
	}

	if scheme == nil {
		err = ErrorSchemeIsNil
		return nil, err
	}

	database = &Database{
		mutex: make(chan interface{}, 1),
	}

	err = transport.Open()
	if err != nil {
		return nil, err
	}

	err = transport.TransportRegister(database)
	if err != nil {
		return nil, err
	}

	err = scheme.SchemeRegister(database)
	if err != nil {
		return nil, err
	}
	database.scheme = scheme

	err = database.scheme.Import()
	if err != nil {
		return nil, err
	}

	return database, nil
}

//--------------------------------------------------------------------------------//
