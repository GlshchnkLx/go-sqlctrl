package sqlctrl

import (
	"database/sql"
	"reflect"
)

//--------------------------------------------------------------------------------//

type transportSimple struct {
	Transport
	database *Database

	mutex            chan interface{}
	sqlDriver        string
	sqlSource        string
	sqlDb            *sql.DB
	sqlTx            *sql.Tx
	sqlTxError       error
	sqlTxIndexLast   int64
	sqlTxChangeCount int64
}

//--------------------------------------------------------------------------------//

func (transport *transportSimple) helperSqlRowsToInterface(sqlRowArray *sql.Rows, responseUnitTable *Table) (response interface{}, err error) {
	var (
		responseArray          reflect.Value
		responseUnitStruct     interface{}
		responseUnitFieldArray []interface{}
	)

	if sqlRowArray == nil || responseUnitTable == nil {
		err = ErrorBuilderWithoutResponse
		return
	}

	responseArray = reflect.MakeSlice(reflect.SliceOf(responseUnitTable.GetGoType()), 0, 0)

	for sqlRowArray.Next() {
		responseUnitStruct, responseUnitFieldArray, err = responseUnitTable.GetStruct(nil)
		if err != nil {
			return
		}

		err = sqlRowArray.Scan(responseUnitFieldArray...)
		if err != nil {
			return
		}

		responseArray = reflect.Append(responseArray, reflect.ValueOf(responseUnitStruct).Elem())
	}

	response = responseArray.Interface()
	return
}

//--------------------------------------------------------------------------------//

func (transport *transportSimple) Lock() {
	transport.mutex <- true
}

func (transport *transportSimple) LockSqlDb() *sql.DB {
	transport.mutex <- true
	return transport.sqlDb
}

func (transport *transportSimple) LockSqlTx() *sql.Tx {
	transport.mutex <- true
	return transport.sqlTx
}

func (transport *transportSimple) Unlock() {
	<-transport.mutex
}

//--------------------------------------------------------------------------------//

func (transport *transportSimple) TransportRegister(database *Database) error {
	transport.mutex <- true

	if transport.sqlDb == nil {
		<-transport.mutex
		return ErrorTransportIsAlreadyClosed
	}

	if database == nil && transport.database == nil {
		<-transport.mutex
		return ErrorTransportMustHaveDatabase
	}

	if database != nil && database.Transport != nil {
		<-transport.mutex
		return ErrorDatabaseIsAlreadyHasTransport
	}

	if database != nil {
		database.Transport = transport
		transport.database = database
	}

	<-transport.mutex
	return nil
}

func (transport *transportSimple) Open() (err error) {
	transport.mutex <- true

	if transport.sqlDb != nil {
		<-transport.mutex
		return ErrorTransportIsAlreadyOpened
	}

	transport.sqlDb, err = sql.Open(transport.sqlDriver, transport.sqlSource)
	if err != nil {
		<-transport.mutex
		return
	}

	<-transport.mutex
	return
}

func (transport *transportSimple) Close() (err error) {
	transport.mutex <- true

	if transport.sqlDb == nil {
		<-transport.mutex
		return ErrorTransportIsAlreadyClosed
	}

	err = transport.sqlDb.Close()
	if err != nil {
		<-transport.mutex
		return
	}
	transport.sqlDb = nil

	<-transport.mutex
	return
}

func (transport *transportSimple) Execute(builderRequest Builder) (transportError error) {
	var (
		builderString string
		builderOption []interface{}
		builderError  error
	)

	if builderRequest == nil {
		return ErrorBuilderIsNil
	}

	transport.mutex <- true

	if transport.sqlDb == nil {
		<-transport.mutex
		return ErrorTransportIsAlreadyClosed
	}

	switch v := builderRequest.(type) {
	case BuilderWithDialect:
		v.SetDialect(transport.sqlDriver)
	}

	builderString, builderOption, builderError = builderRequest.Build()
	if builderError != nil {
		<-transport.mutex
		return builderError
	}

	_, transportError = transport.sqlDb.Exec(builderString, builderOption...)
	if transportError != nil {
		<-transport.mutex
		return
	}

	<-transport.mutex
	return
}

func (transport *transportSimple) Query(builderRequest BuilderWithResponse) (response interface{}, err error) {
	var (
		builderString     string
		builderOption     []interface{}
		sqlRowArray       *sql.Rows
		responseUnitTable *Table
	)

	if builderRequest == nil {
		err = ErrorBuilderIsNil
		return
	}

	transport.mutex <- true

	if transport.sqlDb == nil {
		<-transport.mutex
		return nil, ErrorTransportIsAlreadyClosed
	}

	builderString, builderOption, err = builderRequest.Build()
	if err != nil {
		<-transport.mutex
		return
	}

	responseUnitTable = builderRequest.GetResponseTable()
	if responseUnitTable == nil {
		err = ErrorBuilderWithoutResponse
		<-transport.mutex
		return
	}

	sqlRowArray, err = transport.sqlDb.Query(builderString, builderOption...)
	if err != nil {
		<-transport.mutex
		return
	}

	<-transport.mutex
	return transport.helperSqlRowsToInterface(sqlRowArray, responseUnitTable)
}

//--------------------------------------------------------------------------------//

func (transport *transportSimple) TransactionOpen() (*Transaction, error) {
	var (
		transaction *Transaction
		err         error
	)

	transport.mutex <- true

	if transport.sqlDb == nil {
		<-transport.mutex
		return nil, ErrorTransportIsAlreadyClosed
	}

	if transport.sqlTx != nil {
		<-transport.mutex
		return nil, ErrorTransactionIsAlreadyOpened
	}

	transport.sqlTx, err = transport.sqlDb.Begin()
	if err != nil {
		<-transport.mutex
		return nil, err
	}

	transaction, err = NewTransaction(transport)
	transport.sqlTxError = err
	transport.sqlTxIndexLast = 0
	transport.sqlTxChangeCount = 0
	<-transport.mutex

	return transaction, err
}

func (transport *transportSimple) TransactionCommit() (err error) {
	transport.mutex <- true

	if transport.sqlTx == nil {
		<-transport.mutex
		return ErrorTransactionIsAlreadyClosed
	}

	err = transport.sqlTx.Commit()
	if err != nil {
		<-transport.mutex
		return
	}

	transport.sqlTx = nil
	<-transport.mutex

	return
}

func (transport *transportSimple) TransactionRollback() (err error) {
	transport.mutex <- true

	if transport.sqlTx == nil {
		<-transport.mutex
		return ErrorTransactionIsAlreadyClosed
	}

	err = transport.sqlTx.Rollback()
	if err != nil {
		<-transport.mutex
		return
	}

	transport.sqlTx = nil
	<-transport.mutex

	return
}

func (transport *transportSimple) TransactionStatus() (int64, int64, error) {
	return transport.sqlTxIndexLast, transport.sqlTxChangeCount, transport.sqlTxError
}

func (transport *transportSimple) TransactionExecute(builderRequest Builder) (transactionError error) {
	var (
		builderString string
		builderOption []interface{}
		builderError  error
		sqlResult     sql.Result

		sqlTxIndexLast   int64
		sqlTxChangeCount int64
	)

	if builderRequest == nil {
		return ErrorBuilderIsNil
	}

	transport.mutex <- true

	if transport.sqlTx == nil {
		<-transport.mutex
		return ErrorTransactionIsAlreadyClosed
	}

	defer func() {
		transport.sqlTxError = transactionError
		<-transport.mutex
	}()

	switch v := builderRequest.(type) {
	case BuilderWithDialect:
		v.SetDialect(transport.sqlDriver)
	}

	builderString, builderOption, builderError = builderRequest.Build()
	if builderError != nil {
		return builderError
	}

	sqlResult, transactionError = transport.sqlTx.Exec(builderString, builderOption...)
	if transactionError != nil {
		return
	}

	sqlTxIndexLast, transactionError = sqlResult.LastInsertId()
	if transactionError != nil {
		return
	}
	transport.sqlTxIndexLast = sqlTxIndexLast

	sqlTxChangeCount, transactionError = sqlResult.RowsAffected()
	if transactionError != nil {
		return
	}
	transport.sqlTxChangeCount += sqlTxChangeCount

	return
}

func (transport *transportSimple) TransactionQuery(builderRequest BuilderWithResponse) (response interface{}, err error) {
	var (
		builderString     string
		builderOption     []interface{}
		sqlRowArray       *sql.Rows
		responseUnitTable *Table
	)

	if builderRequest == nil {
		err = ErrorBuilderIsNil
		return
	}

	transport.mutex <- true

	if transport.sqlTx == nil {
		<-transport.mutex
		return nil, ErrorTransactionIsAlreadyClosed
	}

	builderString, builderOption, err = builderRequest.Build()
	if err != nil {
		<-transport.mutex
		return
	}

	responseUnitTable = builderRequest.GetResponseTable()
	if responseUnitTable == nil {
		err = ErrorBuilderWithoutResponse
		<-transport.mutex
		return
	}

	sqlRowArray, err = transport.sqlTx.Query(builderString, builderOption...)
	if err != nil {
		<-transport.mutex
		return
	}

	<-transport.mutex
	return transport.helperSqlRowsToInterface(sqlRowArray, responseUnitTable)
}

//--------------------------------------------------------------------------------//

func NewTransportSimple(sqlDriver string, sqlSource string) Transport {
	return &transportSimple{
		mutex:     make(chan interface{}, 1),
		sqlDriver: sqlDriver,
		sqlSource: sqlSource,
	}
}

//--------------------------------------------------------------------------------//
