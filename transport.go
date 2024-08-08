package sqlctrl

import (
	"database/sql"
)

//--------------------------------------------------------------------------------//

type Transport interface {
	Lock()
	LockSqlDb() *sql.DB
	LockSqlTx() *sql.Tx
	Unlock()

	TransportRegister(*Database) error
	Open() error
	Close() error
	Execute(Builder) error
	Query(BuilderWithResponse) (interface{}, error)

	TransactionOpen() (*Transaction, error)
	TransactionCommit() error
	TransactionRollback() error
	TransactionStatus() (int64, int64, error)
	TransactionExecute(Builder) error
	TransactionQuery(BuilderWithResponse) (interface{}, error)
}

//--------------------------------------------------------------------------------//
