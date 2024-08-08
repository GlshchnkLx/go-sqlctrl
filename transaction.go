package sqlctrl

import (
	"fmt"
)

// --------------------------------------------------------------------------------//

var (
	TransactionInsertBlock  int64 = 10
	TransactionReplaceBlock int64 = 10
)

//--------------------------------------------------------------------------------//

type Transaction struct {
	transport Transport
}

//--------------------------------------------------------------------------------//

func (transaction *Transaction) Commit() error {
	return transaction.transport.TransactionCommit()
}

func (transaction *Transaction) Rollback() error {
	return transaction.transport.TransactionRollback()
}

func (transaction *Transaction) Execute(builderRequest Builder) error {
	return transaction.transport.TransactionExecute(builderRequest)
}

func (transaction *Transaction) Query(builderRequest BuilderWithResponse) (interface{}, error) {
	return transaction.transport.TransactionQuery(builderRequest)
}

func (transaction *Transaction) GetIndexLast() int64 {
	sqlTxLastIndex, _, _ := transaction.transport.TransactionStatus()
	return sqlTxLastIndex
}

func (transaction *Transaction) GetChangeCount() int64 {
	_, sqlTxChangeCount, _ := transaction.transport.TransactionStatus()
	return sqlTxChangeCount
}

func (transaction *Transaction) GetError() error {
	_, _, sqlTxError := transaction.transport.TransactionStatus()
	return sqlTxError
}

//--------------------------------------------------------------------------------//

func (transaction *Transaction) ExecuteCreateTable(createTable *Table) error {
	return transaction.Execute(NewBuilderCreate(createTable).IfNotExists(false))
}

func (transaction *Transaction) ExecuteDropTable(dropTable *Table) (err error) {
	if dropTable == nil {
		return ErrorTableIsNil
	}

	sqlTx := transaction.transport.LockSqlTx()
	defer transaction.transport.Unlock()

	if sqlTx == nil {
		return ErrorTransactionIsAlreadyClosed
	}

	_, err = sqlTx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", dropTable.GetSqlName()))
	if err != nil {
		return
	}

	return
}

func (transaction *Transaction) ExecuteDeleteTable(deleteTable *Table) error {
	return transaction.Execute(NewBuilderDelete(deleteTable))
}

func (transaction *Transaction) ExecuteInsertValue(insertTable *Table, insertValueArray ...interface{}) (err error) {
	var (
		insertOffset  int64
		insertBuilder *BuilderInsert
	)

	insertBuilder = NewBuilderInsert(insertTable)

	for insertOffset = 0; insertOffset < int64(len(insertValueArray)); insertOffset += TransactionInsertBlock {
		insertOffsetNext := insertOffset + TransactionInsertBlock
		if int64(len(insertValueArray)) <= insertOffsetNext {
			insertOffsetNext = int64(len(insertValueArray))
		}

		insertBuilder.Value(insertValueArray[insertOffset:insertOffsetNext]...)
		err = transaction.Execute(insertBuilder)

		if err != nil {
			return
		}
	}

	return
}

func (transaction *Transaction) ExecuteReplaceValue(replaceTable *Table, replaceValueArray ...interface{}) (err error) {
	var (
		replaceOffset  int64
		replaceBuilder *BuilderReplace
	)

	replaceBuilder = NewBuilderReplace(replaceTable)

	for replaceOffset = 0; replaceOffset < int64(len(replaceValueArray)); replaceOffset += TransactionReplaceBlock {
		replaceOffsetNext := replaceOffset + TransactionReplaceBlock
		if int64(len(replaceValueArray)) <= replaceOffsetNext {
			replaceOffsetNext = int64(len(replaceValueArray))
		}

		replaceBuilder.Value(replaceValueArray[replaceOffset:replaceOffsetNext]...)
		err = transaction.Execute(replaceBuilder)

		if err != nil {
			return
		}
	}

	return
}

func (transaction *Transaction) ExecuteUpdateValue(updateBuilder *BuilderUpdate) (err error) {
	if updateBuilder == nil {
		return ErrorBuilderIsNil
	}

	return transaction.Execute(updateBuilder)
}

//--------------------------------------------------------------------------------//

func NewTransaction(transport Transport) (*Transaction, error) {
	if transport == nil {
		return nil, ErrorTransportIsNil
	}

	transaction := &Transaction{
		transport: transport,
	}

	return transaction, nil
}

//--------------------------------------------------------------------------------//
