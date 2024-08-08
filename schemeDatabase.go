package sqlctrl

import (
	"database/sql"
	"fmt"
	"strings"
)

//--------------------------------------------------------------------------------//

type schemeDatabase struct {
	Scheme
	database         *Database
	transport        Transport
	transactionCount int64
	transaction      *Transaction

	mutex chan interface{}

	storageName        string
	storageVersion     int64
	storageV1Table     *Table
	storageStableTable *Table
	storageRemote      map[string]*SchemeStorageTable
	storageLocal       map[string]*SchemeStorageTable
	tableMap           map[string]*Table
}

//--------------------------------------------------------------------------------//

func (scheme *schemeDatabase) transactionOpen() (transaction *Transaction, err error) {
	if scheme.transaction == nil {
		if scheme.transactionCount > 0 {
			err = ErrorSchemeTransactionHasUnknownState
			return
		}

		transaction, err = scheme.database.TransactionOpen()
		if err != nil {
			return
		}

		scheme.transaction = transaction
	} else {
		transaction = scheme.transaction
	}

	scheme.transactionCount++

	return
}

func (scheme *schemeDatabase) transactionClose(errIn error) (errOut error) {
	if scheme.transaction == nil {
		if errIn != nil {
			return errOut
		}

		if scheme.transactionCount > 0 {
			return ErrorSchemeTransactionHasUnknownState
		}

		return ErrorSchemeTransactionIsAlreadyClosed
	} else {
		scheme.transactionCount--

		if scheme.transactionCount <= 0 {
			if errIn == nil {
				errOut = scheme.transaction.Commit()
			} else {
				errOut = scheme.transaction.Rollback()
			}

			if errOut == nil {
				scheme.transaction = nil
			}
		}

		if errIn != nil {
			return errIn
		}
	}

	return
}

//--------------------------------------------------------------------------------//

func (scheme *schemeDatabase) SchemeRegister(database *Database) (err error) {
	scheme.mutex <- true
	defer func() {
		<-scheme.mutex
	}()

	if scheme.storageStableTable == nil {
		return ErrorSchemeMustHaveTable
	}

	if database == nil && scheme.database == nil {
		return ErrorSchemeMustHaveDatabase
	}

	if database == nil && scheme.database == nil {
		return ErrorSchemeMustHaveDatabase
	}

	if database != nil && database.Transport == nil {
		return ErrorTransportIsNil
	}

	if database != nil {
		scheme.database = database
		scheme.transport = database.Transport
	}

	return
}

func (scheme *schemeDatabase) ImportV1() (err error) {
	var (
		transaction       *Transaction
		responseInterface interface{}
		responseArray     []SchemeStorageV1
		ok                bool
		storageRemote     = make(map[string]*SchemeStorageTable)
	)

	if scheme.storageV1Table == nil {
		return ErrorSchemeMustHaveTable
	}

	transaction, err = scheme.transactionOpen()
	if err != nil {
		return
	}

	defer func() {
		err = scheme.transactionClose(err)
	}()

	responseInterface, err = transaction.Query(NewBuilderSelect(scheme.storageV1Table).Where("scheme_header = \"V1\""))
	if err != nil {
		return
	}

	responseArray, ok = responseInterface.([]SchemeStorageV1)
	if !ok {
		return ErrorSchemeHasUnsupportedStruct
	}

	for _, storageField := range responseArray {
		if storageRemote[storageField.RemoteTableName] == nil {
			storageRemote[storageField.RemoteTableName] = &SchemeStorageTable{
				LocalTableName:  storageField.LocalTableName,
				RemoteTableName: storageField.RemoteTableName,
				SchemeHeader:    storageField.SchemeHeader,
				SchemeVersion:   storageField.SchemeVersion,
				FieldMap:        map[string]*SchemeStorageStable{},
			}
		}

		storageRemote[storageField.RemoteTableName].FieldMap[storageField.RemoteFieldName] = &SchemeStorageStable{
			SchemeHeader:         storageField.SchemeHeader,
			SchemeVersion:        storageField.SchemeVersion,
			LocalTableName:       storageField.LocalTableName,
			RemoteTableName:      storageField.RemoteTableName,
			LocalFieldName:       storageField.LocalFieldName,
			RemoteFieldName:      storageField.RemoteFieldName,
			LocalFieldType:       storageField.LocalFieldType,
			RemoteFieldType:      storageField.RemoteFieldType,
			FieldIndex:           storageField.FieldIndex,
			FieldIsPrimaryKey:    storageField.FieldIsPrimaryKey,
			FieldIsAutoIncrement: storageField.FieldIsAutoIncrement,
			FieldIsNotNull:       storageField.FieldIsNotNull,
			FieldInUniqueGroup:   storageField.FieldInUniqueGroup,
			FieldValueDefault:    storageField.FieldValueDefault,
			FieldValueCheck:      storageField.FieldValueCheck,
		}
	}

	scheme.storageRemote = storageRemote

	return
}

func (scheme *schemeDatabase) Import() (err error) {
	var (
		transaction        *Transaction
		sqlTx              *sql.Tx
		sqlRowArray        *sql.Rows
		storageHeaderArray []string
	)

	transaction, err = scheme.transactionOpen()
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			scheme.storageRemote = nil

			if transaction.ExecuteDropTable(scheme.storageStableTable) == nil {
				err = scheme.Export()
			}
		}

		err = scheme.transactionClose(err)
	}()

	sqlTx = scheme.database.LockSqlTx()
	scheme.database.Unlock()

	sqlRowArray, err = sqlTx.Query(fmt.Sprintf("SELECT `scheme_header` FROM `%s` GROUP BY `scheme_header`", scheme.storageName))
	if err != nil {
		return
	}

	for sqlRowArray.Next() {
		var storageHeader string
		err = sqlRowArray.Scan(&storageHeader)
		if err != nil {
			return
		}

		storageHeaderArray = append(storageHeaderArray, storageHeader)
	}

	for _, storageHeader := range storageHeaderArray {
		switch storageHeader {
		case "V1":
			err = scheme.ImportV1()
		default:
			err = ErrorSchemeHasUnsupportedHeader
		}
	}

	return
}

func (scheme *schemeDatabase) Migration(table *Table, fieldMapRemote *SchemeStorageTable, fieldMapLocal *SchemeStorageTable) (err error) {
	var (
		transaction *Transaction
		sqlTx       *sql.Tx

		fieldRemoteNameArray []string
		fieldLocalNameArray  []string
		migrationName        string
	)

	transaction, err = scheme.transactionOpen()
	if err != nil {
		return
	}

	sqlTx = scheme.database.LockSqlTx()
	scheme.database.Unlock()

	defer func() {
		err = scheme.transactionClose(err)
	}()

	for fieldRemoteName, fieldRemoteUnit := range fieldMapRemote.FieldMap {
		var (
			detected       bool
			fieldLocalName string
			fieldLocalUnit *SchemeStorageStable
		)

		for fieldLocalName, fieldLocalUnit = range fieldMapLocal.FieldMap {
			if !detected {
				if fieldRemoteName == fieldLocalName {
					detected = true
				}
			}

			if !detected {
				if fieldRemoteUnit.LocalFieldName == fieldLocalUnit.LocalFieldName {
					detected = true
				}
			}

			if detected {
				break
			}
		}

		if detected {
			fieldRemoteNameArray = append(fieldRemoteNameArray, fieldRemoteName)
			fieldLocalNameArray = append(fieldLocalNameArray, fieldLocalName)
		}
	}

	if len(fieldRemoteNameArray) == 0 {
		err = ErrorSchemeMigrationIsLimitedByVersion
		return
	}

	migrationName = fmt.Sprintf("_migration_%s", table.GetSqlName())
	err = transaction.Execute(NewBuilderCreate(table).CreateName(migrationName).IfNotExists(false))
	if err != nil {
		return
	}

	_, err = sqlTx.Exec(fmt.Sprintf("INSERT INTO `%s` (%s) SELECT %s FROM `%s`", migrationName, strings.Join(fieldLocalNameArray, ", "), strings.Join(fieldRemoteNameArray, ", "), table.GetSqlName()))
	if err != nil {
		return
	}

	_, err = sqlTx.Exec(fmt.Sprintf("DROP TABLE `%s`", table.GetSqlName()))
	if err != nil {
		return
	}

	_, err = sqlTx.Exec(fmt.Sprintf("ALTER TABLE `%s` RENAME TO `%s`", migrationName, table.GetSqlName()))
	if err != nil {
		return
	}

	return
}

func (scheme *schemeDatabase) Export() (err error) {
	var (
		transaction    *Transaction
		requestArray   []interface{}
		fieldMapRemote *SchemeStorageTable
		ok             bool
	)

	transaction, err = scheme.transactionOpen()
	if err != nil {
		return
	}

	defer func() {
		err = scheme.transactionClose(err)
	}()

	if len(scheme.storageRemote) == 0 {
		err = transaction.Execute(NewBuilderCreate(scheme.storageStableTable).IfNotExists(true))
		if err != nil {
			return
		}
	}

	for tableName, fieldMapLocal := range scheme.storageLocal {
		err = transaction.Execute(NewBuilderDelete(scheme.storageStableTable).Where(fmt.Sprintf("remote_table_name = '%s'", fieldMapLocal.RemoteTableName)))
		if err != nil {
			return
		}

		fieldMapRemote, ok = scheme.storageRemote[fieldMapLocal.RemoteTableName]
		if ok {
			err = scheme.Migration(scheme.tableMap[tableName], fieldMapRemote, fieldMapLocal)
		} else {
			err = transaction.Execute(NewBuilderCreate(scheme.tableMap[tableName]).IfNotExists(true))
		}

		if err != nil {
			return
		}

		for _, fieldUnit := range fieldMapLocal.FieldMap {
			fieldUnit.SchemeHeader = "V1"
			fieldUnit.SchemeVersion = fieldMapLocal.SchemeVersion

			requestArray = append(requestArray, SchemeStorageStable{
				SchemeHeader:         fieldUnit.SchemeHeader,
				SchemeVersion:        fieldUnit.SchemeVersion,
				LocalTableName:       fieldUnit.LocalTableName,
				RemoteTableName:      fieldUnit.RemoteTableName,
				LocalFieldName:       fieldUnit.LocalFieldName,
				RemoteFieldName:      fieldUnit.RemoteFieldName,
				LocalFieldType:       fieldUnit.LocalFieldType,
				RemoteFieldType:      fieldUnit.RemoteFieldType,
				FieldIndex:           fieldUnit.FieldIndex,
				FieldIsPrimaryKey:    fieldUnit.FieldIsPrimaryKey,
				FieldIsAutoIncrement: fieldUnit.FieldIsAutoIncrement,
				FieldIsNotNull:       fieldUnit.FieldIsNotNull,
				FieldInUniqueGroup:   fieldUnit.FieldInUniqueGroup,
				FieldValueDefault:    fieldUnit.FieldValueDefault,
				FieldValueCheck:      fieldUnit.FieldValueCheck,
			})
		}
	}

	if len(requestArray) > 0 {
		err = transaction.ExecuteReplaceValue(scheme.storageStableTable, requestArray...)
		if err != nil {
			return
		}
	}

	if len(scheme.storageRemote) == 0 {
		scheme.storageRemote = scheme.storageLocal
	} else {
		for _, tableUnit := range scheme.storageLocal {
			scheme.storageRemote[tableUnit.RemoteTableName] = tableUnit
		}
	}

	scheme.storageLocal = make(map[string]*SchemeStorageTable)

	return
}

//--------------------------------------------------------------------------------//

func (scheme *schemeDatabase) RegisterTable(tableName string, tableStruct interface{}) (*Table, error) {
	var (
		table          *Table
		fieldMapLocal  *SchemeStorageTable
		fieldMapRemote *SchemeStorageTable
		needExport     bool
		err            error
	)

	table, err = NewTable(tableName, tableStruct)
	if err != nil {
		return nil, err
	}

	fieldMapLocal, err = SchemeHelperTableToFieldMap("V1", scheme.storageVersion, table)
	if err != nil {
		return nil, err
	}

	fieldMapRemote = scheme.storageRemote[tableName]

	if fieldMapRemote == nil {
		needExport = true
	} else {
		if fieldMapRemote.SchemeVersion <= fieldMapLocal.SchemeVersion {
			if fieldMapRemote.SchemeHeader != fieldMapLocal.SchemeHeader || fieldMapRemote.SchemeVersion < fieldMapLocal.SchemeVersion || fieldMapRemote.GetHash() != fieldMapLocal.GetHash() {
				needExport = true
			}
		} else {
			err = ErrorSchemeMigrationIsLimitedByVersion
		}
	}

	if err != nil {
		return nil, err
	}

	if needExport {
		scheme.storageLocal[tableName] = fieldMapLocal
		scheme.tableMap[tableName] = table

		err = scheme.Export()
		if err != nil {
			return nil, err
		}
	}

	return table, nil
}

//--------------------------------------------------------------------------------//

func NewSchemeDatabase(storageName string, storageVersion int64) Scheme {
	var (
		storageV1Table     *Table
		storageStableTable *Table
		err                error
	)

	storageV1Table, err = NewTable(storageName, SchemeStorageV1{})
	if err != nil {
		return nil
	}

	storageStableTable, err = NewTable(storageName, SchemeStorageStable{})
	if err != nil {
		return nil
	}

	return &schemeDatabase{
		database:         nil,
		transport:        nil,
		transactionCount: 0,
		transaction:      nil,

		mutex: make(chan interface{}, 1),

		storageName:        storageName,
		storageVersion:     storageVersion,
		storageV1Table:     storageV1Table,
		storageStableTable: storageStableTable,
		storageRemote:      map[string]*SchemeStorageTable{},
		storageLocal:       map[string]*SchemeStorageTable{},
		tableMap:           map[string]*Table{},
	}
}
