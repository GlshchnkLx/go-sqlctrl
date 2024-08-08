package sqlctrl

import "fmt"

//--------------------------------------------------------------------------------//

var (
	ErrorTableFieldMustHaveATable    = fmt.Errorf("table: field must have a table")
	ErrorTableReferenceIsNil         = fmt.Errorf("table: reference is nil")
	ErrorTableReferenceIsUnsupported = fmt.Errorf("table: reference is unsupported")
	ErrorTableReferenceIsUncorrected = fmt.Errorf("table: reference is uncorrected")
)

var (
	ErrorBuilderMustHaveATable                = fmt.Errorf("builder: must have a table")
	ErrorBuilderMustHaveATableWithName        = fmt.Errorf("builder: must have a table with name")
	ErroroBuilderTableHasUnsupportedReferense = fmt.Errorf("builder: table has is unsupported reference")
)

var (
	ErrorGenericInvalidArgument = fmt.Errorf("generic: invalid argument")

	ErrorTableIsNil                 = fmt.Errorf("table: is nil")
	ErrorTableMustHaveAutoincrement = fmt.Errorf("table: must have AUTO_INCREMENT")

	ErrorBuilderIsNil           = fmt.Errorf("builder: is nil")
	ErrorBuilderWithoutResponse = fmt.Errorf("builder: without response")

	ErrorResponseLessThanRequested = fmt.Errorf("response: less than requested")
	ErrorResponseMoreThanRequested = fmt.Errorf("response: less than requested")

	ErrorDatabaseIsNil                 = fmt.Errorf("database: is nil")
	ErrorDatabaseIsAlreadyHasTransport = fmt.Errorf("database: is already has transpport")

	ErrorTransportIsNil            = fmt.Errorf("transport: is nil")
	ErrorTransportIsAlreadyOpened  = fmt.Errorf("transport: is already opened")
	ErrorTransportIsAlreadyClosed  = fmt.Errorf("transport: is already closed")
	ErrorTransportMustHaveDatabase = fmt.Errorf("transport: must have database")

	ErrorTransactionIsAlreadyOpened = fmt.Errorf("transaction: is already opened")
	ErrorTransactionIsAlreadyClosed = fmt.Errorf("transaction: is already closed")

	ErrorSchemeIsNil            = fmt.Errorf("scheme: is nil")
	ErrorSchemeMustHaveTable    = fmt.Errorf("scheme: must have table")
	ErrorSchemeMustHaveDatabase = fmt.Errorf("scheme: must have database")

	ErrorSchemeTransactionIsAlreadyOpened = fmt.Errorf("scheme: transaction is already opened")
	ErrorSchemeTransactionIsAlreadyClosed = fmt.Errorf("scheme: transaction is already closed")
	ErrorSchemeTransactionHasUnknownState = fmt.Errorf("scheme: transaction has unknown state")

	ErrorSchemeHasUnsupportedStruct        = fmt.Errorf("scheme: has unsupported struct")
	ErrorSchemeHasUnsupportedHeader        = fmt.Errorf("scheme: has unsupported header")
	ErrorSchemeMigrationIsLimitedByVersion = fmt.Errorf("scheme: migration is limited by version")
)
