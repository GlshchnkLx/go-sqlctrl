package sqlctrl

import "fmt"

var (
	ErrValueMustBeAStructure          = fmt.Errorf("value must be a structure")
	ErrValueMustBeAStructureOrPointer = fmt.Errorf("value must be a structure or pointer")

	ErrValueDoesNotMatchTableType = fmt.Errorf("value does not match a table type")
)

var (
	ErrInvalidArgument      = fmt.Errorf("invalid argument received")
	ErrTableIsNull          = fmt.Errorf("table is null")
	ErrTableAlreadyExists   = fmt.Errorf("table already exists")
	ErrTableDoesNotExists   = fmt.Errorf("table does not exists")
	ErrTableDoesNotMigtated = fmt.Errorf("table does not migrated")

	ErrTableDoesNotHaveAutoIncrement = fmt.Errorf("table does not have auto increment")
	ErrTableDidNotInsertTheValue     = fmt.Errorf("table did not insert the value")
	ErrTableDidNotReplaceTheValue    = fmt.Errorf("table did not replace the value")
	ErrTableDidNotUpdateTheValue     = fmt.Errorf("table did not update the value")
	ErrTableDidNotDeleteTheValue     = fmt.Errorf("table did not delete the value")

	ErrResponseLessThanRequested = fmt.Errorf("response less than requested")
	ErrResponseMoreThanRequested = fmt.Errorf("response more than requested")
)
