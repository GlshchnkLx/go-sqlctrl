package sqlctrl

import "fmt"

var (
	errValueMustBeAStructure          = fmt.Errorf("value must be a structure")
	errValueMustBeAStructureOrPointer = fmt.Errorf("value must be a structure or pointer")

	errValueDoesNotMatchTableType = fmt.Errorf("value does not match a table type")
)

var (
	errTableIsNull          = fmt.Errorf("table is null")
	errTableAlreadyExists   = fmt.Errorf("table already exists")
	errTableDoesNotExists   = fmt.Errorf("table does not exists")
	errTableDoesNotMigtated = fmt.Errorf("table does not migrated")

	errTableDoesNotHaveAutoIncrement = fmt.Errorf("table does not have auto increment")
	errTableDidNotInsertTheValue     = fmt.Errorf("table did not insert the value")
	errTableDidNotReplaceTheValue    = fmt.Errorf("table did not replace the value")
	errTableDidNotUpdateTheValue     = fmt.Errorf("table did not update the value")
	errTableDidNotDeleteTheValue     = fmt.Errorf("table did not delete the value")

	errResponseLessThanRequested = fmt.Errorf("response less than requested")
	errResponseMoreThanRequested = fmt.Errorf("response more than requested")
)
