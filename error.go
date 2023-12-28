package sqlctrl

import "fmt"

var (
	errValueMustBeAStructure                 = fmt.Errorf("value must be a structure")
	errValueMustBeAStructureOrPointer        = fmt.Errorf("value must be a structure or pointer")
	errValueMustBeAStructureOrStructureArray = fmt.Errorf("value must be a structure or []struct")
	errUnsupportedFieldType                  = fmt.Errorf("unsupported field type")

	errRequestMustBeAStringOrStringArray = fmt.Errorf("request must be a string or []string")

	errValueDoesNotMatchTableType = fmt.Errorf("value does not match a table type")
)

var (
	errTableIsNull                   = fmt.Errorf("table is null")
	errTableDoesNotHaveAutoIncrement = fmt.Errorf("table does not have auto increment")
	errTableAlreadyExists            = fmt.Errorf("table already exists")
	errTableDoesNotExists            = fmt.Errorf("table does not exists")
	errTableDoesNotMigtated          = fmt.Errorf("table does not migrated")

	errTableDidNotInsertTheValue = fmt.Errorf("table did not insert the value")
	errTableDidNotUpdateTheValue = fmt.Errorf("table did not update the value")
	errTableDidNotDeleteTheValue = fmt.Errorf("table did not delete the value")
)
