package postgres

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
)

// Dictionary is a postgres safe alias of map[string]string. This object can be inserted and
// retrieved from the database without the user having to marshal or unmarshal a dictionary.
type Dictionary map[string]string

// Value implements the driver.Value interface. It returns the json bytes representation
// of this dictionary, which is a supported type of postgres. (Used implicitely during INSERT)
func (d Dictionary) Value() (driver.Value, error) {
	bytes, err := json.Marshal(d)
	return bytes, err
}

// Scan implements the sql.scanner interface, unmarshalling a json byte representation of
// a dictionary into this dictionary. (Used implicitely during SELECT)
func (d *Dictionary) Scan(src any) error {
	bytes, ok := src.([]byte)
	if !ok {
		return errors.New("Type assertion failed")
	}
	err := json.Unmarshal(bytes, d)
	return err
}

// NewNullString returns a sql.NullString value from an input string
func NewNullString(s string) sql.NullString {
	return sql.NullString{
		String: s,
		Valid:  len(s) != 0,
	}
}
