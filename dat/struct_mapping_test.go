package dat

import (
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func TestEmbeddedStructMapping(t *testing.T) {
	type Realm struct {
		RealmUUID string `db:"realm_uuid"`
	}
	type Group struct {
		GroupUUID string `db:"group_uuid"`
		*Realm
	}

	g := &Group{Realm: &Realm{"11"}, GroupUUID: "22"}

	sql, args, err := InsertInto("groups").Columns("group_uuid", "realm_uuid").Record(g).ToSQL()
	assert.NoError(t, err)
	expected := `
		INSERT INTO groups (group_uuid, realm_uuid)
		VALUES ($1, $2)
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Exactly(t, []interface{}{"22", "11"}, args)
}

func TestEmbeddedStructInvalidColumns(t *testing.T) {
	type Realm struct {
		RealmUUID string
	}
	type Group struct {
		GroupUUID string `db:"group_uuid"`
		*Realm
	}

	g := &Group{Realm: &Realm{"11"}, GroupUUID: "22"}

	// realm_uuid must be explicitly defined
	_, _, err := InsertInto("groups").Columns("group_uuid", "realm_uuid").Record(g).ToSQL()
	assert.Error(t, err)
}
