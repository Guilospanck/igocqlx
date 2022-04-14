package igocqlx

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/scylladb/gocqlx/v2"
	"github.com/stretchr/testify/assert"
)

type TrackingDataEntity struct {
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Location  string `db:"location"`
}

func makeArg() *TrackingDataEntity {
	return &TrackingDataEntity{
		FirstName: "Guilherme",
		LastName:  "Rodrigues",
		Location:  "Brazil",
	}
}

func makeArgArrayInterface() []interface{} {
	return []interface{}{
		"Guilherme",
		"Rodrigues",
		"Brazil",
	}
}

func GetUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

func Test_Queryx_WithBindTransformer(t *testing.T) {
	t.Run("Should call WithBindTransformer and perform right actions", func(t *testing.T) {
		// arrange
		sut := makeQueryxStruct()

		// act
		result := sut.query.WithBindTransformer(sut.tr)

		/* Getting private field from struct */
		queryInsideResult := result.(*Queryx).Q
		field := reflect.ValueOf(queryInsideResult).Elem().FieldByName("tr")
		unexportedTr := GetUnexportedField(field)

		// assert
		assert.IsType(t, sut.tr, unexportedTr)
		assert.Equal(t, sut.query, result)
	})
}

// https://github.com/scylladb/gocqlx/blob/master/queryx_test.go
func Test_Queryx_BindStruct(t *testing.T) {
	t.Run("Should call Test_Queryx_BindStruct and perform right actions", func(t *testing.T) {
		// arrange
		sut := makeQueryxStruct()
		arg := makeArg()

		// act
		result, err := sut.query.bindStructArgs(arg, nil)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, makeArgArrayInterface(), result)
	})
}

type queryxStruct struct {
	query       *Queryx
	gocqlxquery *gocqlx.Queryx
	tr          gocqlx.Transformer

	stmt  string
	names []string
}

func makeQueryxStruct() queryxStruct {
	tr := func(name string, val interface{}) interface{} { return val }

	stmt := `INSERT INTO tracking_data (first_name,last_name,location) VALUES (?,?,?) `
	names := []string{"first_name", "last_name", "location"}

	query := makeQuery(names)
	gocqlxquery := query.Q

	return queryxStruct{
		query,
		gocqlxquery,
		tr,
		stmt,
		names,
	}
}

func makeQuery(names []string) *Queryx {
	gocqlxquery := gocqlx.Query(nil, names)

	return &Queryx{
		Q: gocqlxquery,
	}
}
