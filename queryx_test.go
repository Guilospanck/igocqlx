package igocqlx

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/scylladb/gocqlx/v2"
	"github.com/stretchr/testify/assert"
)

func GetUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

func Test_Queryx_WithBindTransformer(t *testing.T) {
	t.Run("", func(t *testing.T) {
		// arrange
		sut := makeQueryxStruct()

		// act
		result := sut.query.WithBindTransformer(sut.tr)

		queryInsideResult := result.(*Queryx).Q
		unexportedTr := GetUnexportedField(reflect.ValueOf(queryInsideResult).Elem().FieldByName("tr"))

		// assert
		assert.IsType(t, sut.tr, unexportedTr)
		assert.Equal(t, sut.query, result)
		assert.Equal(t, result.(*Queryx).Q, sut.gocqlx_query)
	})
}

type queryxStruct struct {
	gocqlx_query *gocqlx.Queryx
	query        *Queryx
	tr           gocqlx.Transformer
}

func makeQueryxStruct() queryxStruct {
	gocqlx_query := &gocqlx.Queryx{}
	query := &Queryx{
		Q: gocqlx_query,
	}

	tr := func(name string, val interface{}) interface{} { return nil }

	return queryxStruct{
		gocqlx_query,
		query,
		tr,
	}
}
