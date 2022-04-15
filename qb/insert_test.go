package igocqlxqb

import (
	"context"
	"testing"
	"time"

	"github.com/Guilospanck/igocqlx"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/stretchr/testify/assert"
)

func Test_Insert_ToCql(t *testing.T) {
	t.Run("Should return right ToCql", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder

		expectedStmt, expectedNames := insertBuilder.IB.ToCql()

		// act
		resultStmt, resultNames := insertBuilder.ToCql()

		// assert
		assert.Equal(t, expectedStmt, resultStmt)
		assert.Equal(t, expectedNames, resultNames)
	})
}

func Test_Insert_Query(t *testing.T) {
	t.Run("Should return right Query", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder

		expectedQuery := insertBuilder.IB.Query(*sut.session.S)

		// act
		resultQuery := insertBuilder.Query(sut.session)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*igocqlx.Queryx).Q)
	})
}

func Test_Insert_QueryContext(t *testing.T) {
	t.Run("Should return right QueryContext", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		ctx := context.Background()
		expectedQueryContext := insertBuilder.IB.QueryContext(ctx, *sut.session.S)

		// act
		resultQueryContext := insertBuilder.QueryContext(ctx, sut.session)

		// assert
		assert.Equal(t, expectedQueryContext, resultQueryContext.(*igocqlx.Queryx).Q)
	})
}

func Test_Insert_Into(t *testing.T) {
	t.Run("Should return right Into", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		table := "table"
		expectedInto := insertBuilder.IB.Into(table)

		// act
		resultInto := insertBuilder.Into(table)

		// assert
		assert.Equal(t, expectedInto, resultInto.(*InsertBuilder).IB)
	})
}

func Test_Insert_Json(t *testing.T) {
	t.Run("Should return right Json", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		expectedJson := insertBuilder.IB.Json()

		// act
		resultJson := insertBuilder.Json()

		// assert
		assert.Equal(t, expectedJson, resultJson.(*InsertBuilder).IB)
	})
}

func Test_Insert_Columns(t *testing.T) {
	t.Run("Should return right Columns", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		columns := "columns"
		expectedColumns := insertBuilder.IB.Columns(columns)

		// act
		resultColumns := insertBuilder.Columns(columns)

		// assert
		assert.Equal(t, expectedColumns, resultColumns.(*InsertBuilder).IB)
	})
}

func Test_Insert_NamedColumn(t *testing.T) {
	t.Run("Should return right NamedColumn", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		column := "column"
		name := "name"
		expectedNamedColumn := insertBuilder.IB.NamedColumn(column, name)

		// act
		resultNamedColumn := insertBuilder.NamedColumn(column, name)

		// assert
		assert.Equal(t, expectedNamedColumn, resultNamedColumn.(*InsertBuilder).IB)
	})
}

func Test_Insert_LitColumn(t *testing.T) {
	t.Run("Should return right LitColumn", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		column := "column"
		name := "name"
		expectedLitColumn := insertBuilder.IB.LitColumn(column, name)

		// act
		resultLitColumn := insertBuilder.LitColumn(column, name)

		// assert
		assert.Equal(t, expectedLitColumn, resultLitColumn.(*InsertBuilder).IB)
	})
}

func Test_Insert_FuncColumn(t *testing.T) {
	t.Run("Should return right FuncColumn", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		column := "column"
		fn := &qb.Func{}
		expectedFuncColumn := insertBuilder.IB.FuncColumn(column, fn)

		// act
		resultFuncColumn := insertBuilder.FuncColumn(column, fn)

		// assert
		assert.Equal(t, expectedFuncColumn, resultFuncColumn.(*InsertBuilder).IB)
	})
}

func Test_Insert_TupleColumn(t *testing.T) {
	t.Run("Should return right TupleColumn", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		column := "column"
		count := 1
		expectedTupleColumn := insertBuilder.IB.TupleColumn(column, count)

		// act
		resultTupleColumn := insertBuilder.TupleColumn(column, count)

		// assert
		assert.Equal(t, expectedTupleColumn, resultTupleColumn.(*InsertBuilder).IB)
	})
}

func Test_Insert_Unique(t *testing.T) {
	t.Run("Should return right Unique", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		expectedUnique := insertBuilder.IB.Unique()

		// act
		resultUnique := insertBuilder.Unique()

		// assert
		assert.Equal(t, expectedUnique, resultUnique.(*InsertBuilder).IB)
	})
}

func Test_Insert_TTL(t *testing.T) {
	t.Run("Should return right TTL", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		d := time.Duration(10)
		expectedTTL := insertBuilder.IB.TTL(d)

		// act
		resultTTL := insertBuilder.TTL(d)

		// assert
		assert.Equal(t, expectedTTL, resultTTL.(*InsertBuilder).IB)
	})
}

func Test_Insert_TTLNamed(t *testing.T) {
	t.Run("Should return right TTLNamed", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		name := "name"
		expectedTTLNamed := insertBuilder.IB.TTLNamed(name)

		// act
		resultTTLNamed := insertBuilder.TTLNamed(name)

		// assert
		assert.Equal(t, expectedTTLNamed, resultTTLNamed.(*InsertBuilder).IB)
	})
}

func Test_Insert_Timestamp(t *testing.T) {
	t.Run("Should return right Timestamp", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		timeLayout := "2006-01-02 15:04:05 -0700 MST"
		timestamp, _ := time.Parse(timeLayout, "2017-11-11 12:05:00 +0000 UTC")
		expectedTimestamp := insertBuilder.IB.Timestamp(timestamp)

		// act
		resultTimestamp := insertBuilder.Timestamp(timestamp)

		// assert
		assert.Equal(t, expectedTimestamp, resultTimestamp.(*InsertBuilder).IB)
	})
}

func Test_Insert_TimestampNamed(t *testing.T) {
	t.Run("Should return right TimestampNamed", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		name := "2006-01-02 15:04:05 -0700 MST"
		expectedTimestampNamed := insertBuilder.IB.TimestampNamed(name)

		// act
		resultTimestampNamed := insertBuilder.TimestampNamed(name)

		// assert
		assert.Equal(t, expectedTimestampNamed, resultTimestampNamed.(*InsertBuilder).IB)
	})
}

func Test_Insert_Timeout(t *testing.T) {
	t.Run("Should return right Timeout", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		d := time.Duration(10)
		expectedTimeout := insertBuilder.IB.Timeout(d)

		// act
		resultTimeout := insertBuilder.Timeout(d)

		// assert
		assert.Equal(t, expectedTimeout, resultTimeout.(*InsertBuilder).IB)
	})
}

func Test_Insert_TimeoutNamed(t *testing.T) {
	t.Run("Should return right TimeoutNamed", func(t *testing.T) {
		// arrange
		sut := MakeInsertBuilderSut()
		insertBuilder := sut.InsertBuilder
		name := "name"
		expectedTimeoutNamed := insertBuilder.IB.TimeoutNamed(name)

		// act
		resultTimeoutNamed := insertBuilder.TimeoutNamed(name)

		// assert
		assert.Equal(t, expectedTimeoutNamed, resultTimeoutNamed.(*InsertBuilder).IB)
	})
}

/* ============= SUT helpers ============ */

type insertBuilderSut struct {
	InsertBuilder *InsertBuilder
	session       *igocqlx.Session
}

func MakeInsertBuilderSut() insertBuilderSut {
	gocqlxInsertBuilder := MakeGocqlxInsertBuilderSut()

	session := igocqlx.Session{
		S: &gocqlx.Session{
			Session: &gocql.Session{},
		},
	}

	return insertBuilderSut{
		InsertBuilder: &InsertBuilder{
			IB: gocqlxInsertBuilder.Sut,
		},
		session: &session,
	}
}

type insertBuilderGocqlxSut struct {
	Sut *qb.InsertBuilder
}

func MakeGocqlxInsertBuilderSut() insertBuilderGocqlxSut {
	sut := &qb.InsertBuilder{}

	return insertBuilderGocqlxSut{
		Sut: sut,
	}
}
