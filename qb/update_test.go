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

func Test_Update_ToCql(t *testing.T) {
	t.Run("Should return right ToCql", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder

		expectedStmt, expectedNames := updateBuilder.UB.ToCql()

		// act
		resultStmt, resultNames := updateBuilder.ToCql()

		// assert
		assert.Equal(t, expectedStmt, resultStmt)
		assert.Equal(t, expectedNames, resultNames)
	})
}

func Test_Update_Query(t *testing.T) {
	t.Run("Should return right Query", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder

		expectedQuery := updateBuilder.UB.Query(*sut.session.S)

		// act
		resultQuery := updateBuilder.Query(sut.session)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*igocqlx.Queryx).Q)
	})
}

func Test_Update_QueryContext(t *testing.T) {
	t.Run("Should return right QueryContext", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		ctx := context.Background()
		expectedQueryContext := updateBuilder.UB.QueryContext(ctx, *sut.session.S)

		// act
		resultQueryContext := updateBuilder.QueryContext(ctx, sut.session)

		// assert
		assert.Equal(t, expectedQueryContext, resultQueryContext.(*igocqlx.Queryx).Q)
	})
}

func Test_Update_Table(t *testing.T) {
	t.Run("Should return right Table", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		table := "table"
		expectedTable := updateBuilder.UB.Table(table)

		// act
		resultTable := updateBuilder.Table(table)

		// assert
		assert.Equal(t, expectedTable, resultTable.(*UpdateBuilder).UB)
	})
}

func Test_Update_TTL(t *testing.T) {
	t.Run("Should return right TTL", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		d := time.Duration(10)
		expectedTTL := updateBuilder.UB.TTL(d)

		// act
		resultTTL := updateBuilder.TTL(d)

		// assert
		assert.Equal(t, expectedTTL, resultTTL.(*UpdateBuilder).UB)
	})
}

func Test_Update_TTLNamed(t *testing.T) {
	t.Run("Should return right TTLNamed", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		name := "name"
		expectedTTLNamed := updateBuilder.UB.TTLNamed(name)

		// act
		resultTTLNamed := updateBuilder.TTLNamed(name)

		// assert
		assert.Equal(t, expectedTTLNamed, resultTTLNamed.(*UpdateBuilder).UB)
	})
}

func Test_Update_Timestamp(t *testing.T) {
	t.Run("Should return right Timestamp", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		timeLayout := "2006-01-02 15:04:05 -0700 MST"
		timestamp, _ := time.Parse(timeLayout, "2017-11-11 12:05:00 +0000 UTC")
		expectedTimestamp := updateBuilder.UB.Timestamp(timestamp)

		// act
		resultTimestamp := updateBuilder.Timestamp(timestamp)

		// assert
		assert.Equal(t, expectedTimestamp, resultTimestamp.(*UpdateBuilder).UB)
	})
}

func Test_Update_TimestampNamed(t *testing.T) {
	t.Run("Should return right TimestampNamed", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		name := "2006-01-02 15:04:05 -0700 MST"
		expectedTimestampNamed := updateBuilder.UB.TimestampNamed(name)

		// act
		resultTimestampNamed := updateBuilder.TimestampNamed(name)

		// assert
		assert.Equal(t, expectedTimestampNamed, resultTimestampNamed.(*UpdateBuilder).UB)
	})
}

func Test_Update_Timeout(t *testing.T) {
	t.Run("Should return right Timeout", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		d := time.Duration(10)
		expectedTimeout := updateBuilder.UB.Timeout(d)

		// act
		resultTimeout := updateBuilder.Timeout(d)

		// assert
		assert.Equal(t, expectedTimeout, resultTimeout.(*UpdateBuilder).UB)
	})
}

func Test_Update_TimeoutNamed(t *testing.T) {
	t.Run("Should return right TimeoutNamed", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		name := "name"
		expectedTimeoutNamed := updateBuilder.UB.TimeoutNamed(name)

		// act
		resultTimeoutNamed := updateBuilder.TimeoutNamed(name)

		// assert
		assert.Equal(t, expectedTimeoutNamed, resultTimeoutNamed.(*UpdateBuilder).UB)
	})
}

func Test_Update_Set(t *testing.T) {
	t.Run("Should return right Set", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		columns := "columns"
		expectedSet := updateBuilder.UB.Set(columns)

		// act
		resultSet := updateBuilder.Set(columns)

		// assert
		assert.Equal(t, expectedSet, resultSet.(*UpdateBuilder).UB)
	})
}

func Test_Update_SetNamed(t *testing.T) {
	t.Run("Should return right SetNamed", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		name := "name"
		expectedSetNamed := updateBuilder.UB.SetNamed(column, name)

		// act
		resultSetNamed := updateBuilder.SetNamed(column, name)

		// assert
		assert.Equal(t, expectedSetNamed, resultSetNamed.(*UpdateBuilder).UB)
	})
}

func Test_Update_SetLit(t *testing.T) {
	t.Run("Should return right SetLit", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		literal := "literal"
		expectedSetLit := updateBuilder.UB.SetLit(column, literal)

		// act
		resultSetLit := updateBuilder.SetLit(column, literal)

		// assert
		assert.Equal(t, expectedSetLit, resultSetLit.(*UpdateBuilder).UB)
	})
}

func Test_Update_SetFunc(t *testing.T) {
	t.Run("Should return right SetFunc", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		fn := &qb.Func{}
		expectedSetFunc := updateBuilder.UB.SetFunc(column, fn)

		// act
		resultSetFunc := updateBuilder.SetFunc(column, fn)

		// assert
		assert.Equal(t, expectedSetFunc, resultSetFunc.(*UpdateBuilder).UB)
	})
}

func Test_Update_SetTuple(t *testing.T) {
	t.Run("Should return right SetTuple", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		count := 10
		expectedSetTuple := updateBuilder.UB.SetTuple(column, count)

		// act
		resultSetTuple := updateBuilder.SetTuple(column, count)

		// assert
		assert.Equal(t, expectedSetTuple, resultSetTuple.(*UpdateBuilder).UB)
	})
}

func Test_Update_Add(t *testing.T) {
	t.Run("Should return right Add", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		expectedAdd := updateBuilder.UB.Add(column)

		// act
		resultAdd := updateBuilder.Add(column)

		// assert
		assert.Equal(t, expectedAdd, resultAdd.(*UpdateBuilder).UB)
	})
}

func Test_Update_AddNamed(t *testing.T) {
	t.Run("Should return right AddNamed", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		name := "name"
		expectedAddNamed := updateBuilder.UB.AddNamed(column, name)

		// act
		resultAddNamed := updateBuilder.AddNamed(column, name)

		// assert
		assert.Equal(t, expectedAddNamed, resultAddNamed.(*UpdateBuilder).UB)
	})
}

func Test_Update_AddLit(t *testing.T) {
	t.Run("Should return right AddLit", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		literal := "literal"
		expectedAddLit := updateBuilder.UB.AddLit(column, literal)

		// act
		resultAddLit := updateBuilder.AddLit(column, literal)

		// assert
		assert.Equal(t, expectedAddLit, resultAddLit.(*UpdateBuilder).UB)
	})
}

func Test_Update_AddFunc(t *testing.T) {
	t.Run("Should return right AddFunc", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		fn := &qb.Func{}
		expectedAddFunc := updateBuilder.UB.AddFunc(column, fn)

		// act
		resultAddFunc := updateBuilder.AddFunc(column, fn)

		// assert
		assert.Equal(t, expectedAddFunc, resultAddFunc.(*UpdateBuilder).UB)
	})
}

func Test_Update_Remove(t *testing.T) {
	t.Run("Should return right Remove", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		expectedRemove := updateBuilder.UB.Remove(column)

		// act
		resultRemove := updateBuilder.Remove(column)

		// assert
		assert.Equal(t, expectedRemove, resultRemove.(*UpdateBuilder).UB)
	})
}

func Test_Update_RemoveNamed(t *testing.T) {
	t.Run("Should return right RemoveNamed", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		name := "name"
		expectedRemoveNamed := updateBuilder.UB.RemoveNamed(column, name)

		// act
		resultRemoveNamed := updateBuilder.RemoveNamed(column, name)

		// assert
		assert.Equal(t, expectedRemoveNamed, resultRemoveNamed.(*UpdateBuilder).UB)
	})
}

func Test_Update_RemoveLit(t *testing.T) {
	t.Run("Should return right RemoveLit", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		literal := "literal"
		expectedRemoveLit := updateBuilder.UB.RemoveLit(column, literal)

		// act
		resultRemoveLit := updateBuilder.RemoveLit(column, literal)

		// assert
		assert.Equal(t, expectedRemoveLit, resultRemoveLit.(*UpdateBuilder).UB)
	})
}

func Test_Update_RemoveFunc(t *testing.T) {
	t.Run("Should return right RemoveFunc", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		column := "column"
		fn := &qb.Func{}
		expectedRemoveFunc := updateBuilder.UB.RemoveFunc(column, fn)

		// act
		resultRemoveFunc := updateBuilder.RemoveFunc(column, fn)

		// assert
		assert.Equal(t, expectedRemoveFunc, resultRemoveFunc.(*UpdateBuilder).UB)
	})
}

func Test_Update_Where(t *testing.T) {
	t.Run("Should return right Where", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		w := qb.Cmp{}
		expectedWhere := updateBuilder.UB.Where(w)

		// act
		resultWhere := updateBuilder.Where(w)

		// assert
		assert.Equal(t, expectedWhere, resultWhere.(*UpdateBuilder).UB)
	})
}

func Test_Update_If(t *testing.T) {
	t.Run("Should return right If", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		w := qb.Cmp{}
		expectedIf := updateBuilder.UB.If(w)

		// act
		resultIf := updateBuilder.If(w)

		// assert
		assert.Equal(t, expectedIf, resultIf.(*UpdateBuilder).UB)
	})
}

func Test_Update_Existing(t *testing.T) {
	t.Run("Should return right Existing", func(t *testing.T) {
		// arrange
		sut := MakeUpdateBuilderSut()
		updateBuilder := sut.UpdateBuilder
		expectedExisting := updateBuilder.UB.Existing()

		// act
		resultExisting := updateBuilder.Existing()

		// assert
		assert.Equal(t, expectedExisting, resultExisting.(*UpdateBuilder).UB)
	})
}

/* ============= SUT helpers ============ */

type updateBuilderSut struct {
	UpdateBuilder *UpdateBuilder
	session       *igocqlx.Session
}

func MakeUpdateBuilderSut() updateBuilderSut {
	gocqlxUpdateBuilder := MakeGocqlxUpdateBuilderSut()

	session := igocqlx.Session{
		S: &gocqlx.Session{
			Session: &gocql.Session{},
		},
	}

	return updateBuilderSut{
		UpdateBuilder: &UpdateBuilder{
			UB: gocqlxUpdateBuilder.Sut,
		},
		session: &session,
	}
}

type updateBuilderGocqlxSut struct {
	Sut *qb.UpdateBuilder
}

func MakeGocqlxUpdateBuilderSut() updateBuilderGocqlxSut {
	sut := &qb.UpdateBuilder{}

	return updateBuilderGocqlxSut{
		Sut: sut,
	}
}
