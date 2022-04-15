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

func Test_Delete_ToCql(t *testing.T) {
	t.Run("Should return right ToCql", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder

		expectedStmt, expectedNames := deleteBuilder.DB.ToCql()

		// act
		resultStmt, resultNames := deleteBuilder.ToCql()

		// assert
		assert.Equal(t, expectedStmt, resultStmt)
		assert.Equal(t, expectedNames, resultNames)
	})
}

func Test_Delete_Query(t *testing.T) {
	t.Run("Should return right Query", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder

		expectedQuery := deleteBuilder.DB.Query(*sut.session.S)

		// act
		resultQuery := deleteBuilder.Query(sut.session)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*igocqlx.Queryx).Q)
	})
}

func Test_Delete_QueryContext(t *testing.T) {
	t.Run("Should return right QueryContext", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder
		ctx := context.Background()
		expectedQueryContext := deleteBuilder.DB.QueryContext(ctx, *sut.session.S)

		// act
		resultQueryContext := deleteBuilder.QueryContext(ctx, sut.session)

		// assert
		assert.Equal(t, expectedQueryContext, resultQueryContext.(*igocqlx.Queryx).Q)
	})
}

func Test_Delete_From(t *testing.T) {
	t.Run("Should return right From", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder
		table := "table"
		expectedFrom := deleteBuilder.DB.From(table)

		// act
		resultFrom := deleteBuilder.From(table)

		// assert
		assert.Equal(t, expectedFrom, resultFrom.(*DeleteBuilder).DB)
	})
}

func Test_Delete_Columns(t *testing.T) {
	t.Run("Should return right Columns", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder
		columns := "columns"
		expectedColumns := deleteBuilder.DB.Columns(columns)

		// act
		resultColumns := deleteBuilder.Columns(columns)

		// assert
		assert.Equal(t, expectedColumns, resultColumns.(*DeleteBuilder).DB)
	})
}

func Test_Delete_Timestamp(t *testing.T) {
	t.Run("Should return right Timestamp", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder
		timeLayout := "2006-01-02 15:04:05 -0700 MST"
		timestamp, _ := time.Parse(timeLayout, "2017-11-11 12:05:00 +0000 UTC")
		expectedTimestamp := deleteBuilder.DB.Timestamp(timestamp)

		// act
		resultTimestamp := deleteBuilder.Timestamp(timestamp)

		// assert
		assert.Equal(t, expectedTimestamp, resultTimestamp.(*DeleteBuilder).DB)
	})
}

func Test_Delete_TimestampNamed(t *testing.T) {
	t.Run("Should return right TimestampNamed", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder
		name := "2006-01-02 15:04:05 -0700 MST"
		expectedTimestampNamed := deleteBuilder.DB.TimestampNamed(name)

		// act
		resultTimestampNamed := deleteBuilder.TimestampNamed(name)

		// assert
		assert.Equal(t, expectedTimestampNamed, resultTimestampNamed.(*DeleteBuilder).DB)
	})
}

func Test_Delete_Timeout(t *testing.T) {
	t.Run("Should return right Timeout", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder
		d := time.Duration(10)
		expectedTimeout := deleteBuilder.DB.Timeout(d)

		// act
		resultTimeout := deleteBuilder.Timeout(d)

		// assert
		assert.Equal(t, expectedTimeout, resultTimeout.(*DeleteBuilder).DB)
	})
}

func Test_Delete_TimeoutNamed(t *testing.T) {
	t.Run("Should return right TimeoutNamed", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder
		name := "name"
		expectedTimeoutNamed := deleteBuilder.DB.TimeoutNamed(name)

		// act
		resultTimeoutNamed := deleteBuilder.TimeoutNamed(name)

		// assert
		assert.Equal(t, expectedTimeoutNamed, resultTimeoutNamed.(*DeleteBuilder).DB)
	})
}

func Test_Delete_Where(t *testing.T) {
	t.Run("Should return right Where", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder
		w := qb.Cmp{}
		expectedWhere := deleteBuilder.DB.Where(w)

		// act
		resultWhere := deleteBuilder.Where(w)

		// assert
		assert.Equal(t, expectedWhere, resultWhere.(*DeleteBuilder).DB)
	})
}

func Test_Delete_If(t *testing.T) {
	t.Run("Should return right If", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder
		w := qb.Cmp{}
		expectedIf := deleteBuilder.DB.If(w)

		// act
		resultIf := deleteBuilder.If(w)

		// assert
		assert.Equal(t, expectedIf, resultIf.(*DeleteBuilder).DB)
	})
}

func Test_Delete_Existing(t *testing.T) {
	t.Run("Should return right Existing", func(t *testing.T) {
		// arrange
		sut := MakeDeleteBuilderSut()
		deleteBuilder := sut.DeleteBuilder
		expectedExisting := deleteBuilder.DB.Existing()

		// act
		resultExisting := deleteBuilder.Existing()

		// assert
		assert.Equal(t, expectedExisting, resultExisting.(*DeleteBuilder).DB)
	})
}

/* ============= SUT helpers ============ */

type deleteBuilderSut struct {
	DeleteBuilder *DeleteBuilder
	session       *igocqlx.Session
}

func MakeDeleteBuilderSut() deleteBuilderSut {
	gocqlxDeleteBuilder := MakeGocqlxDeleteBuilderSut()

	session := igocqlx.Session{
		S: &gocqlx.Session{
			Session: &gocql.Session{},
		},
	}

	return deleteBuilderSut{
		DeleteBuilder: &DeleteBuilder{
			DB: gocqlxDeleteBuilder.Sut,
		},
		session: &session,
	}
}

type deleteBuilderGocqlxSut struct {
	Sut *qb.DeleteBuilder
}

func MakeGocqlxDeleteBuilderSut() deleteBuilderGocqlxSut {
	sut := &qb.DeleteBuilder{}

	return deleteBuilderGocqlxSut{
		Sut: sut,
	}
}
