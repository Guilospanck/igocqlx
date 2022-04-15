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

func Test_Select_ToCql(t *testing.T) {
	t.Run("Should return right ToCql", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder

		expectedStmt, expectedNames := selectBuilder.SB.ToCql()

		// act
		resultStmt, resultNames := selectBuilder.ToCql()

		// assert
		assert.Equal(t, expectedStmt, resultStmt)
		assert.Equal(t, expectedNames, resultNames)
	})
}

func Test_Select_Query(t *testing.T) {
	t.Run("Should return right Query", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder

		expectedQuery := selectBuilder.SB.Query(*sut.session.S)

		// act
		resultQuery := selectBuilder.Query(sut.session)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*igocqlx.Queryx).Q)
	})
}

func Test_Select_QueryContext(t *testing.T) {
	t.Run("Should return right QueryContext", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		ctx := context.Background()
		expectedQueryContext := selectBuilder.SB.QueryContext(ctx, *sut.session.S)

		// act
		resultQueryContext := selectBuilder.QueryContext(ctx, sut.session)

		// assert
		assert.Equal(t, expectedQueryContext, resultQueryContext.(*igocqlx.Queryx).Q)
	})
}

func Test_Select_From(t *testing.T) {
	t.Run("Should return right From", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		table := "table"
		expectedFrom := selectBuilder.SB.From(table)

		// act
		resultFrom := selectBuilder.From(table)

		// assert
		assert.Equal(t, expectedFrom, resultFrom.(*SelectBuilder).SB)
	})
}

func Test_Select_Json(t *testing.T) {
	t.Run("Should return right Json", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		expectedJson := selectBuilder.SB.Json()

		// act
		resultJson := selectBuilder.Json()

		// assert
		assert.Equal(t, expectedJson, resultJson.(*SelectBuilder).SB)
	})
}

func Test_Select_Columns(t *testing.T) {
	t.Run("Should return right Columns", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		columns := "columns"
		expectedColumns := selectBuilder.SB.Columns(columns)

		// act
		resultColumns := selectBuilder.Columns(columns)

		// assert
		assert.Equal(t, expectedColumns, resultColumns.(*SelectBuilder).SB)
	})
}

func Test_Select_Distinct(t *testing.T) {
	t.Run("Should return right Distinct", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		columns := "columns"
		expectedDistinct := selectBuilder.SB.Distinct(columns)

		// act
		resultDistinct := selectBuilder.Distinct(columns)

		// assert
		assert.Equal(t, expectedDistinct, resultDistinct.(*SelectBuilder).SB)
	})
}

func Test_Select_Timeout(t *testing.T) {
	t.Run("Should return right Timeout", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		d := time.Duration(10)
		expectedTimeout := selectBuilder.SB.Timeout(d)

		// act
		resultTimeout := selectBuilder.Timeout(d)

		// assert
		assert.Equal(t, expectedTimeout, resultTimeout.(*SelectBuilder).SB)
	})
}

func Test_Select_TimeoutNamed(t *testing.T) {
	t.Run("Should return right TimeoutNamed", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		name := "name"
		expectedTimeoutNamed := selectBuilder.SB.TimeoutNamed(name)

		// act
		resultTimeoutNamed := selectBuilder.TimeoutNamed(name)

		// assert
		assert.Equal(t, expectedTimeoutNamed, resultTimeoutNamed.(*SelectBuilder).SB)
	})
}

func Test_Select_Where(t *testing.T) {
	t.Run("Should return right Where", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		updateBuilder := sut.SelectBuilder
		w := qb.Cmp{}
		expectedWhere := updateBuilder.SB.Where(w)

		// act
		resultWhere := updateBuilder.Where(w)

		// assert
		assert.Equal(t, expectedWhere, resultWhere.(*SelectBuilder).SB)
	})
}

func Test_Select_GroupBy(t *testing.T) {
	t.Run("Should return right GroupBy", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		columns := "columns"
		expectedGroupBy := selectBuilder.SB.GroupBy(columns)

		// act
		resultGroupBy := selectBuilder.GroupBy(columns)

		// assert
		assert.Equal(t, expectedGroupBy, resultGroupBy.(*SelectBuilder).SB)
	})
}

func Test_Select_OrderBy(t *testing.T) {
	t.Run("Should return right OrderBy", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		columns := "columns"
		o := qb.Order(false)
		expectedOrderBy := selectBuilder.SB.OrderBy(columns, o)

		// act
		resultOrderBy := selectBuilder.OrderBy(columns, o)

		// assert
		assert.Equal(t, expectedOrderBy, resultOrderBy.(*SelectBuilder).SB)
	})
}

func Test_Select_Limit(t *testing.T) {
	t.Run("Should return right Limit", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		limit := uint(10)
		expectedLimit := selectBuilder.SB.Limit(limit)

		// act
		resultLimit := selectBuilder.Limit(limit)

		// assert
		assert.Equal(t, expectedLimit, resultLimit.(*SelectBuilder).SB)
	})
}

func Test_Select_LimitNamed(t *testing.T) {
	t.Run("Should return right LimitNamed", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		name := "name"
		expectedLimitNamed := selectBuilder.SB.LimitNamed(name)

		// act
		resultLimitNamed := selectBuilder.LimitNamed(name)

		// assert
		assert.Equal(t, expectedLimitNamed, resultLimitNamed.(*SelectBuilder).SB)
	})
}

func Test_Select_LimitPerPartition(t *testing.T) {
	t.Run("Should return right LimitPerPartition", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		limit := uint(10)
		expectedLimitPerPartition := selectBuilder.SB.LimitPerPartition(limit)

		// act
		resultLimitPerPartition := selectBuilder.LimitPerPartition(limit)

		// assert
		assert.Equal(t, expectedLimitPerPartition, resultLimitPerPartition.(*SelectBuilder).SB)
	})
}

func Test_Select_LimitPerPartitionNamed(t *testing.T) {
	t.Run("Should return right LimitPerPartitionNamed", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		name := "name"
		expectedLimitPerPartitionNamed := selectBuilder.SB.LimitPerPartitionNamed(name)

		// act
		resultLimitPerPartitionNamed := selectBuilder.LimitPerPartitionNamed(name)

		// assert
		assert.Equal(t, expectedLimitPerPartitionNamed, resultLimitPerPartitionNamed.(*SelectBuilder).SB)
	})
}

func Test_Select_AllowFiltering(t *testing.T) {
	t.Run("Should return right AllowFiltering", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		expectedAllowFiltering := selectBuilder.SB.AllowFiltering()

		// act
		resultAllowFiltering := selectBuilder.AllowFiltering()

		// assert
		assert.Equal(t, expectedAllowFiltering, resultAllowFiltering.(*SelectBuilder).SB)
	})
}

func Test_Select_BypassCache(t *testing.T) {
	t.Run("Should return right BypassCache", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		expectedBypassCache := selectBuilder.SB.BypassCache()

		// act
		resultBypassCache := selectBuilder.BypassCache()

		// assert
		assert.Equal(t, expectedBypassCache, resultBypassCache.(*SelectBuilder).SB)
	})
}

func Test_Select_Count(t *testing.T) {
	t.Run("Should return right Count", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		column := "column"
		expectedCount := selectBuilder.SB.Count(column)

		// act
		resultCount := selectBuilder.Count(column)

		// assert
		assert.Equal(t, expectedCount, resultCount.(*SelectBuilder).SB)
	})
}

func Test_Select_CountAll(t *testing.T) {
	t.Run("Should return right CountAll", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		expectedCountAll := selectBuilder.SB.CountAll()

		// act
		resultCountAll := selectBuilder.CountAll()

		// assert
		assert.Equal(t, expectedCountAll, resultCountAll.(*SelectBuilder).SB)
	})
}

func Test_Select_Min(t *testing.T) {
	t.Run("Should return right Min", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		column := "column"
		expectedMin := selectBuilder.SB.Min(column)

		// act
		resultMin := selectBuilder.Min(column)

		// assert
		assert.Equal(t, expectedMin, resultMin.(*SelectBuilder).SB)
	})
}

func Test_Select_Max(t *testing.T) {
	t.Run("Should return right Max", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		column := "column"
		expectedMax := selectBuilder.SB.Max(column)

		// act
		resultMax := selectBuilder.Max(column)

		// assert
		assert.Equal(t, expectedMax, resultMax.(*SelectBuilder).SB)
	})
}

func Test_Select_Avg(t *testing.T) {
	t.Run("Should return right Avg", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		column := "column"
		expectedAvg := selectBuilder.SB.Avg(column)

		// act
		resultAvg := selectBuilder.Avg(column)

		// assert
		assert.Equal(t, expectedAvg, resultAvg.(*SelectBuilder).SB)
	})
}

func Test_Select_Sum(t *testing.T) {
	t.Run("Should return right Sum", func(t *testing.T) {
		// arrange
		sut := MakeSelectBuilderSut()
		selectBuilder := sut.SelectBuilder
		column := "column"
		expectedSum := selectBuilder.SB.Sum(column)

		// act
		resultSum := selectBuilder.Sum(column)

		// assert
		assert.Equal(t, expectedSum, resultSum.(*SelectBuilder).SB)
	})
}

/* ============= SUT helpers ============ */

type selectBuilderSut struct {
	SelectBuilder *SelectBuilder
	session       *igocqlx.Session
}

func MakeSelectBuilderSut() selectBuilderSut {
	gocqlxSelectBuilder := MakeGocqlxSelectBuilderSut()

	session := igocqlx.Session{
		S: &gocqlx.Session{
			Session: &gocql.Session{},
		},
	}

	return selectBuilderSut{
		SelectBuilder: &SelectBuilder{
			SB: gocqlxSelectBuilder.Sut,
		},
		session: &session,
	}
}

type selectBuilderGocqlxSut struct {
	Sut *qb.SelectBuilder
}

func MakeGocqlxSelectBuilderSut() selectBuilderGocqlxSut {
	sut := &qb.SelectBuilder{}

	return selectBuilderGocqlxSut{
		Sut: sut,
	}
}
