package igocqlx

import (
	"context"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/stretchr/testify/assert"
)

func Test_Queryx_WithBindTransformer(t *testing.T) {
	t.Run("Should call WithBindTransformer and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		tr := func(name string, val interface{}) interface{} { return val }
		expectedQuery := query.Q.WithBindTransformer(tr)

		// act
		resultQuery := query.WithBindTransformer(tr)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*Queryx).Q)
	})
}

func Test_Queryx_BindStruct(t *testing.T) {
	t.Run("Should call BindStruct and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		expectedQuery := query.Q.BindStruct(sut.completeData)

		// act
		resultQuery := query.BindStruct(sut.completeData)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*Queryx).Q)
	})
}

func Test_Queryx_BindStructMap(t *testing.T) {
	t.Run("Should call BindStructMap and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		expectedQuery := query.Q.BindStructMap(sut.completeData, nil)

		// act
		resultQuery := query.BindStructMap(sut.completeData, nil)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*Queryx).Q)
	})
}

func Test_Queryx_BindMap(t *testing.T) {
	t.Run("Should call BindMap and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		expectedQuery := query.Q.BindMap(nil)

		// act
		resultQuery := query.BindMap(nil)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*Queryx).Q)
	})
}

func Test_Queryx_Bind(t *testing.T) {
	t.Run("Should call Bind and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		expectedQuery := query.Q.Bind(nil)

		// act
		resultQuery := query.Bind(nil)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*Queryx).Q)
	})
}

func Test_Queryx_Err(t *testing.T) {
	t.Run("Should call Err and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		expectedErr := query.Q.Err()

		// act
		resultErr := query.Err()

		// assert
		assert.Equal(t, expectedErr, resultErr)
	})
}

func Test_Queryx_Consistency(t *testing.T) {
	t.Run("Should call Consistency and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		c := gocql.Consistency(10)
		expectedConsistency := query.Q.Consistency(c)

		// act
		resultConsistency := query.Consistency(c)

		// assert
		assert.Equal(t, expectedConsistency, resultConsistency.(*Queryx).Q)
	})
}

func Test_Queryx_CustomPayload(t *testing.T) {
	t.Run("Should call CustomPayload and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		customPayload := make(map[string][]byte)
		expectedCustomPayload := query.Q.CustomPayload(customPayload)

		// act
		resultCustomPayload := query.CustomPayload(customPayload)

		// assert
		assert.Equal(t, expectedCustomPayload, resultCustomPayload.(*Queryx).Q)
	})
}

func Test_Queryx_Trace(t *testing.T) {
	t.Run("Should call Trace and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		expectedTrace := query.Q.Trace(&traceSut{})

		// act
		resultTrace := query.Trace(&traceSut{})

		// assert
		assert.Equal(t, expectedTrace, resultTrace.(*Queryx).Q)
	})
}

func Test_Queryx_Observer(t *testing.T) {
	t.Run("Should call Observer and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		expectedObserver := query.Q.Observer(&observerSut{})

		// act
		resultObserver := query.Observer(&observerSut{})

		// assert
		assert.Equal(t, expectedObserver, resultObserver.(*Queryx).Q)
	})
}

func Test_Queryx_PageSize(t *testing.T) {
	t.Run("Should call PageSize and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		n := 1
		expectedPageSize := query.Q.PageSize(n)

		// act
		resultPageSize := query.PageSize(n)

		// assert
		assert.Equal(t, expectedPageSize, resultPageSize.(*Queryx).Q)
	})
}

func Test_Queryx_DefaultTimestamp(t *testing.T) {
	t.Run("Should call DefaultTimestamp and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		enable := true
		expectedDefaultTimestamp := query.Q.DefaultTimestamp(enable)

		// act
		resultDefaultTimestamp := query.DefaultTimestamp(enable)

		// assert
		assert.Equal(t, expectedDefaultTimestamp, resultDefaultTimestamp.(*Queryx).Q)
	})
}

func Test_Queryx_WithTimestamp(t *testing.T) {
	t.Run("Should call WithTimestamp and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		timestamp := int64(10)
		expectedWithTimestamp := query.Q.WithTimestamp(timestamp)

		// act
		resultWithTimestamp := query.WithTimestamp(timestamp)

		// assert
		assert.Equal(t, expectedWithTimestamp, resultWithTimestamp.(*Queryx).Q)
	})
}

func Test_Queryx_RoutingKey(t *testing.T) {
	t.Run("Should call RoutingKey and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		routingKey := []byte{}
		expectedRoutingKey := query.Q.RoutingKey(routingKey)

		// act
		resultRoutingKey := query.RoutingKey(routingKey)

		// assert
		assert.Equal(t, expectedRoutingKey, resultRoutingKey.(*Queryx).Q)
	})
}

func Test_Queryx_WithContext(t *testing.T) {
	t.Run("Should call WithContext and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		ctx := context.Background()
		expectedWithContext := query.Q.WithContext(ctx)

		// act
		resultWithContext := query.WithContext(ctx)

		// assert
		assert.Equal(t, expectedWithContext, resultWithContext.(*Queryx).Q)
	})
}

func Test_Queryx_Prefetch(t *testing.T) {
	t.Run("Should call Prefetch and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		prefetch := float64(10.5)
		expectedPrefetch := query.Q.Prefetch(prefetch)

		// act
		resultPrefetch := query.Prefetch(prefetch)

		// assert
		assert.Equal(t, expectedPrefetch, resultPrefetch.(*Queryx).Q)
	})
}

func Test_Queryx_RetryPolicy(t *testing.T) {
	t.Run("Should call RetryPolicy and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		expectedRetryPolicy := query.Q.RetryPolicy(&retryPolicyStruct{})

		// act
		resultRetryPolicy := query.RetryPolicy(&retryPolicyStruct{})

		// assert
		assert.Equal(t, expectedRetryPolicy, resultRetryPolicy.(*Queryx).Q)
	})
}

func Test_Queryx_SetSpeculativeExecutionPolicy(t *testing.T) {
	t.Run("Should call SetSpeculativeExecutionPolicy and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		expectedSetSpeculativeExecutionPolicy := query.Q.SetSpeculativeExecutionPolicy(&speculativeExecutionPolicyStruct{})

		// act
		resultSetSpeculativeExecutionPolicy := query.SetSpeculativeExecutionPolicy(&speculativeExecutionPolicyStruct{})

		// assert
		assert.Equal(t, expectedSetSpeculativeExecutionPolicy, resultSetSpeculativeExecutionPolicy.(*Queryx).Q)
	})
}

func Test_Queryx_Idempotent(t *testing.T) {
	t.Run("Should call Idempotent and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		value := true
		expectedIdempotent := query.Q.Idempotent(value)

		// act
		resultIdempotent := query.Idempotent(value)

		// assert
		assert.Equal(t, expectedIdempotent, resultIdempotent.(*Queryx).Q)
	})
}

func Test_Queryx_SerialConsistency(t *testing.T) {
	t.Run("Should call SerialConsistency and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		cons := gocql.SerialConsistency(10)
		expectedSerialConsistency := query.Q.SerialConsistency(cons)

		// act
		resultSerialConsistency := query.SerialConsistency(cons)

		// assert
		assert.Equal(t, expectedSerialConsistency, resultSerialConsistency.(*Queryx).Q)
	})
}

func Test_Queryx_PageState(t *testing.T) {
	t.Run("Should call PageState and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		pageState := []byte{}
		expectedPageState := query.Q.PageState(pageState)

		// act
		resultPageState := query.PageState(pageState)

		// assert
		assert.Equal(t, expectedPageState, resultPageState.(*Queryx).Q)
	})
}

func Test_Queryx_NoSkipMetadata(t *testing.T) {
	t.Run("Should call NoSkipMetadata and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx
		expectedNoSkipMetadata := query.Q.NoSkipMetadata()

		// act
		resultNoSkipMetadata := query.NoSkipMetadata()

		// assert
		assert.Equal(t, expectedNoSkipMetadata, resultNoSkipMetadata.(*Queryx).Q)
	})
}

func Test_Queryx_Release(t *testing.T) {
	t.Run("Should call Release and return proper result", func(t *testing.T) {
		// arrange
		sut := makeQueryxSut()
		query := sut.Queryx

		// act
		query.Release()

		// assert
		assert.NotPanics(t, query.Release)
	})
}

/* ============= SUT helpers ============ */
type TrackingDataEntity struct {
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Location  string `db:"location"`
}

type queryxSut struct {
	Queryx       *Queryx
	completeData *TrackingDataEntity
}

func makeQueryxSut() queryxSut {
	completeDataEntity := &TrackingDataEntity{
		FirstName: "Guilherme",
		LastName:  "Rodrigues",
		Location:  "Brazil",
	}

	gocqlxsession := makeGocqlxQueryxSut()

	return queryxSut{
		Queryx: &Queryx{
			Q: gocqlxsession.Sut,
		},
		completeData: completeDataEntity,
	}
}

type queryxGocqlxSut struct {
	Sut *gocqlx.Queryx
}

type traceSut struct{}

func (*traceSut) Trace(traceId []byte) {}

type observerSut struct{}

func (*observerSut) ObserveQuery(context.Context, gocql.ObservedQuery) {}

type retryPolicyStruct struct{}

func (*retryPolicyStruct) Attempt(gocql.RetryableQuery) bool  { return true }
func (*retryPolicyStruct) GetRetryType(error) gocql.RetryType { return gocql.RetryType(1) }

type speculativeExecutionPolicyStruct struct{}

func (*speculativeExecutionPolicyStruct) Attempts() int        { return 1 }
func (*speculativeExecutionPolicyStruct) Delay() time.Duration { return time.Duration(10) }

func makeGocqlxQueryxSut() queryxGocqlxSut {
	names := []string{"first_name", "last_name", "location"}

	gocqlQuery := &gocql.Query{}

	sut := &gocqlx.Queryx{
		Query:  gocqlQuery,
		Names:  names,
		Mapper: gocqlx.DefaultMapper,
	}

	return queryxGocqlxSut{
		Sut: sut,
	}
}

type metadataGocqlxSut struct {
	Name    string
	Columns []string
	PartKey []string
	SortKey []string
}

func makeGocqlxMetadataSut() metadataGocqlxSut {
	name := "tracking_data"
	columns := []string{"first_name", "last_name", "location"}
	partKey := []string{"first_name", "last_name"}
	sortKey := []string{"location"}

	return metadataGocqlxSut{
		Name:    name,
		Columns: columns,
		PartKey: partKey,
		SortKey: sortKey,
	}
}
