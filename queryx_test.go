package igocqlx

import (
	"testing"

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

func makeGocqlxQueryxSut() queryxGocqlxSut {
	names := []string{"first_name", "last_name", "location"}

	sut := &gocqlx.Queryx{
		Query:  &gocql.Query{},
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
