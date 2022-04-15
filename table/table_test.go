package igocqlxtable

import (
	"context"
	"testing"

	"github.com/Guilospanck/igocqlx"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/table"
	"github.com/stretchr/testify/assert"
)

func Test_Table_MetadataStruct(t *testing.T) {
	t.Run("Should create a proper Metadata struct", func(t *testing.T) {
		// arrange
		name := "mutant_data"
		columns := []string{"first_name", "last_name", "address", "picture_location"}
		partKey := []string{"first_name", "last_name"}

		metadata := Metadata{
			M: &table.Metadata{
				Name:    name,
				Columns: columns,
				PartKey: partKey,
			},
		}

		assert.Equal(t, metadata.M.Name, name)
		assert.Equal(t, metadata.M.Columns, columns)
		assert.Equal(t, metadata.M.PartKey, partKey)
	})
}

func Test_Table_Metadata(t *testing.T) {
	t.Run("Should return the right Metadata", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		expectedMetadata := sut.metadataUnderTest
		table := sut.tableUnderTest

		// act
		metadata := table.Metadata()

		// assert
		assert.Equal(t, *expectedMetadata, metadata)
	})
}

func Test_Table_New(t *testing.T) {
	t.Run("Should return the right Table", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		expectedMetadata := sut.metadataUnderTest
		expectedTable := sut.tableUnderTest

		// act
		tableTest := New(*expectedMetadata.M)

		// assert
		assert.Equal(t, expectedTable, tableTest)
		assert.Equal(t, *expectedMetadata, tableTest.Metadata())
	})
}

func Test_Table_PrimaryKeyCmp(t *testing.T) {
	t.Run("Should return the right Comparator", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.tableUnderTest
		expectedCmp := table.T.PrimaryKeyCmp()

		// act
		result := table.PrimaryKeyCmp()

		// assert
		assert.Equal(t, expectedCmp, result)
	})
}

func Test_Table_Name(t *testing.T) {
	t.Run("Should return the right Name", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.tableUnderTest
		expectedName := table.T.Name()

		// act
		result := table.Name()

		// assert
		assert.Equal(t, expectedName, result)
	})
}

func Test_Table_Get(t *testing.T) {
	t.Run("Should return the right Get", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.tableUnderTest
		columns := makeGocqlxMetadataSut().columns
		expectedStmt, expectedNames := table.T.Get(columns...)

		// act
		resultStmt, resultNames := table.Get(columns...)

		// assert
		assert.Equal(t, expectedStmt, resultStmt)
		assert.Equal(t, expectedNames, resultNames)
	})
}

func Test_Table_GetQuery(t *testing.T) {
	t.Run("Should return the right GetQuery", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.tableUnderTest
		columns := makeGocqlxMetadataSut().columns
		session := makeSessionSut()
		expectedQueryx := table.T.GetQuery(*session.session.S, columns...)

		// act
		resultQueryx := table.GetQuery(session.session, columns...)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_GetQueryContext(t *testing.T) {
	t.Run("Should return the right GetQueryContext", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.tableUnderTest
		columns := makeGocqlxMetadataSut().columns
		session := makeSessionSut()
		ctx := context.Background()
		expectedQueryx := table.T.GetQueryContext(ctx, *session.session.S, columns...)

		// act
		resultQueryx := table.GetQueryContext(ctx, session.session, columns...)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

/* ============= SUT helpers ============ */

type tableSut struct {
	metadataUnderTest *Metadata
	tableUnderTest    *Table
}

func makeTableSut() tableSut {
	metadataUnderTest := &Metadata{
		M: makeGocqlxMetadataSut().sut,
	}

	tableUnderTest := &Table{
		T: makeGocqlxTableSut().sut,
	}

	return tableSut{
		metadataUnderTest,
		tableUnderTest,
	}
}

type sessionSut struct {
	session *igocqlx.Session
}

func makeSessionSut() sessionSut {
	gocqlxsession := makeGocqlxSessionSut()

	return sessionSut{
		session: &igocqlx.Session{
			S: gocqlxsession.sut,
		},
	}
}

type sessionGocqlxSut struct {
	sut *gocqlx.Session
}

func makeGocqlxSessionSut() sessionGocqlxSut {
	sut := &gocqlx.Session{
		Session: &gocql.Session{},
	}

	return sessionGocqlxSut{
		sut,
	}
}

type tableGocqlxSut struct {
	sut *table.Table
}

func makeGocqlxTableSut() tableGocqlxSut {
	metadataGocqlxSut := makeGocqlxMetadataSut()
	sut := table.New(*metadataGocqlxSut.sut)

	return tableGocqlxSut{
		sut,
	}
}

type metadataGocqlxSut struct {
	name    string
	columns []string
	partKey []string
	sortKey []string
	sut     *table.Metadata
}

func makeGocqlxMetadataSut() metadataGocqlxSut {
	name := "tracking_data"
	columns := []string{
		"first_name", "last_name", "timestamp", "heat",
		"location", "speed", "telepathy_powers",
	}
	partKey := []string{"first_name", "last_name"}
	sortKey := []string{"timestamp"}

	sut := &table.Metadata{
		Name:    name,
		Columns: columns,
		PartKey: partKey,
		SortKey: sortKey,
	}

	return metadataGocqlxSut{
		name,
		columns,
		partKey,
		sortKey,
		sut,
	}
}
