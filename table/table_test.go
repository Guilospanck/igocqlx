package igocqlxtable

import (
	"context"
	"testing"

	"github.com/Guilospanck/igocqlx"
	igocqlxqb "github.com/Guilospanck/igocqlx/qb"
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
		expectedMetadata := sut.MetadataUnderTest
		table := sut.TableUnderTest

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
		expectedMetadata := sut.MetadataUnderTest
		expectedTable := sut.TableUnderTest

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
		table := sut.TableUnderTest
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
		table := sut.TableUnderTest
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
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
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
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		session := makeSessionSut()
		expectedQueryx := table.T.GetQuery(*session.Session.S, columns...)

		// act
		resultQueryx := table.GetQuery(session.Session, columns...)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_GetQueryContext(t *testing.T) {
	t.Run("Should return the right GetQueryContext", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		session := makeSessionSut()
		ctx := context.Background()
		expectedQueryx := table.T.GetQueryContext(ctx, *session.Session.S, columns...)

		// act
		resultQueryx := table.GetQueryContext(ctx, session.Session, columns...)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_Select(t *testing.T) {
	t.Run("Should return the right Select", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		expectedStmt, expectedNames := table.T.Select(columns...)

		// act
		resultStmt, resultNames := table.Select(columns...)

		// assert
		assert.Equal(t, expectedStmt, resultStmt)
		assert.Equal(t, expectedNames, resultNames)
	})
}

func Test_Table_SelectQuery(t *testing.T) {
	t.Run("Should return the right SelectQuery", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		session := makeSessionSut()
		expectedQueryx := table.T.SelectQuery(*session.Session.S, columns...)

		// act
		resultQueryx := table.SelectQuery(session.Session, columns...)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_SelectQueryContext(t *testing.T) {
	t.Run("Should return the right SelectQueryContext", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		session := makeSessionSut()
		ctx := context.Background()
		expectedQueryx := table.T.SelectQueryContext(ctx, *session.Session.S, columns...)

		// act
		resultQueryx := table.SelectQueryContext(ctx, session.Session, columns...)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_SelectBuilder(t *testing.T) {
	t.Run("Should return the right SelectBuilder", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		expectedSB := table.T.SelectBuilder(columns...)

		// act
		resultSB := table.SelectBuilder(columns...)

		// assert
		assert.Equal(t, expectedSB, resultSB.(*igocqlxqb.SelectBuilder).SB)
	})
}

func Test_Table_SelectAll(t *testing.T) {
	t.Run("Should return the right SelectAll", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		expectedStmt, expectedNames := table.T.SelectAll()

		// act
		resultStmt, resultNames := table.SelectAll()

		// assert
		assert.Equal(t, expectedStmt, resultStmt)
		assert.Equal(t, expectedNames, resultNames)
	})
}

func Test_Table_Insert(t *testing.T) {
	t.Run("Should return the right Insert", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		expectedStmt, expectedNames := table.T.Insert()

		// act
		resultStmt, resultNames := table.Insert()

		// assert
		assert.Equal(t, expectedStmt, resultStmt)
		assert.Equal(t, expectedNames, resultNames)
	})
}

func Test_Table_InsertQuery(t *testing.T) {
	t.Run("Should return the right InsertQuery", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		session := makeSessionSut()
		expectedQueryx := table.T.InsertQuery(*session.Session.S)

		// act
		resultQueryx := table.InsertQuery(session.Session)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_InsertQueryContext(t *testing.T) {
	t.Run("Should return the right InsertQueryContext", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		session := makeSessionSut()
		ctx := context.Background()
		expectedQueryx := table.T.InsertQueryContext(ctx, *session.Session.S)

		// act
		resultQueryx := table.InsertQueryContext(ctx, session.Session)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_InsertBuilder(t *testing.T) {
	t.Run("Should return the right InsertBuilder", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		expectedBuilder := table.T.InsertBuilder()

		// act
		resultBuilder := table.InsertBuilder()

		// assert
		assert.Equal(t, expectedBuilder, resultBuilder.(*igocqlxqb.InsertBuilder).IB)
	})
}

func Test_Table_Update(t *testing.T) {
	t.Run("Should return the right Update", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		expectedStmt, expectedNames := table.T.Update(columns...)

		// act
		resultStmt, resultNames := table.Update(columns...)

		// assert
		assert.Equal(t, expectedStmt, resultStmt)
		assert.Equal(t, expectedNames, resultNames)
	})
}

func Test_Table_UpdateQuery(t *testing.T) {
	t.Run("Should return the right UpdateQuery", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		session := makeSessionSut()
		expectedQueryx := table.T.UpdateQuery(*session.Session.S, columns...)

		// act
		resultQueryx := table.UpdateQuery(session.Session, columns...)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_UpdateQueryContext(t *testing.T) {
	t.Run("Should return the right UpdateQueryContext", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		session := makeSessionSut()
		ctx := context.Background()
		expectedQueryx := table.T.UpdateQueryContext(ctx, *session.Session.S, columns...)

		// act
		resultQueryx := table.UpdateQueryContext(ctx, session.Session, columns...)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_UpdateBuilder(t *testing.T) {
	t.Run("Should return the right UpdateBuilder", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		expectedBuilder := table.T.UpdateBuilder()

		// act
		resultBuilder := table.UpdateBuilder()

		// assert
		assert.Equal(t, expectedBuilder, resultBuilder.(*igocqlxqb.UpdateBuilder).UB)
	})
}

func Test_Table_Delete(t *testing.T) {
	t.Run("Should return the right Delete", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		expectedStmt, expectedNames := table.T.Delete(columns...)

		// act
		resultStmt, resultNames := table.Delete(columns...)

		// assert
		assert.Equal(t, expectedStmt, resultStmt)
		assert.Equal(t, expectedNames, resultNames)
	})
}

func Test_Table_DeleteQuery(t *testing.T) {
	t.Run("Should return the right DeleteQuery", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		session := makeSessionSut()
		expectedQueryx := table.T.DeleteQuery(*session.Session.S, columns...)

		// act
		resultQueryx := table.DeleteQuery(session.Session, columns...)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_DeleteQueryContext(t *testing.T) {
	t.Run("Should return the right DeleteQueryContext", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		columns := makeGocqlxMetadataSut().Columns
		session := makeSessionSut()
		ctx := context.Background()
		expectedQueryx := table.T.DeleteQueryContext(ctx, *session.Session.S, columns...)

		// act
		resultQueryx := table.DeleteQueryContext(ctx, session.Session, columns...)

		// assert
		assert.Equal(t, expectedQueryx, resultQueryx.(*igocqlx.Queryx).Q)
	})
}

func Test_Table_DeleteBuilder(t *testing.T) {
	t.Run("Should return the right DeleteBuilder", func(t *testing.T) {
		// arrange
		sut := makeTableSut()
		table := sut.TableUnderTest
		expectedBuilder := table.T.DeleteBuilder()

		// act
		resultBuilder := table.DeleteBuilder()

		// assert
		assert.Equal(t, expectedBuilder, resultBuilder.(*igocqlxqb.DeleteBuilder).DB)
	})
}

/* ============= SUT helpers ============ */

type tableSut struct {
	MetadataUnderTest *Metadata
	TableUnderTest    *Table
}

func makeTableSut() tableSut {
	metadataUnderTest := &Metadata{
		M: makeGocqlxMetadataSut().Sut,
	}

	tableUnderTest := &Table{
		T: makeGocqlxTableSut().Sut,
	}

	return tableSut{
		MetadataUnderTest: metadataUnderTest,
		TableUnderTest:    tableUnderTest,
	}
}

type sessionSut struct {
	Session *igocqlx.Session
}

func makeSessionSut() sessionSut {
	gocqlxsession := makeGocqlxSessionSut()

	return sessionSut{
		Session: &igocqlx.Session{
			S: gocqlxsession.Sut,
		},
	}
}

type sessionGocqlxSut struct {
	Sut *gocqlx.Session
}

func makeGocqlxSessionSut() sessionGocqlxSut {
	sut := &gocqlx.Session{
		Session: &gocql.Session{},
	}

	return sessionGocqlxSut{
		Sut: sut,
	}
}

type tableGocqlxSut struct {
	Sut *table.Table
}

func makeGocqlxTableSut() tableGocqlxSut {
	metadataGocqlxSut := makeGocqlxMetadataSut()
	sut := table.New(*metadataGocqlxSut.Sut)

	return tableGocqlxSut{
		Sut: sut,
	}
}

type metadataGocqlxSut struct {
	Name    string
	Columns []string
	PartKey []string
	SortKey []string
	Sut     *table.Metadata
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
		Name:    name,
		Columns: columns,
		PartKey: partKey,
		SortKey: sortKey,
		Sut:     sut,
	}
}
