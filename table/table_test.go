package igocqlxtable

import (
	"testing"

	"github.com/scylladb/gocqlx/v2/table"
	"github.com/stretchr/testify/assert"
)

func Test_Metadata(t *testing.T) {
	t.Run("", func(t *testing.T) {
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

func Test_Table(t *testing.T) {
	t.Run("", func(t *testing.T) {
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

		tableTest := New(*metadata.M)

		assert.Equal(t, tableTest.Metadata().M.Name, name)
		assert.Equal(t, tableTest.Metadata().M.Columns, columns)
		assert.Equal(t, tableTest.Metadata().M.PartKey, partKey)
	})
}
