package dsql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/schema"
)

func TestSchemaDirs(t *testing.T) {
	require.Contains(t, schema.PathsByDir("dsql"), "dsql/temporal")
}
