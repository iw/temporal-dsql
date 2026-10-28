package dsql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDSQLCompatibleQueriesOnly ensures that all query constants use only
// DSQL-compatible SQL syntax.
func TestDSQLCompatibleQueriesOnly(t *testing.T) {
	// Get the directory of the current test file
	dir := "."

	// Parse all Go files in the DSQL plugin directory
	fileSet := token.NewFileSet()
	packages, err := parser.ParseDir(fileSet, dir, nil, parser.ParseComments)
	require.NoError(t, err, "Failed to parse Go files in DSQL plugin directory")

	var incompatibleFeatures []string

	// List of SQL features not supported by DSQL
	// Build unsupported feature strings without embedding exact tokens in source,
	// to avoid false-positive greps across the repo.
	fForShare := "FOR" + " " + "SHARE"
	fForNoKeyUpdate := "FOR" + " " + "NO" + " " + "KEY" + " " + "UPDATE"
	unsupportedFeatures := []string{
		fForShare,
		fForNoKeyUpdate,
		"BIGSERIAL",
		"CHECK (",
		"REFERENCES ", // Foreign key constraints (basic check)
	}

	// Check each package
	for packageName, pkg := range packages {
		if packageName == "dsql" { // Only check our DSQL package
			// Check each file in the package
			for fileName, file := range pkg.Files {
				// Skip test files for this check
				if strings.HasSuffix(fileName, "_test.go") {
					continue
				}

				// Walk the AST to find string literals that look like SQL
				ast.Inspect(file, func(n ast.Node) bool {
					switch node := n.(type) {
					case *ast.BasicLit:
						if node.Kind == token.STRING {
							// Remove quotes and check for unsupported features
							value := strings.Trim(node.Value, "`\"")
							upperValue := strings.ToUpper(value)

							// Only check strings that look like SQL (contain SELECT, INSERT, UPDATE, DELETE)
							if strings.Contains(upperValue, "SELECT") ||
								strings.Contains(upperValue, "INSERT") ||
								strings.Contains(upperValue, "UPDATE") ||
								strings.Contains(upperValue, "DELETE") {

								for _, feature := range unsupportedFeatures {
									if strings.Contains(upperValue, feature) {
										position := fileSet.Position(node.Pos())
										violation := filepath.Base(position.Filename) + ":" +
											strings.TrimPrefix(position.String(), position.Filename+":") +
											" - contains unsupported feature: " + feature
										incompatibleFeatures = append(incompatibleFeatures, violation)
									}
								}
							}
						}
					}
					return true
				})
			}
		}
	}

	require.Empty(t, incompatibleFeatures, "Found DSQL-incompatible SQL features:\n%s",
		strings.Join(incompatibleFeatures, "\n"))
}
