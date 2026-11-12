//go:build !darwin && !linux

package persistencetests

// lockDSQLSchemaFile has no lock across processes here, so only the process's own
// leases keep test clusters apart.
func lockDSQLSchemaFile(string) (unlock func(), ok bool, err error) {
	return func() {}, true, nil
}
