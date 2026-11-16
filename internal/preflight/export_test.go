package preflight

// SetQuery replaces the query the PostgreSQL check runs in its transaction and returns the restore function.
func SetQuery(sql string) (restore func()) {
	saved := query
	query = sql
	return func() { query = saved }
}
