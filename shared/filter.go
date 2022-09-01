package shared

// filter represents a query for the csv file
// key: column_index, value: value to compare to
type filter map[int]string

// match a row against the filter
func (f filter) match(row []string) bool {
	for k, v := range f {
		if row[k] == v {
			return true
		}
	}
	return false
}
