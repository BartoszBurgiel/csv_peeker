package shared

import (
	"net/url"

	"golang.org/x/exp/slices"
)

// Filter represents a query for the csv file
// key: column_index, value: value to compare to
type Filter map[int]string

// ParseURLFilter derives the filter semantics from an URL
func ParseURLFilter(url url.URL, columns []string) Filter {
	f := Filter(make(map[int]string))

	for k, v := range url.Query() {
		if i := slices.Index(columns, k); i != -1 {
			f[i] = v[0]
		}
	}
	return f
}

// match a row against the Filter
func (f Filter) match(row []string) bool {
	for k, v := range f {
		if row[k] != v {
			return false
		}
	}
	return true
}
