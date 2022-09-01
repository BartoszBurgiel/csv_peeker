package shared

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"time"
)

// File represents a single CSV file
type File struct {
	Path     string
	M_time   time.Time
	Size     int64
	RowCount int
	Columns  []string

	rows  [][]string
	Delim string
}

// NewFile returns a new File instance
func NewFile(path string, delim string) (*File, error) {
	stats, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	f := &File{
		Path:   path,
		M_time: stats.ModTime(),
		Size:   stats.Size(),
		Delim:  delim,
	}
	if err := f.setRowCount(); err != nil {
		return nil, err
	}
	if err := f.ReadHeader(); err != nil {
		return nil, err
	}
	return f, nil
}

// ReadHeader reads the header of the CSV file into the file instance
func (f *File) ReadHeader() error {
	if err := f.checkFile(); err != nil {
		return err
	}

	file, err := os.Open(f.Path)
	if err != nil {
		return err
	}
	defer file.Close()
	sc := bufio.NewScanner(file)

	sc.Scan()
	head := sc.Text()
	f.Columns = strings.Split(head, string(f.Delim))
	return nil
}

// JSON converts the metadata of the file to a JSON format
func (f *File) JSON() []byte {
	j, _ := json.Marshal(f)
	return j
}

// GetRows returns the raw, unseperated rows of the CSV file
func (f *File) GetRows(count int, filter Filter) ([][]string, error) {
	err := f.readRows(count, filter)
	if err != nil {
		return [][]string{}, err
	}
	return f.rows, nil
}

// GetRowsAsCSV returns the stored rows of the file in the CSV format
// together with the headers
func (f *File) GetRowsAsCSV(count int, filter Filter) (string, error) {

	b := strings.Builder{}
	b.WriteString(strings.Join(f.Columns, string(f.Delim)))
	b.WriteByte(10)

	r, err := f.GetRows(count, filter)
	if err != nil {
		return "", err
	}
	for _, v := range r {
		b.WriteString(strings.Join(v, string(f.Delim)))
		b.WriteByte(10)
	}
	return b.String(), nil
}

func (f File) checkFile() error {
	stats, err := os.Stat(f.Path)
	if err != nil {
		return err
	}
	if stats.Size() == 0 {
		return EmptyCSVFileError
	}
	return nil
}

func (f *File) setRowCount() error {

	if err := f.checkFile(); err != nil {
		return err
	}

	file, err := os.Open(f.Path)
	if err != nil {
		return err
	}
	defer file.Close()
	sc := bufio.NewScanner(file)

	for sc.Scan() {
		f.RowCount++
	}
	return nil
}

// readRows reads first count rows that match the filter into the File instance
func (f *File) readRows(count int, filter Filter) error {

	f.rows = [][]string{}
	err := f.ReadHeader()
	if err != nil {
		return err
	}
	if err := f.checkFile(); err != nil {
		return err
	}

	file, err := os.Open(f.Path)
	if err != nil {
		return err
	}
	defer file.Close()
	sc := bufio.NewScanner(file)

	for sc.Scan() {
		r := sc.Text()
		row := strings.Split(r, string(f.Delim))

		if filter.match(row) {
			f.rows = append(f.rows, row)
		}
		if len(f.rows) == count || len(f.rows) == ROWS_COUNT_LIMIT {
			return nil
		}
	}
	return nil

}
