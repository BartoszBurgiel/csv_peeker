package server

import (
	"bufio"
	"os"
	"strings"

	"github.com/BartoszBurgiel/csv_peeker/shared"
)

// config holds all of the files and their labels
// key: label, value: path
type config map[string]*shared.File

// newConfig returns an instance of the confic struct
func newConfig(path string) (config, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	c := config(make(map[string]*shared.File))
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		row := sc.Text()
		sp := strings.Split(row, "=")
		if len(sp) != 2 {
			return nil, ConfigFormatError
		}
		fsp := strings.Split(sp[1], ";delim:")
		c[sp[0]], err = shared.NewFile(fsp[0], fsp[1])
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}
