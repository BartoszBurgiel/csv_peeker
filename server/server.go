package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/BartoszBurgiel/csv_peeker/shared"
)

// Server represents the server which provides the CSV file information and data.
type Server struct {
	conf     config
	confLock *sync.Mutex
}

func NewServer(path string) (Server, error) {
	c, err := newConfig(path)
	if err != nil {
		return Server{}, err
	}
	return Server{
		conf:     c,
		confLock: &sync.Mutex{},
	}, nil
}

func (s Server) Print() {
	for k, v := range s.conf {
		fmt.Println(k, *v)
	}
}

// ServeFileContents serves the contents of the file in the JSON format to the provided writer
func (s Server) ServeFileContents(label string, count int, filter shared.Filter, rw io.Writer) error {

	s.confLock.Lock()
	f, ok := s.conf[label]
	s.confLock.Unlock()
	if !ok {
		return LabelDoesNotExist
	}

	content, err := f.GetRowsAsCSV(count, filter)
	if err != nil {
		return err
	}
	_, err = io.Copy(rw, strings.NewReader(content))
	if err != nil {
		return err
	}
	return nil

}

func (s Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		s.serveBadRequest(w, r)
		return
	}

	path := r.URL.Path
	splittedPath := strings.Split(path, "/")
	if len(splittedPath) != 2 {
		s.serveBadRequest(w, r)
		return
	}
	label := splittedPath[1]

	if label == "labels" {

		j, _ := json.Marshal(s.conf)
		fmt.Fprint(w, string(j))
		return
	}
	s.handleCSVRequest(w, r)
}

func (s Server) serve404(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(404)
	fmt.Fprintln(w, "The requested ressource does not exist.")
}

func (s Server) serveBadRequest(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintln(w, "Bad request.")
}

func (s Server) serveError(w http.ResponseWriter, r *http.Request, msg string) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintln(w, msg)
}

func (s Server) serveInternalError(w http.ResponseWriter, r *http.Request, msg string) {
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintln(w, msg)
}

func (s Server) handleCSVRequest(w http.ResponseWriter, r *http.Request) {

	path := r.URL.Path
	splittedPath := strings.Split(path, "/")
	if len(splittedPath) != 2 {
		s.serveBadRequest(w, r)
		return
	}
	label := splittedPath[1]

	s.confLock.Lock()
	f, ok := s.conf[label]
	s.confLock.Unlock()
	if !ok {
		s.serve404(w, r)
		return
	}

	if r.URL.RawQuery == "" {
		err := f.LoadMetadata()
		if err != nil {
			s.serveError(w, r, err.Error())
			return
		}
		data := f.JSON()
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, string(data))
		return
	}

	filter := shared.ParseURLFilter(*r.URL, f.Columns)
	cnt := shared.ROWS_COUNT_LIMIT
	if r.URL.Query().Has("count") {

		c, err := strconv.Atoi(r.URL.Query().Get("count"))
		if err != nil {
			s.serveError(w, r, err.Error())
			return
		}
		cnt = c
	}

	if r.URL.Query().Has("tail") {
		data, err := f.GetTailAsCSV(cnt, filter)
		if err != nil {
			s.serveError(w, r, err.Error())
			return
		}
		fmt.Fprint(w, data)
		return
	}

	data, err := f.GetRowsAsCSV(cnt, filter)
	if err != nil {
		s.serveError(w, r, err.Error())
		return
	}
	fmt.Fprint(w, data)
}
