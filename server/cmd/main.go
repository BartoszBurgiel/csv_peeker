package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/BartoszBurgiel/csv_peeker/server"
)

func main() {

	configPath := os.Args[1]
	port := os.Args[2]
	logPath := os.Args[3]
	s, err := server.NewServer(configPath, logPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(http.ListenAndServe(":"+port, s))
}
