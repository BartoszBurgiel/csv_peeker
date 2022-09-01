package main

import (
	"fmt"
	"net/http"
	"os"

	"bartosz.com/server"
)

func main() {

	configPath := os.Args[1]
	port := os.Args[2]
	s, err := server.NewServer(configPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(http.ListenAndServe(":"+port, s))
}
