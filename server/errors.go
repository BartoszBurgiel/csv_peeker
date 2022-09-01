package server

import "fmt"

var ConfigFormatError error = fmt.Errorf("The config file has invalid format.")
var LabelDoesNotExist error = fmt.Errorf("The requested label does not exist.")
