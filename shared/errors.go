package shared

import "fmt"

var EmptyCSVFileError error = fmt.Errorf("The requested CSV file is empty.")
