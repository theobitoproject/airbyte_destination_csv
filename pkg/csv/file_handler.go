package csv

import (
	gocsv "encoding/csv"
	"os"
)

type fileHandler struct {
	file   *os.File
	writer *gocsv.Writer
}

func newFileHandler(
	filename string,
) (*fileHandler, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	writer := gocsv.NewWriter(file)
	return &fileHandler{file, writer}, nil
}
