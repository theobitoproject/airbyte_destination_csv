package main

import (
	"log"
	"os"

	"github.com/theobitoproject/airbyte_destination_csv/pkg"
	"github.com/theobitoproject/kankuro/pkg/destination"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

func main() {
	csvRecordChan := pkg.NewCsvRecordChannel()
	recordMarshalerWorkersChan := make(chan bool)
	csvWriterWorkersChan := make(chan bool)

	rm := pkg.NewRecordMarshaler(csvRecordChan, recordMarshalerWorkersChan)
	cw := pkg.NewCsvWriter(csvRecordChan, csvWriterWorkersChan)

	dst := pkg.NewDestinationCsv(
		protocol.LocalRoot,
		rm,
		cw,
		csvRecordChan,
		recordMarshalerWorkersChan,
		csvWriterWorkersChan,
	)

	runner := destination.NewSafeDestinationRunner(dst, os.Stdout, os.Stdin, os.Args)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
