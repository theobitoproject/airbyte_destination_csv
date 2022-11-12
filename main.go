package main

import (
	"log"
	"os"

	"github.com/theobitoproject/airbyte_destination_csv/pkg/csv"
	"github.com/theobitoproject/kankuro/pkg/destination"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

func main() {
	rowChan := csv.NewRowChannel()
	recordMarshalerWorkersChan := make(chan bool)
	csvWriterWorkersChan := make(chan bool)

	rm := csv.NewMarshaler(rowChan, recordMarshalerWorkersChan)
	cw := csv.NewWriter(rowChan, csvWriterWorkersChan)

	dst := csv.NewDestinationCsv(
		protocol.LocalRoot,
		rm,
		cw,
		rowChan,
		recordMarshalerWorkersChan,
		csvWriterWorkersChan,
	)

	runner := destination.NewSafeDestinationRunner(dst, os.Stdout, os.Stdin, os.Args)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
