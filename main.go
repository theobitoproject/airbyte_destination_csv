package main

import (
	"log"
	"os"

	"github.com/theobitoproject/kankuro/pkg/destination"
	"github.com/theobitoproject/kankuro/pkg/protocol"

	"github.com/theobitoproject/airbyte_destination_csv/pkg/csv"
)

func main() {
	rowChan := csv.NewRowChannel()
	marshalerWorkersChan := make(chan bool)
	writerWorkersChan := make(chan bool)

	rm := csv.NewMarshaler(rowChan, marshalerWorkersChan)
	cw := csv.NewWriter(rowChan, writerWorkersChan)

	dst := csv.NewDestinationCsv(
		protocol.LocalRoot,
		rm,
		cw,
		rowChan,
		marshalerWorkersChan,
		writerWorkersChan,
	)

	runner := destination.NewSafeDestinationRunner(
		dst,
		os.Stdout,
		os.Stdin,
		os.Args,
	)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
