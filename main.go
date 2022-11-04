package main

import (
	"log"
	"os"

	"github.com/theobitoproject/kankuro/pkg/destination"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

func main() {
	dst := newDestinationCsv(protocol.LocalRoot)
	runner := destination.NewSafeDestinationRunner(dst, os.Stdout, os.Stdin, os.Args)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
