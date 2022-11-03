package main

import (
	"log"
	"os"

	"github.com/theobitoproject/kankuro/pkg/destination"
)

func main() {
	dst := newDestinationCsv()
	runner := destination.NewSafeDestinationRunner(dst, os.Stdout, os.Stdin, os.Args)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
