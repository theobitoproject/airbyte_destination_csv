package pkg

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

// CsvWriter takes csv records and writes them into files
type CsvWriter interface {
	// AddWorker adds a new thread to write csv records in a file
	AddWorker(messenger.ChannelHub, string)
	// CloseAndFlush performs final cleaning
	// after all csv records are written
	CloseAndFlush()
}

type csvWriter struct {
	hub            messenger.ChannelHub
	csvRecordChann CsvRecordChannel

	workersDoneChan chan bool
	fileWriterPairs map[string]*fileWriterPair
	mu              *sync.Mutex
}

type fileWriterPair struct {
	file   *os.File
	writer *csv.Writer
}

// NewCsvWriter creates a new instance of CsvWriter
func NewCsvWriter(
	csvRecordChann CsvRecordChannel,
	workersDoneChan chan bool,
) CsvWriter {
	return &csvWriter{
		csvRecordChann:  csvRecordChann,
		workersDoneChan: workersDoneChan,
		fileWriterPairs: map[string]*fileWriterPair{},
		mu:              &sync.Mutex{},
	}
}

// AddWorker adds a new thread to write csv records in a file
func (cw *csvWriter) AddWorker(
	hub messenger.ChannelHub,
	path string,
) {
	go func() {
		for {
			csvRec, channelOpen := <-cw.csvRecordChann
			if !channelOpen {
				cw.removeWorker()
				return
			}

			cw.mu.Lock()

			fwPair, err := cw.getFileWriterPairForStream(
				path,
				csvRec.streamName,
			)
			if err != nil {
				cw.hub.GetErrorChannel() <- err
				continue
			}

			err = fwPair.writer.Write(csvRec.data)
			if err != nil {
				cw.hub.GetErrorChannel() <- err
				continue
			}

			cw.mu.Unlock()
		}
	}()
}

// CloseAndFlush performs final cleaning
// after all csv records are written
func (cw *csvWriter) CloseAndFlush() {
	for _, fileWriterPair := range cw.fileWriterPairs {
		fileWriterPair.writer.Flush()
		fileWriterPair.file.Close()
	}
}

func (cw *csvWriter) getFileWriterPairForStream(
	path string,
	streamName string,
) (*fileWriterPair, error) {
	fwPair, created := cw.fileWriterPairs[streamName]
	if created {
		return fwPair, nil
	}

	filename := fmt.Sprintf(
		"%s/%s%s.csv",
		path,
		protocol.AirbyteRaw,
		streamName,
	)
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	w := csv.NewWriter(f)

	fwPair = &fileWriterPair{
		file:   f,
		writer: w,
	}

	cw.fileWriterPairs[streamName] = fwPair

	return fwPair, nil
}

func (cw *csvWriter) removeWorker() {
	cw.workersDoneChan <- true
}
