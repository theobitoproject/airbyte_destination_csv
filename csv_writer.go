package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"

	"github.com/theobitoproject/kankuro/pkg/messenger"
)

type csvWriter struct {
	hub            messenger.ChannelHub
	csvRecordChann csvRecordChannel

	workersDoneChan chan bool
	fileWriterPairs map[string]*fileWriterPair
	mu              *sync.Mutex
}

type fileWriterPair struct {
	file   *os.File
	writer *csv.Writer
}

func newCsvWriter(
	hub messenger.ChannelHub,
	csvRecordChann csvRecordChannel,
	workersDoneChan chan bool,
) *csvWriter {
	return &csvWriter{
		hub:             hub,
		csvRecordChann:  csvRecordChann,
		workersDoneChan: workersDoneChan,
		fileWriterPairs: map[string]*fileWriterPair{},
		mu:              &sync.Mutex{},
	}
}

func (cw *csvWriter) addWorker() {
	go func() {
		for {
			csvRec, channelOpen := <-cw.csvRecordChann
			if !channelOpen {
				cw.removeWorker()
				return
			}

			cw.mu.Lock()

			fwPair, err := cw.getFileWriterPairForStream(csvRec.streamName)
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

func (cw *csvWriter) getFileWriterPairForStream(
	streamName string,
) (*fileWriterPair, error) {
	fwPair, created := cw.fileWriterPairs[streamName]
	if created {
		return fwPair, nil
	}

	f, err := os.Create(fmt.Sprintf("/local/%s.csv", streamName))
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

func (cw *csvWriter) closeAndFlush() {
	for _, fileWriterPair := range cw.fileWriterPairs {
		fileWriterPair.writer.Flush()
		fileWriterPair.file.Close()
	}
}
