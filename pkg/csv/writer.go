package csv

import (
	"fmt"
	"sync"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

// Writer takes rows and writes them into files
type Writer interface {
	// AddWorker adds a new thread to write rows in a file
	AddWorker(messenger.ChannelHub, string)
	// CloseAndFlush performs final cleaning
	// after all rows are written
	CloseAndFlush()
}

type writer struct {
	hub     messenger.ChannelHub
	rowChan RowChannel

	workersDoneChan chan bool
	fileHandlers    map[string]*fileHandler
	mu              *sync.Mutex
}

// NewWriter creates a new instance of Writer
func NewWriter(
	rowChan RowChannel,
	workersDoneChan chan bool,
) Writer {
	return &writer{
		rowChan:         rowChan,
		workersDoneChan: workersDoneChan,
		fileHandlers:    map[string]*fileHandler{},
		mu:              &sync.Mutex{},
	}
}

// AddWorker adds a new thread to write rows in a file
func (w *writer) AddWorker(
	hub messenger.ChannelHub,
	path string,
) {
	go func() {
		for {
			csvRec, channelOpen := <-w.rowChan
			if !channelOpen {
				w.removeWorker()
				return
			}

			w.mu.Lock()

			fh, err := w.getFileHandler(
				path,
				csvRec.streamName,
			)
			if err != nil {
				w.hub.GetErrorChannel() <- err
				continue
			}

			err = fh.writer.Write(csvRec.data)
			if err != nil {
				w.hub.GetErrorChannel() <- err
				continue
			}

			w.mu.Unlock()
		}
	}()
}

// CloseAndFlush performs final cleaning
// after all rows are written
func (w *writer) CloseAndFlush() {
	for _, fh := range w.fileHandlers {
		fh.writer.Flush()
		fh.file.Close()
	}
}

func (w *writer) getFileHandler(
	path string,
	streamName string,
) (*fileHandler, error) {
	fh, created := w.fileHandlers[streamName]
	if created {
		return fh, nil
	}

	filename := fmt.Sprintf(
		"%s/%s%s.csv",
		path,
		protocol.AirbyteRaw,
		streamName,
	)

	fh, err := newFileHandler(filename)
	if err != nil {
		return nil, err
	}

	w.fileHandlers[streamName] = fh

	return fh, nil
}

func (w *writer) removeWorker() {
	w.workersDoneChan <- true
}
