package pkg

import (
	"strconv"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

// RecordMarshaler takes incoming record messages,
// creates csv records and send these to the next stage of processing
type RecordMarshaler interface {
	// AddWorker adds a new thread to marshal records async
	// and send these to the next stage of processing
	AddWorker(messenger.ChannelHub)
	// ExtractHeaders defines the headers that should be written
	// in the csv file, creates a csv record
	// and send these to the next stage of processing
	ExtractHeaders([]protocol.ConfiguredStream)
}

type recordMarshaler struct {
	csvRecordChann  CsvRecordChannel
	workersDoneChan chan bool
}

// NewRecordMarshaler creates a new instance of RecordMarshaler
func NewRecordMarshaler(
	csvRecordChann CsvRecordChannel,
	workersDoneChan chan bool,
) RecordMarshaler {
	return &recordMarshaler{
		csvRecordChann:  csvRecordChann,
		workersDoneChan: workersDoneChan,
	}
}

// AddWorker adds a new thread to marshal records async
// and send these to the next stage of processing
func (rm *recordMarshaler) AddWorker(hub messenger.ChannelHub) {
	go func() {
		for {
			rec, channelOpen := <-hub.GetRecordChannel()
			if !channelOpen {
				rm.removeWorker()
				return
			}

			csvRec, err := rm.marshal(rec)
			if err != nil {
				hub.GetErrorChannel() <- err
				continue
			}

			rm.csvRecordChann <- csvRec
		}
	}()
}

// ExtractHeaders defines the headers that should be written
// in the csv file, creates a csv record
// and send these to the next stage of processing
func (rm *recordMarshaler) ExtractHeaders(streams []protocol.ConfiguredStream) {
	go func() {
		for _, stream := range streams {
			csvRec := &csvRecord{
				streamName: stream.Stream.Name,
				data: []string{
					protocol.AirbyteAbId,
					protocol.AirbyteEmittedAt,
					protocol.AirbyteData,
				},
			}

			rm.csvRecordChann <- csvRec
		}
	}()
}

func (rm *recordMarshaler) removeWorker() {
	rm.workersDoneChan <- true
}

func (rm *recordMarshaler) marshal(rec *protocol.Record) (*csvRecord, error) {
	csvRec := &csvRecord{
		streamName: rec.Stream,
	}

	rawRec := rec.GetRawRecord()
	csvRec.data = append(csvRec.data, rawRec.ID)
	csvRec.data = append(csvRec.data, strconv.Itoa(int(rawRec.EmittedAt)))
	csvRec.data = append(csvRec.data, rawRec.Data.String())

	return csvRec, nil
}
