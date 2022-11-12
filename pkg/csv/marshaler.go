package csv

import (
	"strconv"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

// Marshaler takes incoming record messages,
// creates rows and send these to the next stage of processing
type Marshaler interface {
	// AddWorker adds a new thread to marshal records async
	// and send these to the next stage of processing
	AddWorker(messenger.ChannelHub)
	// ExtractHeaders defines the headers that should be written
	// in the csv file, creates a row
	// and send this to the next stage of processing
	ExtractHeaders([]protocol.ConfiguredStream)
}

type marshaler struct {
	rowChan         RowChannel
	workersDoneChan chan bool
}

// NewMarshaler creates a new instance of Marshaler
func NewMarshaler(
	rowChan RowChannel,
	workersDoneChan chan bool,
) Marshaler {
	return &marshaler{
		rowChan:         rowChan,
		workersDoneChan: workersDoneChan,
	}
}

// AddWorker adds a new thread to marshal records async
// and send these to the next stage of processing
func (m *marshaler) AddWorker(hub messenger.ChannelHub) {
	go func() {
		for {
			rec, channelOpen := <-hub.GetRecordChannel()
			if !channelOpen {
				m.removeWorker()
				return
			}

			r, err := m.marshal(rec)
			if err != nil {
				hub.GetErrorChannel() <- err
				continue
			}

			m.rowChan <- r
		}
	}()
}

// ExtractHeaders defines the headers that should be written
// in the csv file, creates a row
// and send this to the next stage of processing
func (m *marshaler) ExtractHeaders(streams []protocol.ConfiguredStream) {
	go func() {
		for _, stream := range streams {
			r := &row{
				streamName: stream.Stream.Name,
				data: []string{
					protocol.AirbyteAbId,
					protocol.AirbyteEmittedAt,
					protocol.AirbyteData,
				},
			}

			m.rowChan <- r
		}
	}()
}

func (m *marshaler) removeWorker() {
	m.workersDoneChan <- true
}

func (m *marshaler) marshal(rec *protocol.Record) (*row, error) {
	r := &row{
		streamName: rec.Stream,
	}

	rawRec := rec.GetRawRecord()
	r.data = append(r.data, rawRec.ID)
	r.data = append(r.data, strconv.Itoa(int(rawRec.EmittedAt)))
	r.data = append(r.data, rawRec.Data.String())

	return r, nil
}
