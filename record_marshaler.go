package main

import (
	"strconv"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

type recordMarshaler struct {
	hub             messenger.ChannelHub
	csvRecordChann  csvRecordChannel
	workersDoneChan chan bool
}

func newRecordMarshaler(
	hub messenger.ChannelHub,
	csvRecordChann csvRecordChannel,
	workersDoneChan chan bool,
) *recordMarshaler {
	return &recordMarshaler{
		hub:             hub,
		csvRecordChann:  csvRecordChann,
		workersDoneChan: workersDoneChan,
	}
}

func (rm *recordMarshaler) addWorker() {
	go func() {
		for {
			rec, channelOpen := <-rm.hub.GetRecordChannel()
			if !channelOpen {
				rm.removeWorker()
				return
			}

			csvRec, err := rm.marshal(rec)
			if err != nil {
				rm.hub.GetErrorChannel() <- err
				continue
			}

			rm.csvRecordChann <- csvRec
		}
	}()
}

func (rm *recordMarshaler) writeHeaders(streams []protocol.ConfiguredStream) {
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
