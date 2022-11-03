package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

type recordMarshaler struct {
	hub             messenger.ChannelHub
	csvRecordChann  csvRecordChannel
	workersDoneChan chan bool
	fieldsPerStream map[string][]string
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
		fieldsPerStream: map[string][]string{},
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
			headers := []string{}

			for propertyName := range stream.Stream.JSONSchema.Properties {
				headers = append(headers, string(propertyName))
			}

			csvRec := &csvRecord{
				streamName: stream.Stream.Name,
				data:       headers,
			}

			rm.csvRecordChann <- csvRec

			rm.fieldsPerStream[stream.Stream.Name] = headers
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

	fields := rm.fieldsPerStream[rec.Stream]

	for _, f := range fields {
		data := *rec.Data

		str, err := convertToString(data[f])
		if err != nil {
			panic(err)
		}

		csvRec.data = append(csvRec.data, str)
	}

	return csvRec, nil
}

func convertToString(v interface{}) (string, error) {
	switch assert := v.(type) {
	case nil:
		return "", nil

	case string:
		return assert, nil

	case int:
		return strconv.Itoa(assert), nil

	case float64:
		if assert == float64(int64(assert)) {
			return strconv.Itoa(int(assert)), nil
		}
		return fmt.Sprintf("%f", assert), nil

	case []interface{}:
		strValues := []string{}

		for _, a := range assert {
			sa, err := convertToString(a)
			if err != nil {
				return "", err
			}

			strValues = append(strValues, sa)
		}

		bt, err := json.Marshal(strValues)
		if err != nil {
			return "", err
		}

		return string(bt), nil

	case map[string]interface{}:
		strValues := []string{}

		for _, a := range assert {
			sa, err := convertToString(a)
			if err != nil {
				return "", err
			}

			strValues = append(strValues, sa)
		}

		bt, err := json.Marshal(strValues)
		if err != nil {
			return "", err
		}

		return string(bt), nil

	default:
		return "", fmt.Errorf("type not supported for %v", v)
	}
}
