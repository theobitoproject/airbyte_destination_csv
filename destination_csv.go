package main

import (
	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

const (
	recordMarshalerWorkers = 4
	csvWriterWorkers       = 2
)

type destinationCsv struct{}

type destinationConfiguration struct {
	DestinationPath string `json:"destination_path"`
}

func newDestinationCsv() *destinationCsv {
	return &destinationCsv{}
}

// Spec returns the schema which described how the destination connector can be configured
func (d *destinationCsv) Spec(
	mw messenger.MessageWriter,
	cp messenger.ConfigParser,
) (*protocol.ConnectorSpecification, error) {
	return &protocol.ConnectorSpecification{
		DocumentationURL:      "https://example-csv-api.com/",
		ChangeLogURL:          "https://example-csv-api.com/",
		SupportsIncremental:   false,
		SupportsNormalization: false,
		SupportsDBT:           false,
		SupportedDestinationSyncModes: []protocol.DestinationSyncMode{
			protocol.DestinationSyncModeOverwrite,
		},
		ConnectionSpecification: protocol.ConnectionSpecification{
			Title:       "Golang - Local CSV",
			Description: "This destination writes all data in CSV files",
			Type:        "object",
			Required:    []protocol.PropertyName{"destination_path"},
			Properties: protocol.Properties{
				Properties: map[protocol.PropertyName]protocol.PropertySpec{
					"destination_path": {
						Description: "path where files will be placed",
						PropertyType: protocol.PropertyType{
							Type: []protocol.PropType{
								protocol.String,
							},
						},
					},
				},
			},
		},
	}, nil
}

// Check verifies that, given a configuration, data can be accessed properly
func (d *destinationCsv) Check(
	mw messenger.MessageWriter,
	cp messenger.ConfigParser,
) error {
	return nil
}

// Write takes the data from the record channel
// and stores it in the destination
// Note: all channels except record channel from hub needs to be closed
func (d *destinationCsv) Write(
	cc *protocol.ConfiguredCatalog,
	mw messenger.MessageWriter,
	cp messenger.ConfigParser,
	hub messenger.ChannelHub,
) {
	err := mw.WriteLog(protocol.LogLevelInfo, "running write from csv dst")
	if err != nil {
		hub.GetErrorChannel() <- err
	}

	var dc destinationConfiguration
	err = cp.UnmarshalSourceConfigPath(&dc)
	if err != nil {
		hub.GetErrorChannel() <- err
		return
	}

	csvRecordChan := newCsvRecordChannel()
	recordMarshalerWorkersChan := make(chan bool)
	csvWriterWorkersChan := make(chan bool)

	rm := newRecordMarshaler(hub, csvRecordChan, recordMarshalerWorkersChan)
	rm.writeHeaders(cc.Streams)
	for i := 0; i < recordMarshalerWorkers; i++ {
		rm.addWorker()
	}

	cw := newCsvWriter(hub, csvRecordChan, csvWriterWorkersChan)
	for i := 0; i < csvWriterWorkers; i++ {
		cw.addWorker()
	}

	for i := 0; i < recordMarshalerWorkers; i++ {
		<-recordMarshalerWorkersChan
	}
	close(csvRecordChan)
	for i := 0; i < csvWriterWorkers; i++ {
		<-csvWriterWorkersChan
	}

	close(recordMarshalerWorkersChan)
	close(csvWriterWorkersChan)

	cw.closeAndFlush()

	close(hub.GetErrorChannel())
}
