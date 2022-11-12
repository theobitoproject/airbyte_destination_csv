package pkg

import (
	"os"
	"strings"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

const (
	// TODO: is there are best way to handle the amount of workers?
	recordMarshalerWorkers = 4
	csvWriterWorkers       = 2
)

// DestinationCsv is the Airbyte destination connector
// to write data in csv files
type DestinationCsv struct {
	rootPath string

	rm RecordMarshaler
	cw CsvWriter

	csvRecordChan              CsvRecordChannel
	recordMarshalerWorkersChan chan (bool)
	csvWriterWorkersChan       chan (bool)
}

type destinationConfiguration struct {
	DestinationPath string `json:"destination_path"`
}

// NewDestinationCsv creates a new instance of DestinationCsv
func NewDestinationCsv(
	rootPath string,
	rm RecordMarshaler,
	cw CsvWriter,
	csvRecordChan CsvRecordChannel,
	recordMarshalerWorkersChan chan (bool),
	csvWriterWorkersChan chan (bool),
) *DestinationCsv {
	return &DestinationCsv{
		rootPath,
		rm,
		cw,
		csvRecordChan,
		recordMarshalerWorkersChan,
		csvWriterWorkersChan,
	}
}

// Spec returns the schema which described how the destination connector can be configured
func (d *DestinationCsv) Spec(
	mw messenger.MessageWriter,
	cp messenger.ConfigParser,
) (*protocol.ConnectorSpecification, error) {
	return &protocol.ConnectorSpecification{
		DocumentationURL:      "https://example-csv-api.com/",
		ChangeLogURL:          "https://example-csv-api.com/",
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
func (d *DestinationCsv) Check(
	mw messenger.MessageWriter,
	cp messenger.ConfigParser,
) error {
	// TODO: check properly
	return nil
}

// Write takes the data from the record channel
// and stores it in the destination
// Note: all channels except record channel from hub needs to be closed
func (d *DestinationCsv) Write(
	cc *protocol.ConfiguredCatalog,
	mw messenger.MessageWriter,
	cp messenger.ConfigParser,
	hub messenger.ChannelHub,
) {
	err := mw.WriteLog(protocol.LogLevelInfo, "running write from csv dst")
	if err != nil {
		hub.GetErrorChannel() <- err
	}

	absoluteDestinationPath, err := d.createDestinationPath(cp)
	if err != nil {
		hub.GetErrorChannel() <- err
		return
	}

	// rm := newRecordMarshaler(hub, csvRecordChan, recordMarshalerWorkersChan)
	d.rm.ExtractHeaders(cc.Streams)
	for i := 0; i < recordMarshalerWorkers; i++ {
		d.rm.AddWorker(hub)
	}

	// cw := newCsvWriter(
	// 	hub,
	// 	csvRecordChan,
	// 	absoluteDestinationPath,
	// 	csvWriterWorkersChan,
	// )
	for i := 0; i < csvWriterWorkers; i++ {
		d.cw.AddWorker(hub, absoluteDestinationPath)
	}

	for i := 0; i < recordMarshalerWorkers; i++ {
		<-d.recordMarshalerWorkersChan
	}
	close(d.csvRecordChan)
	for i := 0; i < csvWriterWorkers; i++ {
		<-d.csvWriterWorkersChan
	}

	close(d.recordMarshalerWorkersChan)
	close(d.csvWriterWorkersChan)

	d.cw.CloseAndFlush()

	close(hub.GetErrorChannel())
}

func (d *DestinationCsv) createDestinationPath(
	cp messenger.ConfigParser,
) (string, error) {
	var dc destinationConfiguration
	err := cp.UnmarshalConfigPath(&dc)
	if err != nil {
		return "", err
	}

	// TODO: is this the best way to check destination path
	// starts with "/"
	destinationPath := dc.DestinationPath
	if !strings.HasPrefix(destinationPath, "/") {
		destinationPath = "/" + destinationPath
	}

	absoluteDestinationPath := d.rootPath + destinationPath

	err = os.MkdirAll(absoluteDestinationPath, os.ModePerm)
	if err != nil {
		return "", err
	}

	return absoluteDestinationPath, nil
}
