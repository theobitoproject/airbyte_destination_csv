package csv

import (
	"os"
	"strings"

	"github.com/theobitoproject/kankuro/pkg/messenger"
	"github.com/theobitoproject/kankuro/pkg/protocol"
)

const (
	// TODO: is there are best way to handle the amount of workers?
	marshalerWorkers = 4
	writerWorkers    = 2
)

// DestinationCsv is the Airbyte destination connector
// to write data in csv files
type DestinationCsv struct {
	rootPath string

	m Marshaler
	w Writer

	rowChan              RowChannel
	marshalerWorkersChan chan (bool)
	writerWorkersChan    chan (bool)
}

type destinationConfiguration struct {
	DestinationPath string `json:"destination_path"`
}

// NewDestinationCsv creates a new instance of DestinationCsv
func NewDestinationCsv(
	rootPath string,
	m Marshaler,
	w Writer,
	rowChan RowChannel,
	marshalerWorkersChan chan (bool),
	writerWorkersChan chan (bool),
) *DestinationCsv {
	return &DestinationCsv{
		rootPath,
		m,
		w,
		rowChan,
		marshalerWorkersChan,
		writerWorkersChan,
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
						Title:       "Destination path",
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

	d.m.ExtractHeaders(cc.Streams)
	for i := 0; i < marshalerWorkers; i++ {
		d.m.AddWorker(hub)
	}

	for i := 0; i < writerWorkers; i++ {
		d.w.AddWorker(hub, absoluteDestinationPath)
	}

	for i := 0; i < marshalerWorkers; i++ {
		<-d.marshalerWorkersChan
	}

	close(d.rowChan)

	for i := 0; i < writerWorkers; i++ {
		<-d.writerWorkersChan
	}

	close(d.marshalerWorkersChan)
	close(d.writerWorkersChan)

	d.w.CloseAndFlush()

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
