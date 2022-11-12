package pkg

// CsvRecordChannel is a channel to share csv records
type CsvRecordChannel chan *csvRecord

type csvRecord struct {
	streamName string
	data       []string
}

// NewCsvRecordChannel creates a new instance of CsvRecordChannel
func NewCsvRecordChannel() CsvRecordChannel {
	return make(CsvRecordChannel)
}
