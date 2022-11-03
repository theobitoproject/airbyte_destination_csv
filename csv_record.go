package main

type csvRecord struct {
	streamName string
	data       []string
}

type csvRecordChannel chan *csvRecord

func newCsvRecordChannel() csvRecordChannel {
	return make(csvRecordChannel)
}
