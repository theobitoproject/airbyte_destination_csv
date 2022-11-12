package csv

// RowChannel is a channel to share rows
type RowChannel chan *row

type row struct {
	streamName string
	data       []string
}

// NewRowChannel creates a new instance of RowChannel
func NewRowChannel() RowChannel {
	return make(RowChannel)
}
