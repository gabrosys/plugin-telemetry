package queries

import "encoding/binary"

// SeriesID returns an identifier of a series: the metric and each label key and value, each as a
// uvarint length and its bytes, so different series never share an ID.
func SeriesID(metric string, labels []Label) string {
	n := len(metric) + binary.MaxVarintLen64
	for _, l := range labels {
		n += len(l.Key) + len(l.Value) + 2*binary.MaxVarintLen64
	}
	b := make([]byte, 0, n)
	b = appendString(b, metric)
	for _, l := range labels {
		b = appendString(b, l.Key)
		b = appendString(b, l.Value)
	}

	return string(b)
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))

	return append(b, s...)
}
