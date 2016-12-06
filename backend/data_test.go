package backend

import (
	"encoding/binary"
	"io/ioutil"
	"math"

	"testing"
)

func TestParseData(t *testing.T) {
	body, err := ioutil.ReadFile("testdata/t1.rowbinary")
	if err != nil {
		t.Fatal(err)
	}

	d, err := ParseData(body)
	if err != nil {
		t.FailNow()
	}

	if d.Size() != 163911 {
		t.FailNow()
	}
}

func BenchmarkParseAppend(b *testing.B) {
	// old version

	body, err := ioutil.ReadFile("testdata/t1.rowbinary")
	if err != nil {
		b.Fatal(err)
	}

	parse := func(data []byte) []Point {
		points := make([]Point, 0)
		var namelen uint64
		offset := 0
		readBytes := 0
		l := len(data) - 1

		for {
			if offset >= l {
				break
			}
			namelen, readBytes, err = ReadUvarint(data[offset:])
			if err != nil {
				break
			}
			offset += readBytes
			if len(data[offset:]) < int(namelen)+4+8+4 {
				// logger.Error("Malformed response from clickhouse")
				break
			}

			name := unsafeString(data[offset : offset+int(namelen)])
			offset += int(namelen)

			time := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4

			value := math.Float64frombits(binary.LittleEndian.Uint64(data[offset : offset+8]))
			offset += 8

			timestamp := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4

			points = append(points, Point{
				Metric:    name,
				Time:      int32(time),
				Value:     value,
				Timestamp: int32(timestamp),
			})
		}

		return points
	}

	b.ResetTimer()

	for i := 0; i < b.N; i += 1 {
		points := parse(body)
		if len(points) != 163911 {
			b.FailNow()
		}
	}
}

func BenchmarkParseData(b *testing.B) {
	body, err := ioutil.ReadFile("testdata/t1.rowbinary")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i += 1 {
		d, _ := ParseData(body)
		if d.Size() != 163911 {
			b.FailNow()
		}
		d.Release()
	}
}
