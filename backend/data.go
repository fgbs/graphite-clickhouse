package backend

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
)

type Point struct {
	Metric    string
	Time      int32
	Value     float64
	Timestamp int32 // keep max if metric and time equal on two points
}

type Points []Point

func (s Points) Len() int      { return len(s) }
func (s Points) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type ByKey struct{ Points }

const PointBufferSize = 65536

type PointBuffer struct {
	Used   int
	Points [PointBufferSize]Point
}

var PointBufferPool = sync.Pool{
	New: func() interface{} {
		return &PointBuffer{}
	},
}

func GetPointBuffer() *PointBuffer {
	return PointBufferPool.Get().(*PointBuffer).Reset()
}

func (b *PointBuffer) Reset() *PointBuffer {
	b.Used = 0
	return b
}

func (b *PointBuffer) Release() {
	b.Used = 0
	PointBufferPool.Put(b)
}

type Data struct {
	Body   []byte // raw RowBinary data from ClickHouse
	Points []*PointBuffer
}

func ParseData(body []byte) (*Data, error) {
	d := &Data{
		Body:   body,
		Points: make([]*PointBuffer, 0),
	}

	d.Points = append(d.Points, GetPointBuffer())

	var err error
	var offset, readBytes int
	var namelen uint64

	bodyLen := len(d.Body)

	points := d.Points[len(d.Points)-1]

ParseLoop:
	for {
		if offset == bodyLen {
			break ParseLoop
		}

		namelen, readBytes, err = ReadUvarint(d.Body[offset:])
		if err != nil {
			break ParseLoop
		}

		offset += readBytes
		if bodyLen-offset < int(namelen)+4+8+4 {
			err = errors.New("Malformed response from clickhouse")
			break ParseLoop
		}

		name := unsafeString(d.Body[offset : offset+int(namelen)])
		offset += int(namelen)

		time := binary.LittleEndian.Uint32(d.Body[offset : offset+4])
		offset += 4

		value := math.Float64frombits(binary.LittleEndian.Uint64(d.Body[offset : offset+8]))
		offset += 8

		timestamp := binary.LittleEndian.Uint32(d.Body[offset : offset+4])
		offset += 4

		// fmt.Println(string(name), time, value, timestamp)

		if points.Used == PointBufferSize {
			d.Points = append(d.Points, GetPointBuffer())
			points = d.Points[len(d.Points)-1]
		}

		points.Points[points.Used].Metric = name
		points.Points[points.Used].Time = int32(time)
		points.Points[points.Used].Value = value
		points.Points[points.Used].Timestamp = int32(timestamp)
		points.Used++
	}

	// fmt.Println(body)
	return d, nil
}

func (d *Data) Release() {
	for _, p := range d.Points {
		p.Release()
	}
}

func (d *Data) Size() int {
	total := 0
	for _, p := range d.Points {
		total += p.Used
	}
	return total
}
