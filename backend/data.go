package backend

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"sync"
)

const PointBufferSize = 65536

type Record struct {
	Metric    []byte
	Time      int32
	Value     float64
	Timestamp int32 // keep max if metric and time equal on two points
}

type PointBuffer struct {
	Used   int
	Points [PointBufferSize]Record
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

	prevName := []byte{}

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

		name := d.Body[offset : offset+int(namelen)]
		offset += int(namelen)

		if bytes.Compare(name, prevName) == 0 {
			name = prevName
		} else {
			prevName = name
		}

		time := binary.LittleEndian.Uint32(d.Body[offset : offset+4])
		offset += 4

		value := math.Float64frombits(binary.LittleEndian.Uint64(d.Body[offset : offset+8]))
		offset += 8

		timestamp := binary.LittleEndian.Uint32(d.Body[offset : offset+4])
		offset += 4

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

func (d *Data) Len() int {
	total := 0
	for _, p := range d.Points {
		total += p.Used
	}
	return total
}

func (d *Data) p(i int) *Record {
	return &d.Points[i/PointBufferSize].Points[i%PointBufferSize]
}

func (d *Data) Less(i, j int) bool {
	p1, p2 := d.p(i), d.p(j)

	var c int
	// if int(&p1.Metric) == int(&p2.Metric) {
	// c = 0
	// } else {
	c = bytes.Compare([]byte(p1.Metric), []byte(p2.Metric))
	// }

	switch c {
	case -1:
		return true
	case 1:
		return false
	case 0:
		return p1.Time < p2.Time
	}

	return false
}

func (d *Data) Swap(i, j int) {
	p1, p2 := d.p(i), d.p(j)
	*p1, *p2 = *p2, *p1
}
