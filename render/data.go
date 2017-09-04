package render

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"unsafe"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/point"
)

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

var errUvarintRead = errors.New("ReadUvarint: Malformed array")
var errUvarintOverflow = errors.New("ReadUvarint: varint overflows a 64-bit integer")
var errClickHouseResponse = errors.New("Malformed response from clickhouse")

func ReadUvarint(array []byte) (uint64, int, error) {
	var x uint64
	var s uint
	l := len(array) - 1
	for i := 0; ; i++ {
		if i > l {
			return x, i + 1, errUvarintRead
		}
		if array[i] < 0x80 {
			if i > 9 || i == 9 && array[i] > 1 {
				return x, i + 1, errUvarintOverflow
			}
			return x | uint64(array[i])<<s, i + 1, nil
		}
		x |= uint64(array[i]&0x7f) << s
		s += 7
	}
}

type DataValue struct {
	id   int
	name string
}

type Data struct {
	body        []byte // raw RowBinary from clickhouse
	Points      []point.Point
	nameToID    map[string]*DataValue
	revNameToID map[string]*DataValue
	maxID       int

	prevName         []byte
	prevValue        *DataValue
	prevNameReverse  []byte
	prevValueReverse *DataValue

	Finder finder.Finder
}

func (d *Data) value(name string) *DataValue {
	v := d.nameToID[name]
	if v == nil {
		d.maxID++
		v = &DataValue{
			id:   d.maxID,
			name: name,
		}
		d.nameToID[name] = v
	}
	return v
}

func (d *Data) NameToID(name []byte) (int, string) {
	if bytes.Compare(name, d.prevName) != 0 {
		d.prevName = name
		d.prevValue = d.value(unsafeString(name))
	}

	return d.prevValue.id, d.prevValue.name
}

func (d *Data) RevNameToID(revName []byte) (int, string) {

	if bytes.Compare(revName, d.prevNameReverse) != 0 {
		v := d.revNameToID[unsafeString(revName)]
		if v == nil {
			v = d.value(finder.ReverseString(unsafeString(revName)))
			d.revNameToID[unsafeString(revName)] = v
		}

		d.prevNameReverse = revName
		d.prevValueReverse = v
	}

	return d.prevValueReverse.id, d.prevValueReverse.name
}

func DataCount(body []byte) (int, error) {
	var namelen uint64
	bodyLen := len(body)
	var count, offset, readBytes int
	var err error

	for {
		if offset >= bodyLen {
			if offset == bodyLen {
				return count, nil
			}
			return 0, errClickHouseResponse
		}
		namelen, readBytes, err = ReadUvarint(body[offset:])
		if err != nil {
			return 0, err
		}
		offset += readBytes + int(namelen) + 16
		count++
	}

	return 0, nil
}

func DataParse(body []byte, extraPoints []point.Point) (*Data, error) {
	count, err := DataCount(body)
	if err != nil {
		return nil, err
	}

	d := &Data{
		Points:      make([]point.Point, count+len(extraPoints)),
		nameToID:    make(map[string]*DataValue),
		revNameToID: make(map[string]*DataValue),
	}

	var namelen uint64
	offset := 0
	readBytes := 0
	bodyLen := len(body)
	index := 0

	// add extraPoints. With NameToID
	for i := 0; i < len(extraPoints); i++ {
		d.Points[index] = extraPoints[i]
		d.Points[index].MetricID, d.Points[index].Metric = d.NameToID([]byte(d.Points[index].Metric))
		index++
	}

	for {
		if offset >= bodyLen {
			if offset == bodyLen {
				break
			}
			return nil, errClickHouseResponse
		}

		namelen, readBytes, err = ReadUvarint(body[offset:])
		if err != nil {
			return nil, errClickHouseResponse
		}
		offset += readBytes

		if bodyLen-offset < int(namelen)+16 {
			return nil, errClickHouseResponse
		}

		name := body[offset : offset+int(namelen)]
		offset += int(namelen)

		time := binary.LittleEndian.Uint32(body[offset : offset+4])
		offset += 4

		value := math.Float64frombits(binary.LittleEndian.Uint64(body[offset : offset+8]))
		offset += 8

		timestamp := binary.LittleEndian.Uint32(body[offset : offset+4])
		offset += 4

		d.Points[index].MetricID, d.Points[index].Metric = d.NameToID(name)
		d.Points[index].Time = int32(time)
		d.Points[index].Value = value
		d.Points[index].Timestamp = int32(timestamp)
		index++
	}

	return d, nil
}

func (d *Data) Len() int {
	return len(d.Points)
}

func (d *Data) Less(i, j int) bool {
	if d.Points[i].MetricID == d.Points[j].MetricID {
		return d.Points[i].Time < d.Points[j].Time
	}

	return d.Points[i].MetricID < d.Points[j].MetricID
}

func (d *Data) Swap(i, j int) {
	d.Points[i], d.Points[j] = d.Points[j], d.Points[i]
}
