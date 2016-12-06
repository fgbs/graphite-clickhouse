package render

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"unsafe"
)

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

var errUvarintRead = errors.New("ReadUvarint: Malformed array")
var errUvarintOverflow = errors.New("ReadUvarint: varint overflows a 64-bit integer")

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

// Parse ClickHouse reply in RowBinary format
func Parse(body []byte) (*Data, error) {
	d := &Data{
		Body:     body,
		Blocks:   make([]*Block, 0),
		nameToID: make(map[string]int),
	}

	var err error
	var offset, readBytes int
	var namelen uint64

	bodyLen := len(d.Body)

	block := d.AddBlock()

	name := []byte{}
	id := 0

	var p *Point

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

		newName := d.Body[offset : offset+int(namelen)]
		// name = d.Body[offset : offset+int(namelen)]
		offset += int(namelen)

		if bytes.Compare(newName, name) != 0 {
			name = newName
			id = d.NameToID(unsafeString(name))
			// fmt.Println(unsafeString(name), id)
		}

		time := binary.LittleEndian.Uint32(d.Body[offset : offset+4])
		offset += 4

		value := math.Float64frombits(binary.LittleEndian.Uint64(d.Body[offset : offset+8]))
		offset += 8

		timestamp := binary.LittleEndian.Uint32(d.Body[offset : offset+4])
		offset += 4

		if block.Full() {
			block = d.AddBlock()
		}

		p = block.Add()
		p.id = id
		p.Metric = name
		p.Time = int32(time)
		p.Value = value
		p.Timestamp = int32(timestamp)
	}

	// fmt.Println(body)
	return d, nil
}
