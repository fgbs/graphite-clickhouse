package backend

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/lomik/graphite-clickhouse/carbonzipperpb"
	"github.com/uber-go/zap"
)

// type Point struct {
// 	Metric    string
// 	Time      int32
// 	Value     float64
// 	Timestamp int32 // keep max if metric and time equal on two points
// }

// type Points []Point

// func (s Points) Len() int      { return len(s) }
// func (s Points) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// type ByKey struct{ Points }

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

func (s ByKey) Less(i, j int) bool {
	c := strings.Compare(s.Points[i].Metric, s.Points[j].Metric)

	switch c {
	case -1:
		return true
	case 1:
		return false
	case 0:
		return s.Points[i].Time < s.Points[j].Time
	}

	return false
}

type RenderHandler struct {
	config *Config
}

func (h *RenderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestStart := time.Now()

	defer func() {
		Logger(r.Context()).Debug("total", zap.Duration("time_ns", time.Since(requestStart)))
	}()

	logger := Logger(r.Context())
	target := r.URL.Query().Get("target")

	if strings.IndexByte(target, '\'') > -1 { // sql injection dumb fix
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	var prefix string
	var err error

	if h.config.ClickHouse.ExtraPrefix != "" {
		prefix, target, err = RemoveExtraPrefix(h.config.ClickHouse.ExtraPrefix, target)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if target == "" {
			h.Reply(w, r, make([]Point, 0), 0, 0, "")
			return
		}
	}

	fromTimestamp, err := strconv.ParseInt(r.URL.Query().Get("from"), 10, 32)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	untilTimestamp, err := strconv.ParseInt(r.URL.Query().Get("until"), 10, 32)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	dateWhere := fmt.Sprintf(
		"(Date >='%s' AND Date <= '%s' AND Time >= %d AND Time <= %d)",
		time.Unix(fromTimestamp, 0).Format("2006-01-02"),
		time.Unix(untilTimestamp, 0).Format("2006-01-02"),
		fromTimestamp,
		untilTimestamp,
	)

	var pathWhere string

	if hasWildcard(target) {
		// Search in small index table first
		treeWhere := makeWhere(target, true)
		if treeWhere == "" {
			http.Error(w, "Bad or unsupported query", http.StatusBadRequest)
			return
		}

		treeData, err := Query(
			r.Context(),
			h.config.ClickHouse.Url,
			fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path", h.config.ClickHouse.TreeTable, treeWhere),
			h.config.ClickHouse.TreeTimeout.Value(),
		)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		listBuf := bytes.NewBuffer(nil)
		first := true
		for _, p := range strings.Split(string(treeData), "\n") {
			if p == "" {
				continue
			}

			if !first {
				listBuf.Write([]byte{','})
			}
			first = false

			listBuf.WriteString("'" + p + "'") // SQL-Injection
		}

		if listBuf.Len() == 0 {
			h.Reply(w, r, make([]Point, 0), 0, 0, "")
			return
		}

		pathWhere = fmt.Sprintf(
			"Path IN (%s)",
			string(listBuf.Bytes()),
		)

		// pathWhere = fmt.Sprintf(
		// 	"Path IN (SELECT Path FROM %s WHERE %s)",
		// 	h.config.ClickHouse.DataTable,
		// )
		// pathWhere = makeWhere(target, false)
	} else {
		pathWhere = fmt.Sprintf("Path = '%s'", target)
	}

	// @TODO: change format to RowBinary
	query := fmt.Sprintf(
		`
		SELECT 
			Path, Time, Value, Timestamp
		FROM %s 
		PREWHERE (%s) AND (%s)
		FORMAT RowBinary
		`,
		h.config.ClickHouse.DataTable,
		pathWhere,
		dateWhere,
	)

	data, err := Query(
		r.Context(),
		h.config.ClickHouse.Url,
		query,
		h.config.ClickHouse.DataTimeout.Value(),
	)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	parseStart := time.Now()
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
			logger.Error("Malformed response from clickhouse")
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

	if err != nil && err != io.EOF {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = nil

	logger.Debug("parse", zap.Duration("time_ns", time.Since(parseStart)))

	sortStart := time.Now()
	sort.Sort(ByKey{points})
	logger.Debug("sort", zap.Duration("time_ns", time.Since(sortStart)))

	points = PointsUniq(points)

	// fmt.Printf("%+v\n", points)
	h.Reply(w, r, points, int32(fromTimestamp), int32(untilTimestamp), prefix)
}

func (h *RenderHandler) Reply(w http.ResponseWriter, r *http.Request, points []Point, from, until int32, prefix string) {
	start := time.Now()
	switch r.URL.Query().Get("format") {
	case "pickle":
		h.ReplyPickle(w, r, points, from, until, prefix)
	case "protobuf":
		h.ReplyProtobuf(w, r, points, from, until, prefix)
	}
	Logger(r.Context()).Debug("reply", zap.Duration("time_ns", time.Since(start)))
}

func (h *RenderHandler) ReplyPickle(w http.ResponseWriter, r *http.Request, points []Point, from, until int32, prefix string) {
	var rollupTime time.Duration
	var pickleTime time.Duration

	defer func() {
		Logger(r.Context()).Debug("rollup", zap.Duration("time_ns", rollupTime))
		Logger(r.Context()).Debug("pickle", zap.Duration("time_ns", pickleTime))
	}()

	if len(points) == 0 {
		w.Write(PickleEmptyList)
		return
	}

	writer := bufio.NewWriterSize(w, 1024*1024)
	p := NewPickler(writer)
	defer writer.Flush()

	p.List()

	writeMetric := func(points []Point) {
		rollupStart := time.Now()
		points, step := h.config.Rollup.RollupMetric(points)
		rollupTime += time.Since(rollupStart)

		pickleStart := time.Now()
		p.Dict()

		p.String("name")
		if prefix != "" {
			p.String(prefix + "." + points[0].Metric)
		} else {
			p.String(points[0].Metric)
		}
		p.SetItem()

		p.String("step")
		p.Uint32(uint32(step))
		p.SetItem()

		start := from - (from % step)
		if start < from {
			start += step
		}
		end := until - (until % step)
		last := start - step

		p.String("values")
		p.List()
		for _, point := range points {
			if point.Time < start || point.Time > end {
				continue
			}

			if point.Time > last+step {
				p.AppendNulls(int(((point.Time - last) / step) - 1))
			}

			p.AppendFloat64(point.Value)

			last = point.Time
		}

		if end > last {
			p.AppendNulls(int((end - last) / step))
		}
		p.SetItem()

		p.String("start")
		p.Uint32(uint32(start))
		p.SetItem()

		p.String("end")
		p.Uint32(uint32(end))
		p.SetItem()

		p.Append()
		pickleTime += time.Since(pickleStart)
	}

	// group by Metric
	var i, n int
	// i - current position of iterator
	// n - position of the first record with current metric
	l := len(points)

	for i = 1; i < l; i++ {
		if points[i].Metric != points[n].Metric {
			writeMetric(points[n:i])
			n = i
			continue
		}
	}
	writeMetric(points[n:i])

	p.Stop()
}

func (h *RenderHandler) ReplyProtobuf(w http.ResponseWriter, r *http.Request, points []Point, from, until int32, prefix string) {
	var rollupTime time.Duration
	var protobufTime time.Duration

	defer func() {
		Logger(r.Context()).Debug("rollup", zap.Duration("time_ns", rollupTime))
		Logger(r.Context()).Debug("protobuf", zap.Duration("time_ns", protobufTime))
	}()

	if len(points) == 0 {
		return
	}

	var multiResponse carbonzipperpb.MultiFetchResponse

	writeMetric := func(points []Point) {
		rollupStart := time.Now()
		points, step := h.config.Rollup.RollupMetric(points)
		rollupTime += time.Since(rollupStart)

		protobufStart := time.Now()
		var name string

		if prefix != "" {
			name = prefix + "." + points[0].Metric
		} else {
			name = points[0].Metric
		}

		start := from - (from % step)
		if start < from {
			start += step
		}
		stop := until - (until % step)
		count := ((stop - start) / step) + 1

		response := carbonzipperpb.FetchResponse{
			Name:      proto.String(name),
			StartTime: &start,
			StopTime:  &stop,
			StepTime:  &step,
			Values:    make([]float64, count),
			IsAbsent:  make([]bool, count),
		}

		var index int32
		// skip points before start
		for index = 0; points[index].Time < start; index++ {
		}

		for i := int32(0); i < count; i++ {
			if index < int32(len(points)) && points[index].Time == start+step*i {
				response.Values[i] = points[index].Value
				response.IsAbsent[i] = false
				index++
			} else {
				response.Values[i] = 0
				response.IsAbsent[i] = true
			}
		}

		multiResponse.Metrics = append(multiResponse.Metrics, &response)
		protobufTime += time.Since(protobufStart)
	}

	// group by Metric
	var i, n int
	// i - current position of iterator
	// n - position of the first record with current metric
	l := len(points)

	for i = 1; i < l; i++ {
		if points[i].Metric != points[n].Metric {
			writeMetric(points[n:i])
			n = i
			continue
		}
	}
	writeMetric(points[n:i])

	protobufStart := time.Now()

	body, _ := proto.Marshal(&multiResponse)
	protobufTime += time.Since(protobufStart)
	w.Write(body)
}

func NewRenderHandler(config *Config) *RenderHandler {
	return &RenderHandler{
		config: config,
	}
}
