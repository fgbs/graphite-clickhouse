package render

type Data struct {
	Body     []byte // raw RowBinary data from ClickHouse
	Blocks   []*Block
	nameToID map[string]int
	maxID    int
}

func (d *Data) AddBlock() *Block {
	d.Blocks = append(d.Blocks, GetBlock())
	return d.Blocks[len(d.Blocks)-1]
}

func (d *Data) Release() {
	for _, b := range d.Blocks {
		b.Release()
	}
}

func (d *Data) Len() int {
	total := 0
	for _, b := range d.Blocks {
		total += b.Used
	}
	return total
}

func (d *Data) NameToID(name string) int {
	id := d.nameToID[name]
	if id == 0 {
		d.maxID++
		id = d.maxID
		d.nameToID[name] = id
	}
	return id
}
