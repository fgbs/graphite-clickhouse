package render

import "sync"

type Point struct {
	id        int
	Metric    []byte
	Time      int32
	Value     float64
	Timestamp int32 // keep max if metric and time equal on two points
}

const BlockSize = 65536

type Block struct {
	Used   int
	Points [BlockSize]Point
}

var BlockPool = sync.Pool{
	New: func() interface{} {
		return &Block{}
	},
}

func GetBlock() *Block {
	return BlockPool.Get().(*Block).Reset()
}

func (b *Block) Reset() *Block {
	b.Used = 0
	return b
}

func (b *Block) Release() {
	b.Used = 0
	BlockPool.Put(b)
}

func (b *Block) Full() bool {
	return b.Used == BlockSize
}

func (b *Block) Add() *Point {
	p := &b.Points[b.Used]
	b.Used++
	return p
}
