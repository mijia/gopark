package gopark

import (
    "sort"
)

// We need the []interface{} type for the gob to work
type CombinerCreator func(interface{}) []interface{}
type CombinerMerger func([]interface{}, []interface{}) []interface{}
type ValueMerger func([]interface{}, interface{}) []interface{}

type _Aggregator struct {
    combinerCreator CombinerCreator
    combinerMerger  CombinerMerger
    valueMerger     ValueMerger
}

func newMergeAggregator() *_Aggregator {
    a := &_Aggregator{}
    a.combinerCreator = func(x interface{}) []interface{} {
        return []interface{}{x}[:]
    }
    a.combinerMerger = func(x, y []interface{}) []interface{} {
        return append(x, y...)
    }
    a.valueMerger = func(x []interface{}, y interface{}) []interface{} {
        return append(x, y)
    }
    return a
}

type Partitioner interface {
    numPartitions() int
    getPartition(key interface{}) int
}

type _HashPartitioner struct {
    partitions int
}

func (p *_HashPartitioner) numPartitions() int {
    return p.partitions
}

func (p *_HashPartitioner) getPartition(key interface{}) int {
    hashCode := hashCode(key)
    return int(hashCode % int64(p.partitions))
}

func newHashPartitioner(partitions int) Partitioner {
    p := &_HashPartitioner{}
    if partitions < 1 {
        p.partitions = 1
    } else {
        p.partitions = partitions
    }
    return p
}

type _RangePartitioner struct {
    keys    []interface{}
    reverse bool
    fn      KeyLessFunc
}

func (p *_RangePartitioner) numPartitions() int {
    return len(p.keys) + 1
}

func (p *_RangePartitioner) getPartition(key interface{}) int {
    index := sort.Search(len(p.keys), func(i int) bool {
        return !p.fn(p.keys[i], key)
    })
    if !p.reverse {
        return index
    }
    return len(p.keys) - index
}

func newRangePartitioner(fn KeyLessFunc, keys []interface{}, reverse bool) Partitioner {
    p := &_RangePartitioner{}
    p.fn = fn
    p.reverse = reverse
    sorter := NewParkSorter(keys, fn)
    sort.Sort(sorter)
    p.keys = keys
    return p
}
