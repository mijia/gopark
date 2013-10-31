package gopark

import (
    "github.com/mijia/ty"
    "log"
    "reflect"
    "sort"
)

type CombinerCreator func(interface{}) interface{}
type CombinerMerger func(interface{}, interface{}) interface{}
type ValueMerger func(interface{}, interface{}) interface{}

type Aggregator struct {
    combinerCreator CombinerCreator
    combinerMerger  CombinerMerger
    valueMerger     ValueMerger
}

func newMergeAggregator() *Aggregator {
    a := &Aggregator{}
    a.combinerCreator = func(x interface{}) interface{} {
        vx := reflect.ValueOf(x)
        vxs := reflect.MakeSlice(reflect.SliceOf(vx.Type()), 1, 1)
        vxs.Index(0).Set(vx)
        return vxs.Interface()
    }
    a.combinerMerger = func(x, y interface{}) interface{} {
        vx, vy := reflect.ValueOf(x), reflect.ValueOf(y)
        if vx.Kind() != reflect.Slice || vy.Kind() != reflect.Slice {
            log.Panicf("MergeAggregator needs two slice, instead of %s and %s", vx.Type(), vy.Type())
        }
        return reflect.AppendSlice(vx, vy).Interface()
    }
    a.valueMerger = func(x interface{}, y interface{}) interface{} {
        chk := ty.Check(new(func([]ty.A, ty.A) []ty.A), x, y)
        vx, vy := chk.Args[0], chk.Args[1]
        return reflect.Append(vx, vy).Interface()
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
    fn      interface{}
}

func (p *_RangePartitioner) numPartitions() int {
    return len(p.keys) + 1
}

func (p *_RangePartitioner) getPartition(key interface{}) int {
    index := sort.Search(len(p.keys), func(i int) bool {
        chk := ty.Check(new(func(func(ty.A, ty.A) bool, ty.A, ty.A) bool), p.fn, p.keys[i], key)
        vfn, vx, vy := chk.Args[0], chk.Args[1], chk.Args[2]
        isLess := vfn.Call([]reflect.Value{vx, vy})[0].Interface().(bool)
        return !isLess
    })
    if !p.reverse {
        return index
    }
    return len(p.keys) - index
}

func newRangePartitioner(fn interface{}, keys []interface{}, reverse bool) Partitioner {
    p := &_RangePartitioner{}
    p.fn = fn
    p.reverse = reverse
    sorter := NewParkSorter(keys, fn)
    sort.Sort(sorter)
    p.keys = keys
    return p
}
