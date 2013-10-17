package gopark

import (
    "bytes"
    "encoding/gob"
    "fmt"
    "io/ioutil"
    "log"
    "sort"
)

type MapperFunc func(interface{}) interface{}
type PartitionMapperFunc func(Yielder) Yielder
type FlatMapperFunc func(interface{}) []interface{}
type ReducerFunc func(interface{}, interface{}) interface{}
type FilterFunc func(interface{}) bool
type LoopFunc func(interface{})

type RDD interface {
    Map(f MapperFunc) RDD
    MapPartition(f PartitionMapperFunc) RDD
    FlatMap(f FlatMapperFunc) RDD
    Filter(f FilterFunc) RDD
    Sample(fraction float32, seed int64) RDD
    GroupByKey() RDD
    SortByKey(fn KeyLessFunc, reverse bool) RDD
    PartitionByKey() RDD
    ReduceByKey(fn ReducerFunc) RDD
    Distinct() RDD
    Union(other RDD) RDD
    Join(other RDD) RDD
    LeftOuterJoin(other RDD) RDD
    RightOuterJoin(other RDD) RDD
    GroupWith(other RDD) RDD
    Cartesian(other RDD) RDD

    GroupByKey_N(numPartitions int) RDD
    SortByKey_N(fn KeyLessFunc, reverse bool, numPartitions int) RDD
    PartitionByKey_N(numPartitions int) RDD
    ReduceByKey_N(fn ReducerFunc, numPartitions int) RDD
    Distinct_N(numPartitions int) RDD
    Join_N(other RDD, numPartitions int) RDD
    LeftOuterJoin_N(other RDD, numPartitions int) RDD
    RightOuterJoin_N(other RDD, numPartitions int) RDD
    GroupWith_N(other RDD, numPartitions int) RDD

    Reduce(fn ReducerFunc) interface{}
    CountByKey() map[interface{}]int64
    CountByValue() map[interface{}]int64
    Take(n int64) []interface{}
    Collect() []interface{}
    CollectAsMap() map[interface{}]interface{}
    Count() int64
    Foreach(fn LoopFunc)
    SaveAsTextFile(pathname string)

    Cache() RDD
    Persist() RDD

    getId() int64
    getContext() *Context
    getSplits() []Split
    getSplit(int) Split
    len() int
    traverse(split Split) Yielder
    compute(split Split) Yielder
}

type Split interface {
    getIndex() int
}

type _BaseRDD struct {
    ctx             *Context
    prototype       RDD
    id              int64
    splits          []Split
    shouldCache     bool
    cache           [][]interface{}
    shouldPersist   bool
    persistLocation string
    length          int
}

//////////////////////////////////////////////////////
// Base RDD operations implementation
//////////////////////////////////////////////////////
func (r *_BaseRDD) Cache() RDD {
    if !r.shouldCache {
        r.shouldCache = true
        r.cache = make([][]interface{}, r.length)
    }
    return r.prototype
}

func (r *_BaseRDD) Persist() RDD {
    if !r.shouldPersist {
        r.shouldPersist = true
    }
    return r.prototype
}

func (r *_BaseRDD) Union(other RDD) RDD {
    rdds := []RDD{r.prototype, other}[:]
    return newUnionRDD(r.ctx, rdds)
}

func (r *_BaseRDD) Join(other RDD) RDD {
    return r.Join_N(other, 0)
}

func (r *_BaseRDD) Join_N(other RDD, numPartitions int) RDD {
    return r.join(other, numPartitions, true, true)
}

func (r *_BaseRDD) LeftOuterJoin(other RDD) RDD {
    return r.LeftOuterJoin_N(other, 0)
}

func (r *_BaseRDD) LeftOuterJoin_N(other RDD, numPartitions int) RDD {
    return r.join(other, numPartitions, true, false)
}

func (r *_BaseRDD) RightOuterJoin(other RDD) RDD {
    return r.RightOuterJoin_N(other, 0)
}

func (r *_BaseRDD) RightOuterJoin_N(other RDD, numPartitions int) RDD {
    return r.join(other, numPartitions, false, true)
}

func (r *_BaseRDD) join(other RDD, numPartitions int, needLeft, needRight bool) RDD {
    return r.GroupWith_N(other, numPartitions).FlatMap(func(x interface{}) []interface{} {
        keyGroups := x.(*KeyGroups)
        groups := keyGroups.Groups
        if needLeft && len(groups[0]) == 0 {
            return nil
        }
        if needRight && len(groups[1]) == 0 {
            return nil
        }
        results := make([]interface{}, 0)
        if len(groups[0]) == 0 {
            groups[0] = append(groups[0], nil)
        }
        if len(groups[1]) == 0 {
            groups[1] = append(groups[1], nil)
        }
        for _, leftValue := range groups[0] {
            for _, rightValue := range groups[1] {
                results = append(results, &KeyValue{
                    Key:   keyGroups.Key,
                    Value: []interface{}{leftValue, rightValue}[:],
                })
            }
        }
        return results
    })
}

func (r *_BaseRDD) GroupWith(other RDD) RDD {
    return r.GroupWith_N(other, 0)
}

func (r *_BaseRDD) GroupWith_N(other RDD, numPartitions int) RDD {
    if numPartitions <= 0 {
        switch {
        case env.parallel == 0:
            numPartitions = r.len()
        default:
            numPartitions = env.parallel
        }
    }
    rdds := []RDD{r.prototype, other}[:]
    return newCoGroupedRDD(r.ctx, rdds, numPartitions)
}

func (r *_BaseRDD) SortByKey(fn KeyLessFunc, reverse bool) RDD {
    return r.SortByKey_N(fn, reverse, 0)
}

func (r *_BaseRDD) SortByKey_N(fn KeyLessFunc, reverse bool, numPartitions int) RDD {
    if numPartitions <= 0 {
        switch {
        case env.parallel == 0:
            numPartitions = r.len()
        default:
            numPartitions = env.parallel
        }
    }
    sortMapper := func(list []interface{}) []interface{} {
        sorter := NewParkSorter(list, func(x, y interface{}) bool {
            return fn(x.(*KeyValue).Key, y.(*KeyValue).Key)
        })
        if !reverse {
            sort.Sort(sorter)
        } else {
            sort.Sort(sort.Reverse(sorter))
        }
        return list
    }
    goSortMapper := func(iter Yielder) Yielder {
        yield := make(chan interface{}, 1)
        go func() {
            values := make([]interface{}, 0)
            for value := range iter {
                values = append(values, value)
            }
            sorted := sortMapper(values)
            for _, value := range sorted {
                yield <- value
            }
            close(yield)
        }()
        return yield
    }

    if r.len() == 1 {
        return r.MapPartition(goSortMapper)
    }
    // we choose some sample records as the key to partition results
    n := numPartitions * 10 / r.len()
    samples := r.MapPartition(func(iter Yielder) Yielder {
        yield := make(chan interface{}, 1)
        go func() {
            index := 0
            for value := range iter {
                yield <- value
                index++
                if index >= n {
                    break
                }
            }
            close(yield)
        }()
        return yield
    }).Collect()
    samples = sortMapper(samples)
    keys := make([]interface{}, 0)
    for i := 0; i < numPartitions-1; i++ {
        if i*10+5 >= len(samples) {
            break
        }
        keys = append(keys, samples[i*10+5].(*KeyValue).Key)
    }
    partitioner := newRangePartitioner(fn, keys, reverse)
    aggregator := newMergeAggregator()
    shuffleRdd := newShuffledRDD(r.prototype, aggregator, partitioner)
    return shuffleRdd.FlatMap(func(x interface{}) []interface{} {
        keyValue := x.(*KeyValue)
        values := keyValue.Value.([]interface{})
        results := make([]interface{}, len(values))
        for i := range values {
            results[i] = &KeyValue{
                Key:   keyValue.Key,
                Value: values[i],
            }
        }
        return results
    }).MapPartition(goSortMapper)
}

func (r *_BaseRDD) Cartesian(other RDD) RDD {
    return newCartesianRDD(r.ctx, r.prototype, other)
}

func (r *_BaseRDD) Distinct() RDD {
    return r.Distinct_N(0)
}

func (r *_BaseRDD) Distinct_N(numPartitions int) RDD {
    d := r.Map(func(arg interface{}) interface{} {
        return &KeyValue{arg, nil}
    }).ReduceByKey_N(func(x, y interface{}) interface{} {
        return nil
    }, numPartitions).Map(func(arg interface{}) interface{} {
        return arg.(*KeyValue).Key
    })
    return d
}

func (r *_BaseRDD) ReduceByKey(fn ReducerFunc) RDD {
    return r.ReduceByKey_N(fn, 0)
}

func (r *_BaseRDD) ReduceByKey_N(fn ReducerFunc, numPartitions int) RDD {
    combinerCreator := func(x interface{}) []interface{} {
        y := []interface{}{x}[:]
        return y
    }
    combinderMerger := func(x, y []interface{}) []interface{} {
        x[0] = fn(x[0], y[0])
        return x
    }
    valueMerger := func(x []interface{}, y interface{}) []interface{} {
        x[0] = fn(x[0], y)
        return x
    }
    aggregator := &_Aggregator{combinerCreator, combinderMerger, valueMerger}
    return r.combineByKey(aggregator, numPartitions).Map(func(x interface{}) interface{} {
        keyValue := x.(*KeyValue)
        return &KeyValue{keyValue.Key, keyValue.Value.([]interface{})[0]}
    })
}

func (r *_BaseRDD) PartitionByKey() RDD {
    return r.PartitionByKey_N(0)
}

func (r *_BaseRDD) PartitionByKey_N(numPartitions int) RDD {
    return r.GroupByKey_N(numPartitions).Map(func(arg interface{}) interface{} {
        keyValue := arg.(*KeyValue)
        values := keyValue.Value.([]interface{})
        results := make([]interface{}, len(values))
        for i := range values {
            results[i] = &KeyValue{keyValue.Key, values[i]}
        }
        return results
    }).FlatMap(func(arg interface{}) []interface{} {
        return arg.([]interface{})
    })
}

func (r *_BaseRDD) GroupByKey() RDD {
    return r.GroupByKey_N(0)
}

func (r *_BaseRDD) GroupByKey_N(numPartitions int) RDD {
    aggregator := newMergeAggregator()
    return r.combineByKey(aggregator, numPartitions)
}

func (r *_BaseRDD) Map(f MapperFunc) RDD {
    return newMappedRDD(r.prototype, f)
}

func (r *_BaseRDD) MapPartition(f PartitionMapperFunc) RDD {
    return newPartitionMappedRDD(r.prototype, f)
}

func (r *_BaseRDD) FlatMap(f FlatMapperFunc) RDD {
    return newFlatMappedRDD(r.prototype, f)
}

func (r *_BaseRDD) Filter(f FilterFunc) RDD {
    return newFilteredRDD(r.prototype, f)
}

func (r *_BaseRDD) Sample(fraction float32, seed int64) RDD {
    return newSampledRDD(r.prototype, fraction, seed)
}

func (r *_BaseRDD) CountByKey() map[interface{}]int64 {
    return r.Map(func(arg interface{}) interface{} {
        return arg.(*KeyValue).Key
    }).CountByValue()
}

func (r *_BaseRDD) CountByValue() map[interface{}]int64 {
    log.Printf("<CountByValue> %s", r.prototype)
    iters := r.ctx.runRoutine(r.prototype, nil, func(yield Yielder, partition int) interface{} {
        cnts := make(map[interface{}]int64)
        for value := range yield {
            if _, ok := cnts[value]; ok {
                cnts[value]++
            } else {
                cnts[value] = 1
            }
        }
        return cnts
    })
    cnts := make(map[interface{}]int64)
    for _, iter := range iters {
        for value := range iter {
            for key, cnt := range value.(map[interface{}]int64) {
                if _, ok := cnts[key]; ok {
                    cnts[key] += cnt
                } else {
                    cnts[key] = cnt
                }
            }
        }
    }
    return cnts
}

func (r *_BaseRDD) Reduce(fn ReducerFunc) interface{} {
    log.Printf("<Reduce> %s", r.prototype)
    iters := r.ctx.runRoutine(r.prototype, nil, func(yield Yielder, partition int) interface{} {
        var accu interface{} = nil
        for value := range yield {
            switch {
            case accu == nil:
                accu = value
            default:
                accu = fn(accu, value)
            }
        }
        return accu
    })
    var accu interface{} = nil
    for _, iter := range iters {
        for value := range iter {
            if value != nil {
                switch {
                case accu == nil:
                    accu = value
                default:
                    accu = fn(accu, value)
                }
            }
        }
    }
    return accu
}

func (r *_BaseRDD) SaveAsTextFile(pathname string) {
    newOutputTextFileRDD(r.prototype, pathname).Collect()
}

func (r *_BaseRDD) Collect() []interface{} {
    log.Printf("<Collect> %s", r.prototype)
    iters := r.ctx.runRoutine(r.prototype, nil, func(yield Yielder, partition int) interface{} {
        subCollections := make([]interface{}, 0)
        for value := range yield {
            subCollections = append(subCollections, value)
        }
        return subCollections
    })
    collections := make([]interface{}, 0)
    for _, iter := range iters {
        subCollections := (<-iter).([]interface{})
        collections = append(collections, subCollections...)
    }
    return collections
}

func (r *_BaseRDD) CollectAsMap() map[interface{}]interface{} {
    log.Printf("<CollectAsMap> %s", r.prototype)
    collections := r.Collect()
    sets := make(map[interface{}]interface{})
    for _, item := range collections {
        keyValue := item.(*KeyValue)
        sets[keyValue.Key] = keyValue.Value
    }
    return sets
}

func (r *_BaseRDD) Foreach(fn LoopFunc) {
    log.Printf("<Foreach> %s", r.prototype)
    dumps := r.ctx.runRoutine(r.prototype, nil, func(yield Yielder, partition int) interface{} {
        for value := range yield {
            fn(value)
        }
        return struct{}{}
    })
    // we need to dump all the channels otherwise the function will not be executed.
    for _, dump := range dumps {
        for _ = range dump {
        }
    }
}

func (r *_BaseRDD) Count() int64 {
    log.Printf("<Count> %s", r.prototype)
    var cnt int64 = 0
    iters := r.ctx.runRoutine(r.prototype, nil, func(yield Yielder, partition int) interface{} {
        var total int64 = 0
        for _ = range yield {
            total++
        }
        return total
    })
    for _, iter := range iters {
        for subCount := range iter {
            cnt += subCount.(int64)
        }
    }
    return cnt
}

func (r *_BaseRDD) Take(n int64) []interface{} {
    if n < 0 {
        return nil
    }
    log.Printf("<Take>=%d %s", n, r.prototype)
    results := make([]interface{}, n)
    var index int64 = 0
    p := make([]int, 1)
    p[0] = 0
    for index < n && p[0] < r.prototype.len() {
        if y := r.ctx.runRoutine(r.prototype, p, func(yield Yielder, partition int) interface{} {
            s := make([]interface{}, n-index)
            var i int64 = 0
            for ; i < n-index; i++ {
                if value, ok := <-yield; ok {
                    s[i] = value
                } else {
                    break
                }
            }
            return s[:i]
        }); len(y) > 0 {
            for taked := range y[0] {
                takedSlice := taked.([]interface{})
                copy(results[index:], takedSlice)
                index += int64(len(takedSlice))
            }
        }
        p[0]++
    }
    return results
}

func (r *_BaseRDD) combineByKey(aggregator *_Aggregator, numPartitions int) RDD {
    if numPartitions <= 0 {
        switch {
        case env.parallel == 0:
            numPartitions = r.len()
        default:
            numPartitions = env.parallel
        }
    }
    patitioner := newHashPartitioner(numPartitions)
    return newShuffledRDD(r.prototype, aggregator, patitioner)
}

func (r *_BaseRDD) getOrCompute(split Split) Yielder {
    rdd := r.prototype
    i := split.getIndex()
    yield := make(chan interface{}, 1)
    go func() {
        if r.cache[i] != nil {
            log.Printf("Cache hit <%s> on Split[%d]", rdd, i)
            for _, value := range r.cache[i] {
                yield <- value
            }
        } else {
            r.cache[i] = make([]interface{}, 0)
            for value := range rdd.compute(split) {
                r.cache[i] = append(r.cache[i], value)
                yield <- value
            }
            r.cache[i] = r.cache[i][:]
        }
        close(yield)
    }()
    return yield
}

func (r *_BaseRDD) persistOrCompute(split Split) Yielder {
    rdd := r.prototype
    i := split.getIndex()
    yield := make(chan interface{}, 1)
    go func() {
        if len(r.persistLocation) != 0 {
            pathname := env.getLocalRDDPath(r.id, i)
            log.Printf("Decoding rdd-%d/%d[GOB] from local file %s", r.id, i, pathname)
            var values []interface{}
            bs, err := ioutil.ReadFile(pathname)
            if err != nil {
                panic(fmt.Errorf("Error when persist/decode rdd split[%d], %v", i, err))
            }
            buffer := bytes.NewBuffer(bs)
            decoder := gob.NewDecoder(buffer)
            if err = decoder.Decode(&values); err != nil {
                panic(fmt.Errorf("Error when persist/decode rdd split[%d], %v", i, err))
            }
            for _, value := range values {
                yield <- value
            }
        } else {
            values := make([]interface{}, 0)
            for value := range rdd.compute(split) {
                values = append(values, value)
                yield <- value
            }
            pathname := env.getLocalRDDPath(r.id, i)
            buffer := new(bytes.Buffer)
            encoder := gob.NewEncoder(buffer)
            encoder.Encode(values[:])
            if err := ioutil.WriteFile(pathname, buffer.Bytes(), 0644); err != nil {
                panic(fmt.Errorf("Error when persist/encode rdd split[%d], %v", i, err))
            }
            log.Printf("Encoding rdd-%d/%d[GOB] into local file %s", r.id, i, pathname)
            r.persistLocation = pathname
        }
        close(yield)
    }()
    return yield
}

func (r *_BaseRDD) traverse(split Split) Yielder {
    rdd := r.prototype
    if r.shouldCache {
        return r.getOrCompute(split)
    }
    if r.shouldPersist {
        return r.persistOrCompute(split)
    }
    return rdd.compute(split)
}

func (r *_BaseRDD) getId() int64 {
    return r.id
}

func (r *_BaseRDD) getContext() *Context {
    return r.ctx
}

func (r *_BaseRDD) getSplits() []Split {
    return r.splits
}

func (r *_BaseRDD) getSplit(index int) Split {
    return r.splits[index]
}

func (r *_BaseRDD) len() int {
    return r.length
}

func (r *_BaseRDD) init(ctx *Context, prototype RDD) {
    r.ctx = ctx
    r.prototype = prototype
    r.id = r.newRddId()
    r.splits = make([]Split, 0)

    r.ctx.init()
}

var nextRddId AtomicInt = 0

func (r *_BaseRDD) newRddId() int64 {
    nextRddId.Add(1)
    return nextRddId.Get()
}

var nextShuffleId AtomicInt = 0

func (r *_BaseRDD) newShuffleId() int64 {
    nextShuffleId.Add(1)
    return nextShuffleId.Get()
}

var _ = fmt.Println
