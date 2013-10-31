package gopark

import (
    "bytes"
    "encoding/gob"
    "fmt"
    "github.com/mijia/ty"
    "io/ioutil"
    "log"
    "reflect"
    "sort"
)

type RDD interface {
    Map(fn interface{}) RDD
    MapPartition(fn interface{}) RDD
    FlatMap(fn interface{}) RDD
    Filter(fn interface{}) RDD
    Sample(fraction float32, seed int64) RDD
    GroupByKey() RDD
    PartitionByKey() RDD
    ReduceByKey(fn interface{}) RDD
    Distinct() RDD
    SortByKey(fn interface{}, reverse bool) RDD
    Join(other RDD) RDD
    LeftOuterJoin(other RDD) RDD
    RightOuterJoin(other RDD) RDD
    GroupWith(other RDD) RDD
    Union(other RDD) RDD
    Cartesian(other RDD) RDD

    GroupByKey_N(numPartitions int) RDD
    PartitionByKey_N(numPartitions int) RDD
    ReduceByKey_N(fn interface{}, numPartitions int) RDD
    Distinct_N(numPartitions int) RDD
    SortByKey_N(fn interface{}, reverse bool, numPartitions int) RDD
    Join_N(other RDD, numPartitions int) RDD
    LeftOuterJoin_N(other RDD, numPartitions int) RDD
    RightOuterJoin_N(other RDD, numPartitions int) RDD
    GroupWith_N(other RDD, numPartitions int) RDD

    Reduce(fn interface{}) interface{}
    CountByKey() interface{}
    CountByValue() interface{}
    Take(n int) interface{}
    Collect() interface{}
    CollectAsMap() interface{}
    Count() int
    Foreach(fn interface{})
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
    return r.GroupWith_N(other, numPartitions).FlatMap(func(x *KeyValue) []*KeyValue {
        groups := x.Value.([]interface{})
        vg1, vg2 := reflect.ValueOf(groups[0]), reflect.ValueOf(groups[1])
        if needLeft && (!vg1.IsValid() || vg1.Len() == 0) {
            return nil
        }
        if needRight && (!vg2.IsValid() || vg2.Len() == 0) {
            return nil
        }
        results := make([]*KeyValue, 0)
        if !vg1.IsValid() || vg1.Len() == 0 {
            vg1 = reflect.ValueOf([]interface{}{nil}[:])
        }
        if !vg2.IsValid() || vg2.Len() == 0 {
            vg2 = reflect.ValueOf([]interface{}{nil}[:])
        }
        for i := 0; i < vg1.Len(); i++ {
            for j := 0; j < vg2.Len(); j++ {
                results = append(results, &KeyValue{
                    Key:   x.Key,
                    Value: []interface{}{vg1.Index(i).Interface(), vg2.Index(j).Interface()}[:],
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

func (r *_BaseRDD) SortByKey(fn interface{}, reverse bool) RDD {
    return r.SortByKey_N(fn, reverse, 0)
}

func (r *_BaseRDD) SortByKey_N(fn interface{}, reverse bool, numPartitions int) RDD {
    if numPartitions <= 0 {
        switch {
        case env.parallel == 0:
            numPartitions = r.len()
        default:
            numPartitions = env.parallel
        }
    }
    sortMapper := func(list []*KeyValue) []*KeyValue {
        sorter := NewParkSorter(list, func(x, y *KeyValue) bool {
            xkey, ykey := x.Key, y.Key
            chk := ty.Check(new(func(func(ty.A, ty.A) bool, ty.A, ty.A) bool), fn, xkey, ykey)
            vfn, vxkey, vykey := chk.Args[0], chk.Args[1], chk.Args[2]
            return vfn.Call([]reflect.Value{vxkey, vykey})[0].Interface().(bool)
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
            values := make([]*KeyValue, 0)
            for value := range iter {
                values = append(values, value.(*KeyValue))
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
    }).Collect().([]*KeyValue)
    samples = sortMapper(samples)
    keys := make([]interface{}, 0)
    for i := 0; i < numPartitions-1; i++ {
        if i*10+5 >= len(samples) {
            break
        }
        keys = append(keys, samples[i*10+5].Key)
    }
    partitioner := newRangePartitioner(fn, keys, reverse)
    aggregator := newMergeAggregator()
    shuffleRdd := newShuffledRDD(r.prototype, aggregator, partitioner)
    return shuffleRdd.FlatMap(func(keyValue *KeyValue) []*KeyValue {
        values := reflect.ValueOf(keyValue.Value)
        results := make([]*KeyValue, values.Len())
        for i := 0; i < values.Len(); i++ {
            results[i] = &KeyValue{
                Key:   keyValue.Key,
                Value: values.Index(i).Interface(),
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
    d := r.Map(func(arg interface{}) *KeyValue {
        return &KeyValue{arg, 1}
    }).ReduceByKey_N(func(x, y interface{}) interface{} {
        return 1
    }, numPartitions).Map(func(arg *KeyValue) interface{} {
        return arg.Key
    })
    return d
}

func (r *_BaseRDD) ReduceByKey(fn interface{}) RDD {
    return r.ReduceByKey_N(fn, 0)
}

func (r *_BaseRDD) ReduceByKey_N(fn interface{}, numPartitions int) RDD {
    combinerCreator := func(x interface{}) interface{} {
        return x
    }
    combinderMerger := func(x, y interface{}) interface{} {
        chk := ty.Check(new(func(func(ty.A, ty.A) ty.A, ty.A, ty.A) ty.A), fn, x, y)
        vfn, vx, vy := chk.Args[0], chk.Args[1], chk.Args[2]
        return vfn.Call([]reflect.Value{vx, vy})[0].Interface()
    }
    valueMerger := func(x interface{}, y interface{}) interface{} {
        chk := ty.Check(new(func(func(ty.A, ty.A) ty.A, ty.A, ty.A) ty.A), fn, x, y)
        vfn, vx, vy := chk.Args[0], chk.Args[1], chk.Args[2]
        return vfn.Call([]reflect.Value{vx, vy})[0].Interface()
    }
    aggregator := &Aggregator{combinerCreator, combinderMerger, valueMerger}
    return r.combineByKey(aggregator, numPartitions)
}

func (r *_BaseRDD) PartitionByKey() RDD {
    return r.PartitionByKey_N(0)
}

func (r *_BaseRDD) PartitionByKey_N(numPartitions int) RDD {
    return r.GroupByKey_N(numPartitions).FlatMap(func(arg *KeyValue) []*KeyValue {
        vvals := reflect.ValueOf(arg.Value)
        if vvals.IsValid() && vvals.Kind() == reflect.Slice {
            results := make([]*KeyValue, vvals.Len())
            for i := 0; i < vvals.Len(); i++ {
                results[i] = &KeyValue{
                    Key:   arg.Key,
                    Value: vvals.Index(i).Interface(),
                }
            }
            return results
        }
        return []*KeyValue{}
    })
}

func (r *_BaseRDD) GroupByKey() RDD {
    return r.GroupByKey_N(0)
}

func (r *_BaseRDD) GroupByKey_N(numPartitions int) RDD {
    aggregator := newMergeAggregator()
    return r.combineByKey(aggregator, numPartitions)
}

func (r *_BaseRDD) Map(fn interface{}) RDD {
    return newMappedRDD(r.prototype, fn)
}

func (r *_BaseRDD) MapPartition(fn interface{}) RDD {
    return newPartitionMappedRDD(r.prototype, fn)
}

func (r *_BaseRDD) FlatMap(fn interface{}) RDD {
    return newFlatMappedRDD(r.prototype, fn)
}

func (r *_BaseRDD) Filter(fn interface{}) RDD {
    return newFilteredRDD(r.prototype, fn)
}

func (r *_BaseRDD) Sample(fraction float32, seed int64) RDD {
    return newSampledRDD(r.prototype, fraction, seed)
}

func (r *_BaseRDD) CountByKey() interface{} {
    return r.Map(func(arg *KeyValue) interface{} {
        return arg.Key
    }).CountByValue()
}

func (r *_BaseRDD) CountByValue() interface{} {
    parklog("<CountByValue> %s", r.prototype)
    var cnts reflect.Value
    iters := r.ctx.runRoutine(r.prototype, nil, func(yield Yielder, partition int) interface{} {
        var localCnts reflect.Value
        for value := range yield {
            vval := reflect.ValueOf(value)
            if !cnts.IsValid() {
                cnts = reflect.MakeMap(reflect.MapOf(vval.Type(), reflect.TypeOf(1)))
            }
            if !localCnts.IsValid() {
                localCnts = reflect.MakeMap(reflect.MapOf(vval.Type(), reflect.TypeOf(1)))
            }
            if cValue := localCnts.MapIndex(vval); cValue.IsValid() {
                localCnts.SetMapIndex(vval, reflect.ValueOf(cValue.Interface().(int)+1))
            } else {
                localCnts.SetMapIndex(vval, reflect.ValueOf(1))
            }
        }
        return localCnts
    })
    for _, iter := range iters {
        for value := range iter {
            vval := value.(reflect.Value)
            for _, key := range vval.MapKeys() {
                if cValue := cnts.MapIndex(key); cValue.IsValid() {
                    cnts.SetMapIndex(key, reflect.ValueOf(cValue.Interface().(int)+vval.MapIndex(key).Interface().(int)))
                } else {
                    cnts.SetMapIndex(key, vval.MapIndex(key))
                }
            }
        }
    }
    if cnts.IsValid() {
        return cnts.Interface()
    }
    return nil
}

func (r *_BaseRDD) Reduce(fn interface{}) interface{} {
    parklog("<Reduce> %s", r.prototype)
    iters := r.ctx.runRoutine(r.prototype, nil, func(yield Yielder, partition int) interface{} {
        var accu reflect.Value
        for value := range yield {
            switch {
            case !accu.IsValid():
                accu = reflect.ValueOf(value)
            default:
                chk := ty.Check(new(func(func(ty.A, ty.A) ty.A, ty.A, ty.A) ty.A), fn, accu.Interface(), value)
                vfn, vval := chk.Args[0], chk.Args[2]
                accu = vfn.Call([]reflect.Value{accu, vval})[0]
            }
        }
        return accu.Interface()
    })
    var accu reflect.Value
    for _, iter := range iters {
        for value := range iter {
            switch {
            case !accu.IsValid():
                accu = reflect.ValueOf(value)
            default:
                chk := ty.Check(new(func(func(ty.A, ty.A) ty.A, ty.A, ty.A) ty.A), fn, accu.Interface(), value)
                vfn, vval := chk.Args[0], chk.Args[2]
                accu = vfn.Call([]reflect.Value{accu, vval})[0]
            }
        }
    }
    return accu.Interface()
}

func (r *_BaseRDD) SaveAsTextFile(pathname string) {
    newOutputTextFileRDD(r.prototype, pathname).Collect()
}

func (r *_BaseRDD) Collect() interface{} {
    parklog("<Collect> %s", r.prototype)
    iters := r.ctx.runRoutine(r.prototype, nil, func(yield Yielder, partition int) interface{} {
        var subCollections reflect.Value
        for value := range yield {
            if !subCollections.IsValid() {
                tv := reflect.TypeOf(value)
                subCollections = reflect.MakeSlice(reflect.SliceOf(tv), 0, 0)
            }
            subCollections = reflect.Append(subCollections, reflect.ValueOf(value))
        }
        return subCollections
    })
    var collections reflect.Value
    for _, iter := range iters {
        subCollections := (<-iter).(reflect.Value)
        if subCollections.IsValid() {
            if !collections.IsValid() {
                collections = reflect.MakeSlice(subCollections.Type(), 0, 0)
            }
            collections = reflect.AppendSlice(collections, subCollections)
        }
    }
    if collections.IsValid() {
        return collections.Interface()
    }
    return nil
}

func (r *_BaseRDD) CollectAsMap() interface{} {
    parklog("<CollectAsMap> %s", r.prototype)
    collections := r.Collect().([]*KeyValue)
    var sets reflect.Value
    for _, item := range collections {
        vkey := reflect.ValueOf(item.Key)
        vval := reflect.ValueOf(item.Value)
        if !sets.IsValid() {
            sets = reflect.MakeMap(reflect.MapOf(vkey.Type(), vval.Type()))
        }
        sets.SetMapIndex(vkey, vval)
    }
    if sets.IsValid() {
        return sets.Interface()
    }
    return nil
}

func (r *_BaseRDD) Foreach(fn interface{}) {
    parklog("<Foreach> %s", r.prototype)
    dumps := r.ctx.runRoutine(r.prototype, nil, func(yield Yielder, partition int) interface{} {
        for value := range yield {
            chk := ty.Check(new(func(func(ty.A), ty.A)), fn, value)
            vfn, vvalue := chk.Args[0], chk.Args[1]
            vfn.Call([]reflect.Value{vvalue})
        }
        return struct{}{}
    })
    // we need to dump all the channels otherwise the function will not be executed.
    for _, dump := range dumps {
        for _ = range dump {
        }
    }
}

func (r *_BaseRDD) Count() int {
    parklog("<Count> %s", r.prototype)
    cnt := 0
    iters := r.ctx.runRoutine(r.prototype, nil, func(yield Yielder, partition int) interface{} {
        total := 0
        for _ = range yield {
            total++
        }
        return total
    })
    for _, iter := range iters {
        for subCount := range iter {
            cnt += subCount.(int)
        }
    }
    return cnt
}

func (r *_BaseRDD) Take(n int) interface{} {
    if n < 0 {
        return nil
    }
    parklog("<Take>=%d %s", n, r.prototype)
    var results reflect.Value
    index := 0
    p := make([]int, 1)
    p[0] = 0
    for index < n && p[0] < r.prototype.len() {
        if y := r.ctx.runRoutine(r.prototype, p, func(yield Yielder, partition int) interface{} {
            var i int = 0
            for ; i < n-index; i++ {
                if value, ok := <-yield; ok {
                    tv := reflect.TypeOf(value)
                    if !results.IsValid() {
                        results = reflect.MakeSlice(reflect.SliceOf(tv), n, n)
                    }
                    results.Index(i + index).Set(reflect.ValueOf(value))
                } else {
                    break
                }
            }
            return i
        }); len(y) > 0 {
            for taked := range y[0] {
                takedLength := taked.(int)
                index += takedLength
            }
        }
        p[0]++
    }
    if results.IsValid() {
        return results.Slice(0, index).Interface()
    }
    return nil
}

func (r *_BaseRDD) combineByKey(aggregator *Aggregator, numPartitions int) RDD {
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
            parklog("Cache hit <%s> on Split[%d]", rdd, i)
            for _, value := range r.cache[i] {
                yield <- value
            }
        } else {
            r.cache[i] = make([]interface{}, 0)
            for value := range rdd.compute(split) {
                r.cache[i] = append(r.cache[i], value)
                yield <- value
            }
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
