package gopark

import (
    "bufio"
    "bytes"
    "encoding/gob"
    "fmt"
    "github.com/mijia/ty"
    "io"
    "io/ioutil"
    "log"
    "math/rand"
    "os"
    "path/filepath"
    "reflect"
    "sync"
)

type _DerivedRDD struct {
    _BaseRDD
    previous RDD
}

func (d *_DerivedRDD) init(prevRdd, prototype RDD) {
    d._BaseRDD.init(prevRdd.getContext(), prototype)
    d.previous = prevRdd
    d.length = prevRdd.len()
    d.splits = prevRdd.getSplits()
}

type _MappedRDD struct {
    _DerivedRDD
    fn  interface{}
}

func (m *_MappedRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", m, split.getIndex())
        for value := range m.previous.traverse(split) {
            chk := ty.Check(new(func(func(ty.A) ty.B, ty.A) ty.B), m.fn, value)
            vfn, vval := chk.Args[0], chk.Args[1]
            y := vfn.Call([]reflect.Value{vval})[0]
            yield <- y.Interface()
        }
        close(yield)
    }()
    return yield
}

func (m *_MappedRDD) init(rdd RDD, fn interface{}) {
    m._DerivedRDD.init(rdd, m)
    m.fn = fn
}

func (m *_MappedRDD) String() string {
    return fmt.Sprintf("MappedRDD-%d <%s>", m.id, m._DerivedRDD.previous)
}

func newMappedRDD(rdd RDD, fn interface{}) RDD {
    mRdd := &_MappedRDD{}
    mRdd.init(rdd, fn)
    return mRdd
}

type _PartitionMappedRDD struct {
    _DerivedRDD
    fn  interface{}
}

func (r *_PartitionMappedRDD) compute(split Split) Yielder {
    parklog("Computing <%s> on Split[%d]", r, split.getIndex())
    yielder := r.previous.traverse(split)
    chk := ty.Check(new(func(func(ty.A) ty.A, ty.A) ty.A), r.fn, yielder)
    vfn, vval := chk.Args[0], chk.Args[1]
    return vfn.Call([]reflect.Value{vval})[0].Interface().(Yielder)
}

func (r *_PartitionMappedRDD) init(rdd RDD, fn interface{}) {
    r._DerivedRDD.init(rdd, r)
    r.fn = fn
}

func (r *_PartitionMappedRDD) String() string {
    return fmt.Sprintf("PartitionMappedRDD-%d <%s>", r.id, r._DerivedRDD.previous)
}

func newPartitionMappedRDD(rdd RDD, fn interface{}) RDD {
    r := &_PartitionMappedRDD{}
    r.init(rdd, fn)
    return r
}

type _FlatMappedRDD struct {
    _DerivedRDD
    fn  interface{}
}

func (r *_FlatMappedRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        for arg := range r.previous.traverse(split) {
            chk := ty.Check(new(func(func(ty.A) ty.B, ty.A) ty.B), r.fn, arg)
            vfn, varg := chk.Args[0], chk.Args[1]
            y := vfn.Call([]reflect.Value{varg})[0]
            for i := 0; i < y.Len(); i++ {
                yield <- y.Index(i).Interface()
            }
        }
        close(yield)
    }()
    return yield
}

func (r *_FlatMappedRDD) init(rdd RDD, fn interface{}) {
    r._DerivedRDD.init(rdd, r)
    r.fn = fn
}

func (r *_FlatMappedRDD) String() string {
    return fmt.Sprintf("FlatMappedRDD-%d <%s>", r.id, r._DerivedRDD.previous)
}

func newFlatMappedRDD(rdd RDD, fn interface{}) RDD {
    r := &_FlatMappedRDD{}
    r.init(rdd, fn)
    return r
}

type _FilteredRDD struct {
    _DerivedRDD
    fn  interface{}
}

func (r *_FilteredRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        for value := range r.previous.traverse(split) {
            chk := ty.Check(new(func(func(ty.A) bool, ty.A) bool), r.fn, value)
            vfn, vval := chk.Args[0], chk.Args[1]
            ok := vfn.Call([]reflect.Value{vval})[0].Interface().(bool)
            if ok {
                yield <- value
            }
        }
        close(yield)
    }()
    return yield
}

func (r *_FilteredRDD) init(rdd RDD, fn interface{}) {
    r._DerivedRDD.init(rdd, r)
    r.fn = fn
}

func (r *_FilteredRDD) String() string {
    return fmt.Sprintf("FilteredRDD-%d <%s>", r.id, r._DerivedRDD.previous)
}

func newFilteredRDD(rdd RDD, fn interface{}) RDD {
    r := &_FilteredRDD{}
    r.init(rdd, fn)
    return r
}

type _SampledRDD struct {
    _DerivedRDD
    fraction float32
    seed     int64
}

func (r *_SampledRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        rd := rand.New(rand.NewSource(r.seed))
        for value := range r.previous.traverse(split) {
            if rd.Float32() <= r.fraction {
                yield <- value
            }
        }
        close(yield)
    }()
    return yield
}

func (r *_SampledRDD) init(rdd RDD, fraction float32, seed int64) {
    r._DerivedRDD.init(rdd, r)
    r.fraction = fraction
    r.seed = seed
}

func (r *_SampledRDD) String() string {
    return fmt.Sprintf("SampledRDD-%d <%s>", r.id, r._DerivedRDD.previous)
}

func newSampledRDD(rdd RDD, fraction float32, seed int64) RDD {
    r := &_SampledRDD{}
    r.init(rdd, fraction, seed)
    return r
}

type _PartialSplit struct {
    index int
    begin int64
    end   int64
}

func (s *_PartialSplit) getIndex() int {
    return s.index
}

type _TextFileRDD struct {
    _BaseRDD
    path      string
    size      int64
    splitSize int64
}

const DEFAULT_FILE_SPLIT_SIZE = 64 * 1024 * 1024 // 64MB Split Size

func (t *_TextFileRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 100)
    go func() {
        parklog("Computing <%s> on Split[%d]", t, split.getIndex())
        defer close(yield)

        f, err := os.Open(t.path)
        if err != nil {
            panic(err)
        }
        defer f.Close()

        pSplit := split.(*_PartialSplit)
        start := pSplit.begin
        end := pSplit.end
        if start > 0 {
            start, err = seekToNewLine(f, start-1)
            if err == io.EOF {
                return
            }
            if err != nil {
                panic(err)
            }
        }

        if start > end {
            return
        }
        r := bufio.NewReader(f)
        line, err := readLine(r)
        for err == nil {
            yield <- line
            start += int64(len(line)) + 1 // here would be dargon
            if start > end {
                break
            }
            line, err = readLine(r)
        }
        if err != nil && err != io.EOF {
            panic(err)
        }
    }()
    return yield
}

func (t *_TextFileRDD) init(ctx *Context, path string, numSplits int) {
    t._BaseRDD.init(ctx, t)
    t.path = path

    fi, err := os.Stat(path)
    if err != nil {
        panic(err)
    }
    t.size = fi.Size()
    t.splitSize = DEFAULT_FILE_SPLIT_SIZE
    if numSplits > 0 {
        t.splitSize = t.size / int64(numSplits)
    }
    t.length = int(t.size / t.splitSize)
    t.splits = make([]Split, t.length)
    for i := 0; i < t.length; i++ {
        end := int64(i+1) * t.splitSize
        if i == t.length-1 {
            end = t.size
        }
        t.splits[i] = &_PartialSplit{
            index: i,
            begin: int64(i) * t.splitSize,
            end:   end,
        }
    }
}

func (t *_TextFileRDD) String() string {
    return fmt.Sprintf("TextFileRDD-%d <%s %d>", t.id, t.path, t.len())
}

func newTextFileRDD(ctx *Context, path string) RDD {
    textRdd := &_TextFileRDD{}
    textRdd.init(ctx, path, env.parallel)
    return textRdd
}

type _ShuffledSplit struct {
    index int
}

func (s *_ShuffledSplit) getIndex() int {
    return s.index
}

type _ShuffledRDD struct {
    _BaseRDD
    shuffleId   int64
    parent      RDD
    aggregator  *Aggregator
    partitioner Partitioner
    numParts    int
    shuffleJob  sync.Once
}

type _ShuffleBucket map[interface{}]interface{}

func (r *_ShuffledRDD) compute(split Split) Yielder {
    parklog("Computing <%s> on Split[%d]", r, split.getIndex())
    r.shuffleJob.Do(func() {
        r.runShuffleJob()
    })

    numParentSplit := r.parent.len()
    combined := make(_ShuffleBucket)
    outputId := split.getIndex()
    bucketChan := make([]chan _ShuffleBucket, numParentSplit)
    for i := 0; i < numParentSplit; i++ {
        bucketChan[i] = make(chan _ShuffleBucket)
        go func(inputId int, bchan chan _ShuffleBucket) {
            pathName := env.getLocalShufflePath(r.shuffleId, inputId, outputId)
            parklog("Decoding shuffle-%d[GOB] from local file %s", r.shuffleId, pathName)
            var bucket _ShuffleBucket
            bs, err := ioutil.ReadFile(pathName)
            if err != nil {
                panic(err)
            }
            buffer := bytes.NewBuffer(bs)
            decoder := gob.NewDecoder(buffer)
            if err = decoder.Decode(&bucket); err != nil {
                panic(err)
            }
            bchan <- bucket
            close(bchan)
        }(i, bucketChan[i])
    }
    for _, bchan := range bucketChan {
        for bucket := range bchan {
            for key, value := range bucket {
                if collection, ok := combined[key]; ok {
                    combined[key] = r.aggregator.combinerMerger(collection, value)
                } else {
                    combined[key] = value
                }
            }
        }
    }

    yield := make(chan interface{}, 1)
    go func() {
        for key, value := range combined {
            yield <- &KeyValue{key, value}
        }
        close(yield)
    }()
    return yield
}

func (r *_ShuffledRDD) runShuffleJob() {
    parklog("Computing shuffle stage for <%s>", r)
    iters := r.ctx.runRoutine(r.parent, nil, func(yield Yielder, partition int) interface{} {
        numSplits := r.partitioner.numPartitions()
        buckets := make([]_ShuffleBucket, numSplits)
        for i := 0; i < numSplits; i++ {
            buckets[i] = make(_ShuffleBucket)
        }
        for value := range yield {
            keyValue := value.(*KeyValue)
            bucketId := r.partitioner.getPartition(keyValue.Key)
            bucket := buckets[bucketId]
            if collection, ok := bucket[keyValue.Key]; ok {
                bucket[keyValue.Key] = r.aggregator.valueMerger(collection, keyValue.Value)
            } else {
                bucket[keyValue.Key] = r.aggregator.combinerCreator(keyValue.Value)
            }
        }
        for i := 0; i < numSplits; i++ {
            pathName := env.getLocalShufflePath(r.shuffleId, partition, i)
            bucket := buckets[i]
            buffer := new(bytes.Buffer)
            encoder := gob.NewEncoder(buffer)
            encoder.Encode(bucket)
            if err := ioutil.WriteFile(pathName, buffer.Bytes(), 0644); err != nil {
                panic(err)
            }
            parklog("Encoding shuffle-%d[GOB] into local file %s", r.shuffleId, pathName)
        }
        return struct{}{}
    })
    for _, iter := range iters {
        for _ = range iter {
            // we need to dump the yielders that returns to finish up the routine
        }
    }
    parklog("Shuffling DONE for <%s>", r)
}

func (r *_ShuffledRDD) init(rdd RDD, aggregator *Aggregator, partitioner Partitioner) {
    r._BaseRDD.init(rdd.getContext(), r)
    r.shuffleId = r._BaseRDD.newShuffleId()
    r.parent = rdd
    r.numParts = rdd.len()
    r.aggregator = aggregator
    r.partitioner = partitioner
    r.length = partitioner.numPartitions()

    r.splits = make([]Split, r.length)
    for i := 0; i < r.length; i++ {
        r.splits[i] = &_ShuffledSplit{i}
    }
}

func (r *_ShuffledRDD) String() string {
    return fmt.Sprintf("ShuffledRDD-%d <%s %d>", r.id, r.parent, r.length)
}

func newShuffledRDD(rdd RDD, aggregator *Aggregator, partitioner Partitioner) RDD {
    r := &_ShuffledRDD{}
    r.init(rdd, aggregator, partitioner)
    return r
}

type _OutputTextFileRDD struct {
    _DerivedRDD
    pathname string
}

func (r *_OutputTextFileRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Saving <%s> on Split[%d]", r, split.getIndex())
        pathName := filepath.Join(r.pathname, fmt.Sprintf("%05d", split.getIndex()))
        outputFile, err := os.Create(pathName)
        if err != nil {
            panic(err)
        }
        defer outputFile.Close()

        write := func(v string) {
            if _, err := outputFile.Write([]byte(v)); err != nil {
                panic(err)
            }
        }
        for value := range r.previous.traverse(split) {
            sValue := fmt.Sprintln(value)
            write(sValue)
        }
        yield <- pathName
        close(yield)
    }()
    return yield
}

func (r *_OutputTextFileRDD) init(rdd RDD, pathname string) {
    absPathname, err := filepath.Abs(pathname)
    if err != nil {
        panic(err)
    }
    if fStat, err := os.Stat(absPathname); os.IsNotExist(err) {
        os.Mkdir(absPathname, os.ModePerm)
    } else {
        if !fStat.IsDir() {
            panic(fmt.Errorf("%s must be a directory in file system.", pathname))
        }
        // delete all the files under the directory
        err2 := filepath.Walk(absPathname, func(path string, info os.FileInfo, err error) error {
            if !info.IsDir() {
                if e := os.Remove(path); e != nil {
                    return e
                }
            }
            return nil
        })
        if err2 != nil {
            panic(err2)
        }
    }

    r._DerivedRDD.init(rdd, r)
    r.pathname = absPathname
}

func (r *_OutputTextFileRDD) String() string {
    return fmt.Sprintf("OutputTextFileRDD-%d <%s>", r.id, r.pathname)
}

func newOutputTextFileRDD(rdd RDD, pathname string) RDD {
    r := &_OutputTextFileRDD{}
    r.init(rdd, pathname)
    return r
}

type _DataSplit struct {
    index int
    begin int
    end   int
}

func (s *_DataSplit) getIndex() int {
    return s.index
}

type _DataRDD struct {
    _BaseRDD
    size int
    data reflect.Value
}

func (r *_DataRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        dSplit := split.(*_DataSplit)
        for i := dSplit.begin; i < dSplit.end; i++ {
            yield <- r.data.Index(i).Interface()
        }
        close(yield)
    }()
    return yield
}

func (r *_DataRDD) init(ctx *Context, data interface{}, numPartitions int) {
    switch reflect.TypeOf(data).Kind() {
    case reflect.Slice:
        dataValue := reflect.ValueOf(data)

        r._BaseRDD.init(ctx, r)
        r.data = dataValue
        r.size = dataValue.Len()
        if r.size <= 0 {
            log.Panic("Please don't provide an empty data array.")
        }
        if numPartitions <= 0 {
            numPartitions = 1
        }
        if r.size < numPartitions {
            numPartitions = r.size
        }
        splitSize := r.size / numPartitions
        r.length = numPartitions
        r.splits = make([]Split, numPartitions)
        for i := 0; i < numPartitions; i++ {
            end := splitSize*i + splitSize
            if i == numPartitions-1 {
                end = r.size
            }
            r.splits[i] = &_DataSplit{
                index: i,
                begin: splitSize * i,
                end:   end,
            }
        }
    default:
        log.Panicf("Please provide a slice of data instead of a %s", reflect.TypeOf(data))
    }
}

func (r *_DataRDD) String() string {
    return fmt.Sprintf("DataRDD-%d <size=%d>", r.id, r.size)
}

func newDataRDD(ctx *Context, data interface{}) RDD {
    return newDataRDD_N(ctx, data, env.parallel)
}

func newDataRDD_N(ctx *Context, data interface{}, numPartitions int) RDD {
    r := &_DataRDD{}
    r.init(ctx, data, numPartitions)
    return r
}

type _UnionSplit struct {
    index int
    rdd   RDD
    split Split
}

func (s *_UnionSplit) getIndex() int {
    return s.index
}

type _UnionRDD struct {
    _BaseRDD
    size int
    rdds []RDD
}

func (r *_UnionRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        unionSplit := split.(*_UnionSplit)
        for value := range unionSplit.rdd.traverse(unionSplit.split) {
            yield <- value
        }
        close(yield)
    }()
    return yield
}

func (r *_UnionRDD) init(ctx *Context, rdds []RDD) {
    r._BaseRDD.init(ctx, r)
    r.size = len(rdds)
    r.rdds = rdds
    for _, rdd := range rdds {
        r.length += rdd.len()
    }
    r.splits = make([]Split, r.length)
    index := 0
    for _, rdd := range rdds {
        for _, split := range rdd.getSplits() {
            r.splits[index] = &_UnionSplit{
                index: index,
                rdd:   rdd,
                split: split,
            }
            index++
        }
    }
}

func (r *_UnionRDD) String() string {
    return fmt.Sprintf("UnionRDD-%d <%d %s ...>", r.id, r.size, r.rdds[0])
}

func newUnionRDD(ctx *Context, rdds []RDD) RDD {
    r := &_UnionRDD{}
    r.init(ctx, rdds)
    return r
}

type _CoGroupedRDD struct {
    _BaseRDD
    size int
    rdds []RDD
}

func (r *_CoGroupedRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        var keyGroups reflect.Value
        for groupIndex, rdd := range r.rdds {
            for value := range rdd.traverse(split) {
                keyValue := value.(*KeyValue)
                vkey, vval := reflect.ValueOf(keyValue.Key), reflect.ValueOf(keyValue.Value)
                if !keyGroups.IsValid() {
                    empty := make([]interface{}, 0)
                    keyGroups = reflect.MakeMap(reflect.MapOf(vkey.Type(), reflect.TypeOf(empty)))
                }
                if containValue := keyGroups.MapIndex(vkey); containValue.IsValid() {
                    containValue.Index(groupIndex).Set(vval)
                    keyGroups.SetMapIndex(vkey, containValue)
                } else {
                    groups := make([]interface{}, r.size)
                    groups[groupIndex] = keyValue.Value
                    keyGroups.SetMapIndex(vkey, reflect.ValueOf(groups))
                }
            }
        }
        if keyGroups.IsValid() {
            for _, key := range keyGroups.MapKeys() {
                yield <- &KeyValue{
                    Key:   key.Interface(),
                    Value: keyGroups.MapIndex(key).Interface(),
                }
            }
        }
        close(yield)
    }()
    return yield
}

func (r *_CoGroupedRDD) init(ctx *Context, rdds []RDD, numPartitions int) {
    r._BaseRDD.init(ctx, r)
    r.size = len(rdds)
    r.rdds = make([]RDD, len(rdds))
    for i, rdd := range rdds {
        r.rdds[i] = rdd.GroupByKey_N(numPartitions)
    }
    r.length = numPartitions
    r.splits = make([]Split, r.length)
    for i := 0; i < numPartitions; i++ {
        r.splits[i] = &_ShuffledSplit{i}
    }
}

func (r *_CoGroupedRDD) String() string {
    return fmt.Sprintf("CoGroupedRDD-%d <%d %s %s...>", r.id, r.size, r.rdds[0], r.rdds[1])
}

func newCoGroupedRDD(ctx *Context, rdds []RDD, numPartitions int) RDD {
    r := &_CoGroupedRDD{}
    r.init(ctx, rdds, numPartitions)
    return r
}

type _CartesianSplit struct {
    index  int
    split1 Split
    split2 Split
}

func (s *_CartesianSplit) getIndex() int {
    return s.index
}

type _CartesianRDD struct {
    _BaseRDD
    rdd1 RDD
    rdd2 RDD
}

func (r *_CartesianRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    getContainer := func(i, j interface{}) interface{} {
        vi, vj := reflect.ValueOf(i), reflect.ValueOf(j)
        var container reflect.Value
        if vi.Type() != vj.Type() {
            empty := make([]interface{}, 0)
            container = reflect.MakeSlice(reflect.TypeOf(empty), 2, 2)
        } else {
            container = reflect.MakeSlice(reflect.SliceOf(vi.Type()), 2, 2)
        }
        container.Index(0).Set(vi)
        container.Index(1).Set(vj)
        return container.Interface()
    }
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        cSplit := split.(*_CartesianSplit)
        rightYields := make([]interface{}, 0)
        for i := range r.rdd1.traverse(cSplit.split1) {
            if len(rightYields) == 0 {
                for j := range r.rdd2.traverse(cSplit.split2) {
                    yield <- getContainer(i, j)
                    rightYields = append(rightYields, j)
                }
            } else {
                for _, j := range rightYields {
                    yield <- getContainer(i, j)
                }
            }
        }
        close(yield)
    }()
    return yield
}

func (r *_CartesianRDD) init(ctx *Context, rdd1, rdd2 RDD) {
    r._BaseRDD.init(ctx, r)
    r.rdd1 = rdd1
    r.rdd2 = rdd2
    r.length = rdd1.len() * rdd2.len()
    r.splits = make([]Split, r.length)
    n := rdd2.len()
    for i := 0; i < rdd1.len(); i++ {
        for j := 0; j < rdd2.len(); j++ {
            r.splits[i*n+j] = &_CartesianSplit{
                index:  i*n + j,
                split1: rdd1.getSplit(i),
                split2: rdd2.getSplit(j),
            }
        }
    }
}

func (r *_CartesianRDD) String() string {
    return fmt.Sprintf("CartesianRDD-%d <%s %s>", r.id, r.rdd1, r.rdd2)
}

func newCartesianRDD(ctx *Context, rdd1, rdd2 RDD) RDD {
    r := &_CartesianRDD{}
    r.init(ctx, rdd1, rdd2)
    return r
}
