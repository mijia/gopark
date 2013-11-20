package gopark

import (
    "bufio"
    "bytes"
    "encoding/gob"
    "fmt"
    "io"
    "io/ioutil"
    "log"
    "math"
    "math/rand"
    "os"
    "path/filepath"
    "sync"
)

//////////////////////////////////////////////////////
// Derived RDD operations implementation
//////////////////////////////////////////////////////
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

//////////////////////////////////////////////////////
// Mapped RDD Impl
//////////////////////////////////////////////////////
type _MappedRDD struct {
    _DerivedRDD
    fn  MapperFunc
}

func (m *_MappedRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", m, split.getIndex())
        for value := range m.previous.traverse(split) {
            yield <- m.fn(value)
        }
        close(yield)
    }()
    return yield
}

func (m *_MappedRDD) init(rdd RDD, f MapperFunc) {
    m._DerivedRDD.init(rdd, m)
    m.fn = f
}

func (m *_MappedRDD) String() string {
    return fmt.Sprintf("MappedRDD-%d <%s>", m.id, m._DerivedRDD.previous)
}

func newMappedRDD(rdd RDD, f MapperFunc) RDD {
    mRdd := &_MappedRDD{}
    mRdd.init(rdd, f)
    return mRdd
}

////////////////////////////////////////////////////////////////////////
// PartitionMappedRDD Impl
////////////////////////////////////////////////////////////////////////
type _PartitionMappedRDD struct {
    _DerivedRDD
    fn  PartitionMapperFunc
}

func (r *_PartitionMappedRDD) compute(split Split) Yielder {
    parklog("Computing <%s> on Split[%d]", r, split.getIndex())
    return r.fn(r.previous.traverse(split))
}

func (r *_PartitionMappedRDD) init(rdd RDD, f PartitionMapperFunc) {
    r._DerivedRDD.init(rdd, r)
    r.fn = f
}

func (r *_PartitionMappedRDD) String() string {
    return fmt.Sprintf("PartitionMappedRDD-%d <%s>", r.id, r._DerivedRDD.previous)
}

func newPartitionMappedRDD(rdd RDD, f PartitionMapperFunc) RDD {
    r := &_PartitionMappedRDD{}
    r.init(rdd, f)
    return r
}

////////////////////////////////////////////////////////////////////////
// FlatMappedRDD Impl
////////////////////////////////////////////////////////////////////////
type _FlatMappedRDD struct {
    _DerivedRDD
    fn  FlatMapperFunc
}

func (r *_FlatMappedRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        for arg := range r.previous.traverse(split) {
            value := r.fn(arg)
            if value != nil && len(value) > 0 {
                for _, subValue := range value {
                    yield <- subValue
                }
            }
        }
        close(yield)
    }()
    return yield
}

func (r *_FlatMappedRDD) init(rdd RDD, f FlatMapperFunc) {
    r._DerivedRDD.init(rdd, r)
    r.fn = f
}

func (r *_FlatMappedRDD) String() string {
    return fmt.Sprintf("FlatMappedRDD-%d <%s>", r.id, r._DerivedRDD.previous)
}

func newFlatMappedRDD(rdd RDD, f FlatMapperFunc) RDD {
    r := &_FlatMappedRDD{}
    r.init(rdd, f)
    return r
}

////////////////////////////////////////////////////////////////////////
// FilteredRDD Impl
////////////////////////////////////////////////////////////////////////
type _FilteredRDD struct {
    _DerivedRDD
    fn  FilterFunc
}

func (r *_FilteredRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        for value := range r.previous.traverse(split) {
            if r.fn(value) {
                yield <- value
            }
        }
        close(yield)
    }()
    return yield
}

func (r *_FilteredRDD) init(rdd RDD, f FilterFunc) {
    r._DerivedRDD.init(rdd, r)
    r.fn = f
}

func (r *_FilteredRDD) String() string {
    return fmt.Sprintf("FilteredRDD-%d <%s>", r.id, r._DerivedRDD.previous)
}

func newFilteredRDD(rdd RDD, f FilterFunc) RDD {
    r := &_FilteredRDD{}
    r.init(rdd, f)
    return r
}

////////////////////////////////////////////////////////////////////////
// SampledRDD Impl
////////////////////////////////////////////////////////////////////////
type _SampledRDD struct {
    _DerivedRDD
    fraction        float64
    seed            int64
    withReplacement bool
}

func (r *_SampledRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        seed := r.seed + int64(split.getIndex())
        rd := rand.New(rand.NewSource(seed))
        if r.withReplacement {
            allData := make([]interface{}, 0)
            for value := range r.previous.traverse(split) {
                allData = append(allData, value)
            }
            dCount := len(allData)
            sampleSize := int(math.Ceil(float64(dCount) * r.fraction))
            for i := 0; i < sampleSize; i++ {
                yield <- allData[rd.Intn(dCount)]
            }
        } else {
            for value := range r.previous.traverse(split) {
                if rd.Float64() <= r.fraction {
                    yield <- value
                }
            }
        }
        close(yield)
    }()
    return yield
}

func (r *_SampledRDD) init(rdd RDD, fraction float64, seed int64, withReplacement bool) {
    r._DerivedRDD.init(rdd, r)
    r.fraction = fraction
    r.seed = seed
    r.withReplacement = withReplacement
}

func (r *_SampledRDD) String() string {
    return fmt.Sprintf("SampledRDD-%d <%s>", r.id, r._DerivedRDD.previous)
}

func newSampledRDD(rdd RDD, fraction float64, seed int64, withReplacement bool) RDD {
    r := &_SampledRDD{}
    r.init(rdd, fraction, seed, withReplacement)
    return r
}

////////////////////////////////////////////////////////////////////////
// TextFileRDD Impl
////////////////////////////////////////////////////////////////////////
const DEFAULT_FILE_SPLIT_SIZE = 64 * 1024 * 1024 // 64MB Split Size

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

////////////////////////////////////////////////////////////////////////
// ShuffledRDD Impl
////////////////////////////////////////////////////////////////////////
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
    aggregator  *_Aggregator
    partitioner Partitioner
    numParts    int
    shuffleJob  sync.Once
}

type _ShuffleBucket map[interface{}][]interface{}

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
                log.Panic(err)
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

func (r *_ShuffledRDD) init(rdd RDD, aggregator *_Aggregator, partitioner Partitioner) {
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

func newShuffledRDD(rdd RDD, aggregator *_Aggregator, partitioner Partitioner) RDD {
    r := &_ShuffledRDD{}
    r.init(rdd, aggregator, partitioner)
    return r
}

////////////////////////////////////////////////////////////////////////
// OutputTextFileRDD Impl
////////////////////////////////////////////////////////////////////////
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
            log.Panicf("%s must be a directory in file system.", pathname)
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

////////////////////////////////////////////////////////////////////////
// Parallelize DataRDD Impl
////////////////////////////////////////////////////////////////////////
type _DataSplit struct {
    index  int
    values []interface{}
}

func (s *_DataSplit) getIndex() int {
    return s.index
}

type _DataRDD struct {
    _BaseRDD
    size int
}

func (r *_DataRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        dSplit := split.(*_DataSplit)
        for _, value := range dSplit.values {
            yield <- value
        }
        close(yield)
    }()
    return yield
}

func (r *_DataRDD) init(ctx *Context, data []interface{}, numPartitions int) {
    r._BaseRDD.init(ctx, r)
    r.size = len(data)
    if r.size <= 0 {
        log.Panicf("Please don't provide an empty data array.")
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
            index:  i,
            values: data[splitSize*i : end],
        }
    }
}

func (r *_DataRDD) String() string {
    return fmt.Sprintf("DataRDD-%d <size=%d>", r.id, r.size)
}

func newDataRDD(ctx *Context, data []interface{}) RDD {
    return newDataRDD_N(ctx, data, env.parallel)
}

func newDataRDD_N(ctx *Context, data []interface{}, numPartitions int) RDD {
    r := &_DataRDD{}
    r.init(ctx, data, numPartitions)
    return r
}

////////////////////////////////////////////////////////////////////////
// UnionRDD impl
////////////////////////////////////////////////////////////////////////
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

////////////////////////////////////////////////////////////////////////
// CoGroupedRDD impl
////////////////////////////////////////////////////////////////////////
type _CoGroupedRDD struct {
    _BaseRDD
    size int
    rdds []RDD
}

func (r *_CoGroupedRDD) compute(split Split) Yielder {
    yield := make(chan interface{}, 1)
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        keyGroups := make(map[interface{}][][]interface{})
        for groupIndex, rdd := range r.rdds {
            for value := range rdd.traverse(split) {
                keyValue := value.(*KeyValue)
                if _, ok := keyGroups[keyValue.Key]; !ok {
                    keyGroups[keyValue.Key] = make([][]interface{}, r.size)
                }
                keyGroups[keyValue.Key][groupIndex] = keyValue.Value.([]interface{})
            }
        }
        for key, groups := range keyGroups {
            yield <- &KeyGroups{
                Key:    key,
                Groups: groups,
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

////////////////////////////////////////////////////////////////////////
// CartesianRDD Impl
////////////////////////////////////////////////////////////////////////
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
    go func() {
        parklog("Computing <%s> on Split[%d]", r, split.getIndex())
        cSplit := split.(*_CartesianSplit)
        rightYields := make([]interface{}, 0)
        for i := range r.rdd1.traverse(cSplit.split1) {
            if len(rightYields) == 0 {
                for j := range r.rdd2.traverse(cSplit.split2) {
                    yield <- []interface{}{i, j}[:]
                    rightYields = append(rightYields, j)
                }
            } else {
                for _, j := range rightYields {
                    yield <- []interface{}{i, j}[:]
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
