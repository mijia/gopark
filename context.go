package gopark

import (
    "fmt"
    "log"
    "os"
    "os/signal"
    "path/filepath"
    "runtime"
    "syscall"
    "time"
)

type KeyValue struct {
    Key   interface{}
    Value interface{}
}

func (kv *KeyValue) String() string {
    return fmt.Sprintf("%v:%v", kv.Key, kv.Value)
}

type KeyGroups struct {
    Key    interface{}
    Groups [][]interface{}
}

type KeyLessFunc func(x, y interface{}) bool

type ParkSorter struct {
    values []interface{}
    fn     KeyLessFunc
}

func (s *ParkSorter) Len() int {
    return len(s.values)
}

func (s *ParkSorter) Swap(i, j int) {
    s.values[i], s.values[j] = s.values[j], s.values[i]
}

func (s *ParkSorter) Less(i, j int) bool {
    return s.fn(s.values[i], s.values[j])
}

func NewParkSorter(values []interface{}, fn KeyLessFunc) *ParkSorter {
    return &ParkSorter{values, fn}
}

type Yielder chan interface{}
type ReducerFn func(yield Yielder, partition int) interface{}

type Context struct {
    jobName    string
    scheduler  Scheduler
    initialzed bool
    started    bool
    startTime  time.Time
}

func (c *Context) String() string {
    return fmt.Sprintf("Context-[%s]", c.jobName)
}

func (c *Context) init() {
    if c.initialzed {
        return
    }

    c.scheduler = newLocalScheduler()
    c.initialzed = true
    log.Printf("Gpark Context [%s] initialzed.", c.jobName)
}

func (c *Context) start() {
    if c.started {
        return
    }

    c.init()
    env.start()
    c.scheduler.start()

    signalChan := make(chan os.Signal, 1)
    signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT)
    go func() {
        s := <-signalChan
        log.Printf("Captured the signal %v\n", s)
        c.Stop()
        os.Exit(2)
    }()
    c.started = true
    c.startTime = time.Now()
    log.Printf("Context [%s] is started.", c.jobName)
}

func (c *Context) Stop() {
    if !c.started {
        return
    }

    env.stop()
    c.scheduler.stop()
    c.started = false
    log.Printf("Context [%s] is stopped, duration = %s.", c.jobName, (time.Since(c.startTime)))
}

func (c *Context) runRoutine(rdd RDD, partitions []int, rn ReducerFn) []Yielder {
    if partitions == nil {
        partitions = make([]int, rdd.len())
        for i := 0; i < rdd.len(); i++ {
            partitions[i] = i
        }
    }
    if len(partitions) == 0 {
        return nil
    }

    c.start()
    return c.scheduler.runRoutine(rdd, partitions, rn)
}

func (c *Context) TextFile(pathname string) RDD {
    absPathname, err := filepath.Abs(pathname)
    if err != nil {
        panic(err)
    }
    if fStat, err := os.Stat(absPathname); err != nil {
        panic(err)
    } else {
        if !fStat.IsDir() {
            return newTextFileRDD(c, absPathname)
        }
        pathNames := make([]string, 0)
        err = filepath.Walk(absPathname, func(path string, info os.FileInfo, err error) error {
            if !info.IsDir() {
                pathNames = append(pathNames, path)
            }
            return nil
        })
        if err != nil {
            panic(err)
        }

        rdds := make([]RDD, len(pathNames))
        for i := range pathNames {
            rdds[i] = newTextFileRDD(c, pathNames[i])
        }
        return c.Union(rdds)
    }
}

func (c *Context) Union(rdds []RDD) RDD {
    return newUnionRDD(c, rdds)
}

func (c *Context) Data(d []interface{}) RDD {
    return newDataRDD(c, d)
}

func (c *Context) Data_N(d []interface{}, numPartitions int) RDD {
    return newDataRDD_N(c, d, numPartitions)
}

func (c *Context) Accumulator(initValue int) Accumulator {
    return newIntAccumulator(initValue)
}

func (c *Context) AccumulatorWithParam(initValue interface{}, param AccumulatorParam) Accumulator {
    return newAccumulator(initValue, param)
}

func NewContext(jobName string) *Context {
    return &Context{
        jobName:    jobName,
        initialzed: false,
        started:    false,
    }
}

func init() {
    log.SetFlags(log.LstdFlags | log.Lshortfile)
    runtime.GOMAXPROCS(runtime.NumCPU())
}

var _ = fmt.Println
