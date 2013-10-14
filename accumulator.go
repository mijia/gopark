package gopark

import (
    "fmt"
)

type AccumulateFunc func(x, y interface{}) interface{}

type AccumulatorParam interface {
    AddFunc() AccumulateFunc
}

type accumulatorParam struct {
    fn AccumulateFunc
}

func (ap accumulatorParam) AddFunc() AccumulateFunc {
    return ap.fn
}

var IntAccumulatorParam AccumulatorParam
var ListAccumulatorParam AccumulatorParam

func init() {
    IntAccumulatorParam = accumulatorParam{
        fn: func(x, y interface{}) interface{} {
            return x.(int) + y.(int)
        },
    }

    ListAccumulatorParam = accumulatorParam{
        fn: func(x, y interface{}) interface{} {
            return append(x.([]interface{}), y)
        },
    }
}

type Accumulator interface {
    Add(interface{})
    Value() interface{}
}

type _BaseAccumulator struct {
    id       int64
    param    AccumulatorParam
    value    interface{}
    accuChan chan interface{}
}

func (a *_BaseAccumulator) init(initValue interface{}, param AccumulatorParam) {
    a.id = newAccumulatorId()
    a.value = initValue
    a.param = param
    a.accuChan = make(chan interface{})
    go func() {
        for {
            localValue := <-a.accuChan
            a.value = a.param.AddFunc()(a.value, localValue)
        }
    }()
}

func (a *_BaseAccumulator) Add(x interface{}) {
    a.accuChan <- x
}

func (a *_BaseAccumulator) Value() interface{} {
    return a.value
}

func newIntAccumulator(initValue int) Accumulator {
    return newAccumulator(initValue, IntAccumulatorParam)
}

func newAccumulator(initValue interface{}, param AccumulatorParam) Accumulator {
    a := &_BaseAccumulator{}
    a.init(initValue, param)
    return a
}

var nextAccuId AtomicInt = 0

func newAccumulatorId() int64 {
    nextAccuId.Add(1)
    return nextAccuId.Get()
}

var _ = fmt.Println
