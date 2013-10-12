package gopark

import ()

/*
 * This is a really simple recurrsive implementation of scheduler,
 * just call the registered function via the flow of rdd chain.
 * You may need to reconsider this.
 */

//////////////////////////////////////////////////////
// DAGScheduler base impl
//////////////////////////////////////////////////////
type Scheduler interface {
    start()
    clear()
    stop()
    runRoutine(rdd RDD, partitions []int, rn ReducerFn) []Yielder
}

type _DAGScheduler struct {
}

func (d *_DAGScheduler) init() {
}

func (d *_DAGScheduler) runRoutine(s Scheduler, rdd RDD, partitions []int, rn ReducerFn) []Yielder {
    numOutputParts := len(partitions)
    yields := make([]Yielder, numOutputParts)
    for i := 0; i < numOutputParts; i++ {
        split := rdd.getSplit(partitions[i])
        yields[i] = make(chan interface{}, 1)
        go func(yield Yielder, partition int) {
            yield <- rn(rdd.traverse(split), partition)
            close(yield)
        }(yields[i], i)
    }
    return yields
}

//////////////////////////////////////////////////////
// LocalScheduler impl
//////////////////////////////////////////////////////
type _LocalScheduler struct {
    _DAGScheduler
}

func (s *_LocalScheduler) init() {
    s._DAGScheduler.init()
}

func (s *_LocalScheduler) start() {}
func (s *_LocalScheduler) stop()  {}
func (s *_LocalScheduler) clear() {}

func (s *_LocalScheduler) runRoutine(rdd RDD, partitions []int, rn ReducerFn) []Yielder {
    return s._DAGScheduler.runRoutine(s, rdd, partitions, rn)
}

func newLocalScheduler() Scheduler {
    local := &_LocalScheduler{}
    local.init()
    return local
}
