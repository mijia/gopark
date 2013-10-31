GoPark
=============

GoPark is a naive local version porting of [Spark](http://spark.incubator.apache.org/)/[Dpark](https://github.com/douban/dpark), MapReduce(R) alike computing framework supporting iterative computation.

GoPark is implemented in Go languange, and provides the cocurrent MapReduce(R) data operations using GoRoutines. It can only run in the local mode, but you can specify the concurrent numbers.

Dependency
-------------

GoPark now uses type parametric functions with run time type safety. This really counts on the awesome lib provided by [https://github.com/BurntSushi/ty](https://github.com/BurntSushi/ty). But I need a simple workaround for the interface{} defined as parameters in function to work with any other input types, so I forked the project and create a pull request (hasn't been merged yet, will turn to that after it got merged). 

```
go get github.com/mijia/ty
go get github.com/mijia/gopark
```

Examples
-------------

Examples for computing PI:
```
package main

import (
    "fmt"
    "github.com/mijia/gopark"
    "math/rand"
)

func main() {
    gopark.ParseOptions()
    c := gopark.NewContext("ComputePI")
    defer c.Stop()

    N := 100000
    count := c.Data(gopark.Range(0, N)).Map(func(_ int) int {
        x := rand.Float32()
        y := rand.Float32()
        if x*x+y*y < 1 {
            return 1
        } else {
            return 0
        }
    }).Reduce(func(x, y int) int {
        return x + y
    }).(int)
    fmt.Println("Pi =", (4.0 * float64(count) / float64(N)))
}
```

The above code can be runned as (using 4 go routines concurrently.)
```
$ go run hello.go -p=4
```
Checkout the examples/ for more cases.

function as interface{} and reflection
-------------
Let's take an example. RDD.Map's signature is like
```
Map(fn interface{}) RDD
```
The older one is like
```
Map(fn func(interface{}) interface{}) RDD
```
Now since we have the ability of runtime check from the [https://github.com/BurntSushi/ty](https://github.com/BurntSushi/ty) and also take advantage of [go/reflect](http://golang.org/pkg/reflect/), we can do things like
```
d := []int{1, 2, 3, 4, 5}[:]
c.Data(d).Map(func(x int) string {
    return fmt.Sprintf("%05d", x)
}).Collect().([]string)
```

Shuffle and Shuffle_N like funcs
-------------
Some functions which do shuffle job like ```GroupByKey()``` also provides the ```GroupByKey_N()``` func, which user can specify the numPartitions that job should run on. Please check rdd.go for references.

Encode / Gob
-------------
For the shuffle jobs like ```GroupByKey()``` and ```Persist()```, GoPark uses encoding/gob as the encoder/decoder into local files, since GoPark uses interface{} as the parameters, GOB need to know what the interface{} actually is when decoding. Which can be done like the kmeans.go example shows,
```
type CenterCounter struct {
    X     gopark.Vector
    Count int
}

gob.Register(new(CenterCounter))
```
and you cannot use structs with unexported fields. Just be careful with this, if you got runtime panics, please check
* if you have use a complicated struct
* if you hadn't register the type on GOB, even like ```type T int```
* if you have use slices of slices of slices ....
Just make sure GOB knows your objects behind the interface{} and []interface{}.

Things not included
-------------
So far, the Broadcast are still not implemented. I am just using the
vars in closure.

And GoPark now really cannot run in the distributed mode.

Have fun~

Originally, I have only two goals in writing this,
* Write some real stuff in Go, since I am learning the language
* I am doing data mining jobs and I need some better concurrent framework for performance, and runs locally is ok for me.

Spark/DPark
-------------
These projects are really awesome and the RDD is really a fantastic data structure or design pattern. I learned a lot in them. 

Really want to thank these two projects.
