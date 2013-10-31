package main

import (
    "fmt"
    "github.com/mijia/gopark"
    "math"
    "math/rand"
    "strconv"
    "strings"
)

func main() {
    gopark.ParseOptions()
    c := gopark.NewContext("testflight")
    defer c.Stop()

    WordCount(c)
    ComputePi(c)
    LogisticRegression(c)
}

func WordCount(c *gopark.Context) {
    txt := c.TextFile("../")
    counts := txt.FlatMap(func(line interface{}) []interface{} {
        vs := strings.Fields(line.(string))
        words := make([]interface{}, len(vs))
        for i := range vs {
            words[i] = vs[i]
        }
        return words
    }).Map(func(x interface{}) interface{} {
        return &gopark.KeyValue{x, 1}
    }).ReduceByKey(func(x, y interface{}) interface{} {
        return x.(int) + y.(int)
    }).Cache()

    fmt.Println(
        counts.Filter(func(x interface{}) bool {
            return x.(*gopark.KeyValue).Value.(int) > 50
        }).CollectAsMap())

    fmt.Println(
        counts.Filter(func(x interface{}) bool {
            return x.(*gopark.KeyValue).Value.(int) > 50
        }).Map(func(x interface{}) interface{} {
            keyValue := x.(*gopark.KeyValue)
            keyValue.Key, keyValue.Value = keyValue.Value, keyValue.Key
            return keyValue
        }).GroupByKey().Collect())
}

func ComputePi(c *gopark.Context) {
    N := 100000
    iters := c.Data(make([]interface{}, N))
    count := iters.Map(func(_ interface{}) interface{} {
        x := rand.Float32()
        y := rand.Float32()
        if x*x+y*y < 1 {
            return 1
        } else {
            return 0
        }
    }).Reduce(func(x, y interface{}) interface{} {
        return x.(int) + y.(int)
    }).(int)
    fmt.Println("Pi =", (4.0 * float64(count) / float64(N)))
}

func LogisticRegression(c *gopark.Context) {
    type dataPoint struct {
        x   gopark.Vector
        y   float64
    }

    points := c.TextFile("points.txt").Map(func(line interface{}) interface{} {
        vs := strings.Fields(line.(string))
        vector := make(gopark.Vector, len(vs)-1)
        for i := 0; i < len(vs)-1; i++ {
            vector[i], _ = strconv.ParseFloat(vs[i], 64)
        }
        y, _ := strconv.ParseFloat(vs[len(vs)-1], 64)
        return &dataPoint{vector, y}
    }).Cache()

    hx := func(w, x gopark.Vector, y float64) float64 {
        return 1/(1+math.Exp(-1*w.Dot(x))) - y
    }
    var w gopark.Vector = []float64{1, -10}[:]
    for i := 0; i < 10; i++ {
        gradient := points.Map(func(x interface{}) interface{} {
            p := x.(*dataPoint)
            return p.x.Multiply(-1 * hx(w, p.x, p.y))
        }).Reduce(func(x, y interface{}) interface{} {
            return x.(gopark.Vector).Plus(y.(gopark.Vector))
        }).(gopark.Vector)
        w = w.Minus(gradient)
    }
    fmt.Println("Final Weights:", w)
}
