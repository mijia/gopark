package main

import (
    "encoding/gob"
    "fmt"
    "github.com/mijia/gopark"
    "math"
    "math/rand"
    "strconv"
    "strings"
)

func main() {
    gopark.ParseOptions()
    c := gopark.NewContext("Demo")
    defer c.Stop()

    WordCount(c)
    ComputePi(c)
    LogisticRegression(c)
}

func WordCount(c *gopark.Context) {
    txt := c.TextFile("../")
    counts := txt.FlatMap(func(line string) []string {
        vs := strings.Fields(line)
        words := make([]string, len(vs))
        for i := range vs {
            words[i] = vs[i]
        }
        return words
    }).Map(func(x string) *gopark.KeyValue {
        return &gopark.KeyValue{x, 1}
    }).ReduceByKey(func(x, y int) int {
        return x + y
    }).Cache()

    fmt.Println(
        counts.Filter(func(x *gopark.KeyValue) bool {
            return x.Value.(int) > 50
        }).CollectAsMap().(map[string]int))

    fmt.Println(
        counts.Filter(func(x *gopark.KeyValue) bool {
            return x.Value.(int) > 50
        }).Map(func(keyValue *gopark.KeyValue) *gopark.KeyValue {
            keyValue.Key, keyValue.Value = keyValue.Value, keyValue.Key
            return keyValue
        }).GroupByKey().Collect().([]*gopark.KeyValue))
}

func ComputePi(c *gopark.Context) {
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

func LogisticRegression(c *gopark.Context) {
    type DataPoint struct {
        X   gopark.Vector
        Y   float64
    }
    gob.Register(new(DataPoint))

    points := c.TextFile("points.txt").Map(func(line string) *DataPoint {
        vs := strings.Fields(line)
        vector := make(gopark.Vector, len(vs)-1)
        for i := 0; i < len(vs)-1; i++ {
            vector[i], _ = strconv.ParseFloat(vs[i], 64)
        }
        y, _ := strconv.ParseFloat(vs[len(vs)-1], 64)
        return &DataPoint{vector, y}
    }).Cache()

    hx := func(w, x gopark.Vector, y float64) float64 {
        return 1/(1+math.Exp(-1*w.Dot(x))) - y
    }
    var w gopark.Vector = []float64{1, -10}[:]
    for i := 0; i < 10; i++ {
        gradient := points.Map(func(p *DataPoint) gopark.Vector {
            return p.X.Multiply(-1 * hx(w, p.X, p.Y))
        }).Reduce(func(x, y gopark.Vector) gopark.Vector {
            return x.Plus(y)
        }).(gopark.Vector)
        w = w.Minus(gradient)
    }
    fmt.Println("Final Weights:", w)
}
