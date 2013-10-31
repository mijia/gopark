package main

import (
    "encoding/gob"
    "fmt"
    "github.com/mijia/gopark"
    "math/rand"
    "strconv"
    "strings"
)

func CloseCenter(p gopark.Vector, centers []gopark.Vector) int {
    minDist := p.EulaDistance(centers[0])
    minIndex := 0
    for i := 1; i < len(centers); i++ {
        dist := p.EulaDistance(centers[i])
        if dist < minDist {
            minDist = dist
            minIndex = i
        }
    }
    return minIndex
}

type CenterCounter struct {
    X     gopark.Vector
    Count int
}

func main() {
    gopark.ParseOptions()
    c := gopark.NewContext("kmeans")
    defer c.Stop()

    // This is important, have to register user types for gob to correctly encode.
    gob.Register(new(CenterCounter))

    D := 4
    K := 3
    MIN_DIST := 0.01

    centers := make([]gopark.Vector, K)
    for i := range centers {
        center := make(gopark.Vector, D)
        for j := range center {
            center[j] = rand.Float64()
        }
        centers[i] = center
    }
    fmt.Println(centers)

    points := c.TextFile("kmean_data.txt").Map(func(line interface{}) interface{} {
        vs := strings.Fields(line.(string))
        dims := make(gopark.Vector, len(vs))
        for i := range vs {
            dims[i], _ = strconv.ParseFloat(vs[i], 64)
        }
        return dims
    }).Cache()

    for i := 0; i < 10; i++ {
        fmt.Println("Iter:", i)
        mappedPoints := points.Map(func(x interface{}) interface{} {
            p := x.(gopark.Vector)
            center := CloseCenter(p, centers)
            return &gopark.KeyValue{
                Key:   center,
                Value: &CenterCounter{p, 1},
            }
        })
        newCenters := mappedPoints.ReduceByKey(func(x, y interface{}) interface{} {
            cc1 := x.(*CenterCounter)
            cc2 := y.(*CenterCounter)
            return &CenterCounter{
                X:     cc1.X.Plus(cc2.X),
                Count: cc1.Count + cc2.Count,
            }
        }).Map(func(x interface{}) interface{} {
            keyValue := x.(*gopark.KeyValue)
            cc := keyValue.Value.(*CenterCounter)
            return &gopark.KeyValue{
                Key:   keyValue.Key,
                Value: cc.X.Divide(float64(cc.Count)),
            }
        }).CollectAsMap()

        updated := false
        for key, value := range newCenters {
            center := value.(gopark.Vector)
            cid := key.(int)
            if center.EulaDistance(centers[cid]) > MIN_DIST {
                centers[cid] = center
                updated = true
            }
        }
        if !updated {
            break
        }
    }

    fmt.Println("Final Centers:", centers)
}
