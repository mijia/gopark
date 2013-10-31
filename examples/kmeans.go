package main

import (
    "encoding/gob"
    "fmt"
    "github.com/mijia/gopark"
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
    c := gopark.NewContext("kMeans")
    defer c.Stop()

    // This is important, have to register user types for gob to correctly encode.
    gob.Register(new(CenterCounter))

    D := 4
    K := 3
    MIN_DIST := 0.01

    centers := make([]gopark.Vector, K)
    for i := range centers {
        centers[i] = gopark.NewRandomVector(D)
    }
    fmt.Println(centers)

    points := c.TextFile("kmeans_data.txt").Map(func(line string) gopark.Vector {
        vs := strings.Fields(line)
        dims := make(gopark.Vector, len(vs))
        for i := range vs {
            dims[i], _ = strconv.ParseFloat(vs[i], 64)
        }
        return dims
    }).Cache()

    for i := 0; i < 10; i++ {
        fmt.Println("Iter:", i)
        mappedPoints := points.Map(func(p gopark.Vector) *gopark.KeyValue {
            center := CloseCenter(p, centers)
            return &gopark.KeyValue{
                Key:   center,
                Value: &CenterCounter{p, 1},
            }
        })
        newCenters := mappedPoints.ReduceByKey(func(x, y *CenterCounter) *CenterCounter {
            return &CenterCounter{
                X:     x.X.Plus(y.X),
                Count: x.Count + y.Count,
            }
        }).Map(func(keyValue *gopark.KeyValue) *gopark.KeyValue {
            cc := keyValue.Value.(*CenterCounter)
            return &gopark.KeyValue{
                Key:   keyValue.Key,
                Value: cc.X.Divide(float64(cc.Count)),
            }
        }).CollectAsMap().(map[int]gopark.Vector)

        updated := false
        for cid, center := range newCenters {
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
