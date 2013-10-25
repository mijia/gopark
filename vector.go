package gopark

import (
    "encoding/gob"
    "fmt"
    "math"
    "math/rand"
    "time"
)

type Vector []float64

func init() {
    gob.Register(new(Vector))
}

func NewZeroVector(size int) Vector {
    v := make(Vector, size)
    return v
}

func NewRandomVector(size int) Vector {
    r := rand.New(rand.NewSource(time.Now().Unix()))
    v := make(Vector, size)
    for i := range v {
        v[i] = r.Float64()
    }
    return v
}

func (v Vector) Plus(o Vector) Vector {
    return biVectorsOp(v, o, func(x, y float64) float64 {
        return x + y
    })
}

func (v Vector) Minus(o Vector) Vector {
    return biVectorsOp(v, o, func(x, y float64) float64 {
        return x - y
    })
}

func (v Vector) Multiply(m float64) Vector {
    result := make(Vector, len(v))
    for i := range v {
        result[i] = v[i] * m
    }
    return result
}

func (v Vector) Divide(d float64) Vector {
    if d == 0 {
        panic(fmt.Errorf("Vector divided by zero."))
    }
    result := make(Vector, len(v))
    for i := range v {
        result[i] = v[i] / d
    }
    return v
}

func (v Vector) Sign() Vector {
    return v.Multiply(-1)
}

func (v Vector) Sum() float64 {
    sum := 0.0
    for i := range v {
        sum += v[i]
    }
    return sum
}

func (v Vector) Dot(o Vector) float64 {
    if len(v) != len(o) {
        panic(fmt.Errorf("Two vectors of different length"))
    }
    sum := 0.0
    for i := range v {
        sum += v[i] * o[i]
    }
    return sum
}

func (v Vector) Magnitude() float64 {
    return math.Sqrt(v.Dot(v))
}

func (v Vector) EulaDistance(o Vector) float64 {
    if len(v) != len(o) {
        panic(fmt.Errorf("Two vectors of different length"))
    }
    dist := 0.0
    for i := range v {
        dist += (v[i] - o[i]) * (v[i] - o[i])
    }
    return math.Sqrt(dist)
}

type operand func(x, y float64) float64

func biVectorsOp(v1, v2 Vector, fn operand) Vector {
    if len(v1) != len(v2) {
        panic(fmt.Errorf("Two vectors of different length"))
    }
    result := make(Vector, len(v1))
    for i := range v1 {
        result[i] = fn(v1[i], v2[i])
    }
    return result
}
