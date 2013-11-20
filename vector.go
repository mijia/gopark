package gopark

import (
    "encoding/gob"
    "fmt"
    "math"
    "math/rand"
    "strings"
    "time"
)

type Vector []float64
type IndexedVector map[interface{}]float64

func init() {
    gob.Register(new(Vector))
    gob.Register(new(IndexedVector))
}

// Vector methods
func NewZeroVector(size int) Vector {
    v := make(Vector, size)
    return v
}

func NewSameValueVector(size int, value float64) Vector {
    v := make(Vector, size)
    for i := range v {
        v[i] = value
    }
    return v
}

func NewRandomVector(size int) Vector {
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    v := make(Vector, size)
    for i := range v {
        v[i] = r.Float64()
    }
    return v
}

func NewRandomNormVector(size int, dev, mean float64) Vector {
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    v := make(Vector, size)
    for i := range v {
        v[i] = r.NormFloat64()*dev + mean
    }
    return v
}

func NewRandomLimitedVector(size int, minValue, maxValue float64) Vector {
    if maxValue <= minValue {
        panic("NewRandomLimitedVector cannot use maxValue <= minValue")
    }
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    v := make(Vector, size)
    for i := range v {
        v[i] = r.Float64()*(maxValue-minValue) + minValue
    }
    return v
}

func (v Vector) String() string {
    fields := make([]string, len(v))
    for i := range v {
        fields[i] = fmt.Sprintf("%.10f", v[i])
    }
    return strings.Join(fields, "\t")
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
    return result
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

func (v Vector) NormL2() float64 {
    return v.Dot(v)
}

func (v Vector) Magnitude() float64 {
    return math.Sqrt(v.Dot(v))
}

func (v Vector) Cosine(o Vector) float64 {
    return v.Dot(o) / v.Magnitude() / o.Magnitude()
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

// IndexedVector methods
func NewIndexedVector() IndexedVector {
    return make(IndexedVector)
}

func (v IndexedVector) String() string {
    fields := make([]string, len(v))
    i := 0
    for key, value := range v {
        fields[i] = fmt.Sprintf("%v:%v", key, value)
        i++
    }
    return strings.Join(fields, "\t")
}

func (v IndexedVector) RandomFeature(feature interface{}) {
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    v[feature] = r.Float64()
}

func (v IndexedVector) RandomLimitedFeature(feature interface{}, minValue, maxValue float64) {
    if maxValue <= minValue {
        panic("RandomLimitedFeature cannot use maxValue <= minValue")
    }
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    v[feature] = r.Float64()*(maxValue-minValue) + minValue
}

func (v IndexedVector) RandomNormFeature(feature interface{}, dev, mean float64) {
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    v[feature] = r.NormFloat64()*dev + mean
}

func (v IndexedVector) Copy() IndexedVector {
    result := make(IndexedVector)
    for key, value := range v {
        result[key] = value
    }
    return result
}

func (v IndexedVector) Keys() []interface{} {
    keys := make([]interface{}, len(v))
    i := 0
    for key := range v {
        keys[i] = key
        i++
    }
    return keys
}

func (v IndexedVector) Plus(o IndexedVector) IndexedVector {
    result := v.Copy()
    for key, value := range o {
        result[key] += value
    }
    return result
}

func (v IndexedVector) Minus(o IndexedVector) IndexedVector {
    result := v.Copy()
    for key, value := range o {
        result[key] -= value
    }
    return result
}

func (v IndexedVector) Multiply(m float64) IndexedVector {
    result := v.Copy()
    for key := range result {
        result[key] *= m
    }
    return result
}

func (v IndexedVector) Divide(d float64) IndexedVector {
    if d == 0 {
        panic("IndexedVector divide by zero")
    }
    result := v.Copy()
    for key := range result {
        result[key] /= d
    }
    return result
}

func (v IndexedVector) Sum() float64 {
    var sum float64 = 0
    for _, value := range v {
        sum += value
    }
    return sum
}

func (v IndexedVector) Dot(o IndexedVector) float64 {
    var sum float64 = 0
    vx, vy := v, o
    if len(v) > len(o) {
        vx, vy = o, v
    }
    for key, value := range vx {
        sum += value * vy[key]
    }
    return sum
}

func (v IndexedVector) NormL2() float64 {
    var sum float64 = 0
    for _, value := range v {
        sum += value * value
    }
    return sum
}

func (v IndexedVector) Magnitude() float64 {
    return math.Sqrt(v.NormL2())
}

func (v IndexedVector) Cosine(o IndexedVector) float64 {
    return v.Dot(o) / v.Magnitude() / o.Magnitude()
}

func (v IndexedVector) EulaDistance(o IndexedVector) float64 {
    var dist float64 = 0
    for key, value := range v {
        dist += (value - o[key]) * (value - o[key])
    }
    for key, value := range o {
        if _, ok := v[key]; !ok {
            dist += value * value
        }
    }
    return dist
}
