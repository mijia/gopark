package gopark

import (
    "fmt"
    "sort"
    "strings"
    "testing"
)

func setupEnv() {
    env.master = "local"
    env.parallel = 3
    env.goparkWorkDir = "/opt/tmp"
}

func TestDataRDD(t *testing.T) {
    setupEnv()
    c := NewContext("TestDataRDD")
    fmt.Printf("\n\n%s\n", c)
    defer c.Stop()
    a := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}[:]
    data := c.Data(a)
    if data.Count() != 10 {
        t.Errorf("Data parallellized error, with %s", data)
    }
}

func TestTextFile(t *testing.T) {
    setupEnv()
    c := NewContext("TestTextFile")
    fmt.Printf("\n\n%s\n", c)
    defer c.Stop()
    txt := c.TextFile("examples/points.txt")
    samples := txt.Map(func(line interface{}) interface{} {
        vs := strings.Fields(line.(string))
        d2 := vs[1]
        y := vs[2]
        return &KeyValue{y, d2}
    }).Take(5)
    for _, sample := range samples {
        fmt.Println(sample)
        if _, ok := sample.(*KeyValue); !ok {
            t.Error("TextFile KeyValue Error")
        }
    }
    if len(samples) != 5 {
        t.Errorf("TextFile Samples length error, %d", len(samples))
    }
}

func TestSimpleMappers(t *testing.T) {
    setupEnv()
    c := NewContext("TestSimpleMappers")
    fmt.Printf("\n\n%s\n", c)
    defer c.Stop()
    d := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}[:]
    data := c.Data(d)

    mData := data.Map(func(arg interface{}) interface{} {
        return arg
    })
    fmt.Println(mData.Collect())
    if int(mData.Count()) != len(d) {
        t.Errorf("Mapping data error, %v", mData.Collect())
    }

    flatData := mData.FlatMap(func(arg interface{}) []interface{} {
        t := make([]interface{}, 2)
        t[0] = arg
        t[1] = arg.(int) * 10
        return t
    })
    fmt.Println(flatData.Collect())
    if int(flatData.Count()) != 2*len(d) {
        t.Errorf("FlatMap data error, %v", flatData.Collect())
    }

    filterData := flatData.Filter(func(arg interface{}) bool {
        return arg.(int) < 10
    })
    fmt.Println(filterData.Collect())
    if int(filterData.Count()) <= 0 {
        t.Errorf("Filter data error, %v", filterData.Collect())
    }

    samples := flatData.Sample(0.25, 42).Take(5)
    fmt.Println(samples)
    if len(samples) <= 0 {
        t.Errorf("Sample Data error, %v", samples)
    }

    mapPartition := data.MapPartition(func(iter Yielder) Yielder {
        yield := make(chan interface{}, 1)
        go func() {
            for value := range iter {
                yield <- value
                yield <- value
            }
            close(yield)
        }()
        return yield
    }).Collect()
    fmt.Println(mapPartition)
    if len(mapPartition) != 2*len(d) {
        t.Error("MapPartition data error.")
    }
}

func TestKeyValueMappers(t *testing.T) {
    setupEnv()
    c := NewContext("TestKeyValueMappers")
    fmt.Printf("\n\n%s\n", c)
    defer c.Stop()
    d := []interface{}{
        &KeyValue{1, 10},
        &KeyValue{1, 11},
        &KeyValue{2, 12},
        &KeyValue{3, 13},
        &KeyValue{4, 14},
        &KeyValue{4, 15},
        &KeyValue{5, 16},
        &KeyValue{6, 17},
        &KeyValue{5, 18},
    }[:]
    data := c.Data(d)

    group := data.GroupByKey().CollectAsMap()
    fmt.Println(group)
    for i := 1; i <= 6; i++ {
        if _, ok := group[i]; !ok {
            t.Errorf("%d key not in GroupByKey Map", i)
        }
    }

    pKey := data.PartitionByKey().Collect()
    fmt.Println(pKey)
    if len(pKey) != len(d) {
        t.Error("PartitionByKey data error")
    }

    distinct := data.Map(func(x interface{}) interface{} {
        return x.(*KeyValue).Key
    }).Distinct().Collect()
    fmt.Println(distinct)
    if len(distinct) != 6 {
        t.Errorf("Distinct data error, %v", distinct)
    }

    countKeys := data.CountByKey()
    fmt.Println(countKeys)
    for i := 1; i <= 6; i++ {
        if _, ok := countKeys[i]; !ok {
            t.Errorf("%d key not in CountByKey Map", i)
        }
    }
}

func TestSimpleReducer(t *testing.T) {
    setupEnv()
    c := NewContext("TestSimpleReducer")
    fmt.Printf("\n\n%s\n", c)
    defer c.Stop()
    d := []interface{}{1, 2, 4, 5, 7, 3, 3, 3, 1}[:]
    data := c.Data(d)

    sum := data.Reduce(func(x, y interface{}) interface{} {
        return x.(int) + y.(int)
    })
    fmt.Println(sum)
    if sum != 29 {
        t.Error("Reduce function failed.")
    }

    countValues := data.CountByValue()
    fmt.Println(countValues)
    if len(countValues) <= 0 {
        t.Error("CountByValue data error.")
    }

    data.Foreach(func(x interface{}) {
        fmt.Println("Happend to found", x.(int))
    })
}

func TestUnionRDD(t *testing.T) {
    setupEnv()
    c := NewContext("TestUnionRDD")
    fmt.Printf("\n\n%s\n", c)
    defer c.Stop()
    d1 := []interface{}{1, 2, 3, 4, 5}[:]
    d2 := []interface{}{6, 7, 8, 9, 10}[:]
    u := c.Data(d1).Union(c.Data(d2)).Collect()
    fmt.Println(u)
    if len(u) != len(d1)+len(d2) {
        t.Error("Union RDD failed.")
    }
}

func TestJoins(t *testing.T) {
    setupEnv()
    c := NewContext("TestJoins")
    fmt.Printf("\n\n%s\n", c)
    defer c.Stop()
    d1 := []interface{}{
        &KeyValue{1, 10},
        &KeyValue{2, 11},
        &KeyValue{3, 12},
        &KeyValue{5, 13},
        &KeyValue{7, 15},
    }[:]
    d2 := []interface{}{
        &KeyValue{1, 100},
        &KeyValue{2, 111},
        &KeyValue{5, 131},
        &KeyValue{6, 161},
    }[:]

    data1 := c.Data(d1)
    data2 := c.Data(d2)

    join := data1.Join(data2).CollectAsMap()
    for key, value := range join {
        fmt.Println(key, value)
    }
    for _, key := range []int{1, 2, 5}[:] {
        if _, ok := join[key]; !ok {
            t.Errorf("%d key not in join map, that's wrong", key)
        }
    }
    for _, key := range []int{3, 7, 6}[:] {
        if _, ok := join[key]; ok {
            t.Errorf("%d key is in join map, that's wrong", key)
        }
    }

    leftJoin := data1.LeftOuterJoin(data2).CollectAsMap()
    for key, value := range leftJoin {
        fmt.Println(key, value)
    }
    for _, key := range []int{1, 2, 3, 5, 7}[:] {
        if _, ok := leftJoin[key]; !ok {
            t.Errorf("%d key not in left join map, that's wrong", key)
        }
    }

    rightJoin := data1.RightOuterJoin(data2).CollectAsMap()
    for key, value := range rightJoin {
        fmt.Println(key, value)
    }
    for _, key := range []int{1, 2, 5, 6}[:] {
        if _, ok := rightJoin[key]; !ok {
            t.Errorf("%d key not in left join map, that's wrong", key)
        }
    }
}

func TestCartesian(t *testing.T) {
    setupEnv()
    c := NewContext("TestCartesian")
    fmt.Printf("\n\n%s\n", c)
    defer c.Stop()
    d1 := []interface{}{1, 2, 3, 4, 5, 6}[:]
    d2 := []interface{}{"a", "b", "c", "d", "e"}[:]

    data1 := c.Data(d1)
    data2 := c.Data(d2)
    cart := data1.Cartesian(data2).Collect()
    fmt.Println(cart)
    if len(cart) != len(d1)*len(d2) {
        t.Error("Cartesian data error.")
    }
}

func TestSortByKey(t *testing.T) {
    setupEnv()
    c := NewContext("TestSortByKey")
    fmt.Printf("\n\n%s\n", c)
    defer c.Stop()
    d := []interface{}{
        &KeyValue{1, "a"},
        &KeyValue{4, "b"},
        &KeyValue{6, "c"},
        &KeyValue{3, "d"},
        &KeyValue{7, "e"},
        &KeyValue{8, "f"},
        &KeyValue{1, "a"},
        &KeyValue{4, "b"},
        &KeyValue{6, "c"},
        &KeyValue{3, "d"},
        &KeyValue{7, "e"},
        &KeyValue{8, "f"},
    }[:]

    data := c.Data(d).SortByKey(func(x, y interface{}) bool {
        return x.(int) < y.(int)
    }, true).Collect()
    fmt.Println(data)
    sorter := NewParkSorter(data, func(x, y interface{}) bool {
        return x.(*KeyValue).Key.(int) < y.(*KeyValue).Key.(int)
    })
    if !sort.IsSorted(sort.Reverse(sorter)) {
        t.Error("SortByKey failed, is not sorted.")
    }
}

func TestAccumulator(t *testing.T) {
    setupEnv()
    c := NewContext("TestAccumulator")
    fmt.Printf("\n\n%s\n", c)
    defer c.Stop()
    d := []interface{}{1, 2, 3, 5, 6, 7, 8}[:]
    accu := c.Accumulator(0)
    c.Data(d).Foreach(func(_ interface{}) {
        accu.Add(1)
    })
    fmt.Println(accu.Value())
    if accu.Value() != 7 {
        t.Error("Accumulator error")
    }
}
