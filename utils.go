package gopark

import (
    "bufio"
    "bytes"
    "encoding/gob"
    "hash/fnv"
    "log"
    "os"
    "strconv"
    "sync/atomic"
)

func Range(start, end int) []interface{} {
    if start > end {
        log.Panicf("Range start cannot be larger than end, [%d, %d)", start, end)
    }
    r := make([]interface{}, end-start)
    for i := start; i < end; i++ {
        r[i-start] = i
    }
    return r
}

type AtomicInt int64

func (i *AtomicInt) Add(n int64) {
    atomic.AddInt64((*int64)(i), n)
}

func (i *AtomicInt) Get() int64 {
    return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
    return strconv.FormatInt(i.Get(), 10)
}

func MaxInt64(a, b int64) int64 {
    if a > b {
        return a
    }
    return b
}

func MinInt64(a, b int64) int64 {
    if a > b {
        return b
    }
    return a
}

func readLine(r *bufio.Reader) (string, error) {
    var (
        isPrefix bool  = true
        err      error = nil
        line, ln []byte
    )
    for isPrefix && err == nil {
        line, isPrefix, err = r.ReadLine()
        ln = append(ln, line...)
    }
    return string(ln), err
}

func seekToNewLine(f *os.File, start int64) (int64, error) {
    _, err := f.Seek(start, 0)
    if err != nil {
        return start, err
    }
    b := make([]byte, 1)
    _, err = f.Read(b)
    start++
    if err != nil {
        return start, err
    }
    for b[0] != '\n' {
        _, err := f.Read(b)
        if err != nil {
            return start, err
        }
        start++
    }
    return start, nil
}

func hashCode(value interface{}) (hashCode int64) {
    if value == nil {
        hashCode = 0
        return
    }
    hash := fnv.New32()
    buffer := new(bytes.Buffer)
    encoder := gob.NewEncoder(buffer)
    encoder.Encode(value)
    hash.Write(buffer.Bytes())
    hashCode = 0
    hashBytes := hash.Sum(nil)
    for _, hashByte := range hashBytes {
        hashCode = hashCode*256 + int64(hashByte)
    }
    return
}
