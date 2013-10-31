package gopark

import (
    "bufio"
    "bytes"
    "encoding/gob"
    "hash/fnv"
    "os"
    "runtime"
    "strconv"
    "sync/atomic"
    "unsafe"
)

type WeakRef struct {
    t   uintptr // Type
    d   uintptr // Data
}

func NewWeakRef(v interface{}) *WeakRef {
    i := (*[2]uintptr)(unsafe.Pointer(&v))
    w := &WeakRef{^i[0], ^i[1]}
    runtime.SetFinalizer((*uintptr)(unsafe.Pointer(i[1])), func(_ *uintptr) {
        atomic.StoreUintptr(&w.d, 0)
        w.t = 0
    })
    return w
}

func (w *WeakRef) Get() (v interface{}) {
    t := w.t
    d := atomic.LoadUintptr(&w.d)
    if d != 0 {
        i := (*[2]uintptr)(unsafe.Pointer(&v))
        i[0] = ^t
        i[1] = ^d
    }
    return
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
