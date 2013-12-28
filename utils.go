package gopark

import (
    "bufio"
    "bytes"
    "encoding/binary"
    "encoding/gob"
    "fmt"
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

var _ = fmt.Println

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

func HashCode(value interface{}) int64 {
    return hashCode(value)
}

// Encode related funcs

const ENCODE_BUFFER_SIZE = 10000

type EncodeBox struct {
    Object interface{}
}

func init() {
    gob.Register(new(EncodeBox))
    gob.Register(make([]interface{}, 0))
}

type BufferEncoder struct {
    size      int
    buffer    []interface{}
    watermark int
}

func NewBufferEncoder(size int) *BufferEncoder {
    encoder := &BufferEncoder{}
    encoder.size = size
    encoder.buffer = make([]interface{}, size)
    return encoder
}

func (e *BufferEncoder) Encode(f *os.File, value interface{}) error {
    if len(e.buffer) == e.watermark {
        err := encodeObjectIntoFile(f, e.buffer)
        if err != nil {
            return err
        }
        e.watermark = 0
    }
    e.buffer[e.watermark] = value
    e.watermark++
    return nil
}

func (e *BufferEncoder) Flush(f *os.File) error {
    if e.watermark > 0 {
        err := encodeObjectIntoFile(f, e.buffer[:e.watermark])
        if err != nil {
            return err
        }
        e.watermark = 0
    }
    return nil
}

func (e *BufferEncoder) Decode(f *os.File) ([]interface{}, error) {
    buffer, err := decodeObjectFromFile(f)
    if err != nil {
        return nil, err
    }
    return buffer.([]interface{}), nil
}

func encodeObjectIntoFile(f *os.File, value interface{}) error {
    box := EncodeBox{value}
    objBuffer := new(bytes.Buffer)
    err := gob.NewEncoder(objBuffer).Encode(box)
    if err != nil {
        return err
    }

    size := int32(len(objBuffer.Bytes()))
    sizeBuffer := new(bytes.Buffer)
    err = binary.Write(sizeBuffer, binary.LittleEndian, size)
    if err != nil {
        return err
    }

    f.Write(sizeBuffer.Bytes())
    f.Write(objBuffer.Bytes())

    return err
}

func decodeObjectFromFile(f *os.File) (interface{}, error) {
    var (
        size int32
        box  EncodeBox
    )
    err := binary.Read(f, binary.LittleEndian, &size)
    if err != nil {
        return nil, err
    }

    boxBytes := make([]byte, size)
    _, err = f.Read(boxBytes)
    if err != nil {
        return nil, err
    }

    buffer := bytes.NewBuffer(boxBytes)
    decoder := gob.NewDecoder(buffer)
    if err = decoder.Decode(&box); err == nil {
        return box.Object, nil
    }
    return nil, err
}
