package lshensemble

import (
	"bytes"
	"encoding/binary"

	"math/rand"

	mm3 "github.com/spaolacci/murmur3"
)

// HashValueSize is 8, the number of byte used for each hash value
const HashValueSize = 4

// Minhash represents a MinHash object
type Minhash struct {
	mw *MinWise
}

// NewMinhash initializes a MinHash object with a seed and the number of
// hash functions.
func NewMinhash(seed int64, numHash int) *Minhash {
	r := rand.New(rand.NewSource(seed))
	b := binary.BigEndian
	b1 := make([]byte, HashValueSize)
	b2 := make([]byte, HashValueSize)
	b.PutUint32(b1, uint32(r.Int31()))
	b.PutUint32(b2, uint32(r.Int31()))
	mm1 := mm3.New32()
	mm2 := mm3.New32()
	h1 := func(b []byte) uint32 {
		mm1.Reset()
		mm1.Write(b1)
		mm1.Write(b)
		return mm1.Sum32()
	}
	h2 := func(b []byte) uint32 {
		mm2.Reset()
		mm2.Write(b2)
		mm2.Write(b)
		return mm2.Sum32()
	}
	return &Minhash{NewMinWise(h1, h2, numHash)}
}

// Push a new value to the MinHash object.
// The value should be serialized to byte slice.
func (m *Minhash) Push(b []byte) {
	m.mw.Push(b)
}

// Signature exports the MinHash signature.
func (m *Minhash) Signature() []uint32 {
	return m.mw.Signature()
}

// Containment returns the estimated containment of
// |Q \intersect X| / |Q|.
// q and x are the signatures of Q and X respectively.
// If either size is 0, the result is defined to be 0.
func Containment(q, x []uint32, qSize, xSize int) float32 {
	if qSize == 0 || xSize == 0 {
		return 0.0
	}
	var eq int
	for i, hv := range q {
		if x[i] == hv {
			eq++
		}
	}
	jaccard := float32(eq) / float32(len(q))
	c := (float32(xSize)/float32(qSize) + 1.0) * jaccard / (1.0 + jaccard)
	if c > 1.0 {
		return 1.0
	}
	return c
}

// SigToBytes serializes the signature into byte slice
func SigToBytes(sig []uint32) []byte {
	buf := new(bytes.Buffer)
	for _, v := range sig {
		binary.Write(buf, binary.BigEndian, v)
	}
	return buf.Bytes()
}

// BytesToSig converts a byte slice into a signature
func BytesToSig(data []byte) ([]uint32, error) {
	size := len(data) / HashValueSize
	sig := make([]uint32, size)
	buf := bytes.NewReader(data)
	var v uint32
	for i := range sig {
		if err := binary.Read(buf, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		sig[i] = v
	}
	return sig, nil
}
