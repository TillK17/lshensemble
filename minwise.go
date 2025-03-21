package lshensemble

import "math"

// MinWise is a collection of minimum hashes for a set
type MinWise struct {
	minimums []uint32
	h1       Hash32
	h2       Hash32
}

type Hash32 func([]byte) uint32

// NewMinWise returns a new MinWise Hashing implementation
func NewMinWise(h1, h2 Hash32, size int) *MinWise {

	minimums := make([]uint32, size)
	for i := range minimums {
		minimums[i] = math.MaxUint32
	}

	return &MinWise{
		h1:       h1,
		h2:       h2,
		minimums: minimums,
	}
}

// NewMinWiseFromSignatures returns a new MinWise Hashing implementation
// using a user-provided set of signatures
func NewMinWiseFromSignatures(h1, h2 Hash32, signatures []uint32) *MinWise {

	minimums := make([]uint32, len(signatures))
	copy(minimums, signatures)
	return &MinWise{
		h1:       h1,
		h2:       h2,
		minimums: signatures,
	}
}

// Push adds an element to the set.
func (m *MinWise) Push(b []byte) {

	v1 := m.h1(b)
	v2 := m.h2(b)

	for i, v := range m.minimums {
		hv := v1 + uint32(i)*v2
		if hv < v {
			m.minimums[i] = hv
		}
	}
}

// Merge combines the signatures of the second set, creating the signature of their union.
func (m *MinWise) Merge(m2 *MinWise) {

	for i, v := range m2.minimums {

		if v < m.minimums[i] {
			m.minimums[i] = v
		}
	}
}

// Cardinality estimates the cardinality of the set
func (m *MinWise) Cardinality() int {

	// http://www.cohenwang.com/edith/Papers/tcest.pdf

	sum := 0.0

	for _, v := range m.minimums {
		sum += -math.Log(float64(math.MaxUint32-v) / float64(math.MaxUint32))
	}

	return int(float64(len(m.minimums)-1) / sum)
}

// Signature returns a signature for the set.
func (m *MinWise) Signature() []uint32 {
	return m.minimums
}

// Similarity computes an estimate for the similarity between the two sets.
func (m *MinWise) Similarity(m2 *MinWise) float32 {

	if len(m.minimums) != len(m2.minimums) {
		panic("minhash minimums size mismatch")
	}

	intersect := 0

	for i := range m.minimums {
		if m.minimums[i] == m2.minimums[i] {
			intersect++
		}
	}

	return float32(intersect) / float32(len(m.minimums))
}

// SignatureBbit returns a b-bit reduction of the signature.  This will result in unused bits at the high-end of the words if b does not divide 64 evenly.
func (m *MinWise) SignatureBbit(b uint) []uint32 {

	var sig []uint32 // full signature
	var w uint32     // current word
	bits := uint(32) // bits free in current word

	mask := uint32(1<<b) - 1

	for _, v := range m.minimums {
		if bits >= b {
			w <<= b
			w |= v & mask
			bits -= b
		} else {
			sig = append(sig, w)
			w = 0
			bits = 32
		}
	}

	if bits != 32 {
		sig = append(sig, w)
	}

	return sig
}

// SimilarityBbit computes an estimate for the similarity between two b-bit signatures
func SimilarityBbit(sig1, sig2 []uint32, b uint) float32 {

	if len(sig1) != len(sig2) {
		panic("signature size mismatch")
	}

	intersect := 0
	count := 0

	mask := uint32(1<<b) - 1

	for i := range sig1 {
		w1 := sig1[i]
		w2 := sig2[i]

		bits := uint(32)

		for bits >= b {
			v1 := (w1 & mask)
			v2 := (w2 & mask)

			count++
			if v1 == v2 {
				intersect++
			}

			bits -= b
			w1 >>= b
			w2 >>= b
		}
	}

	return float32(intersect) / float32(count)
}
