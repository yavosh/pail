package checksum

import (
	"encoding/binary"
	"hash"
	"math/bits"
)

const (
	prime1 uint64 = 11400714785074694791
	prime2 uint64 = 14029467366897019727
	prime3 uint64 = 1609587929392839161
	prime4 uint64 = 9650029242287828579
	prime5 uint64 = 2870177450012600261
)

// xxh64 is XXH64 with seed 0. Sum returns the big-endian digest, as S3 does.
type xxh64 struct {
	v     [4]uint64
	mem   [32]byte
	n     int // bytes buffered in mem
	total uint64
}

func newXXH64() hash.Hash {
	d := &xxh64{}
	d.Reset()
	return d
}

func (d *xxh64) Size() int      { return 8 }
func (d *xxh64) BlockSize() int { return 32 }

func (d *xxh64) Reset() {
	p1, p2 := prime1, prime2 // variables, so the sums wrap instead of overflowing constants
	d.v = [4]uint64{p1 + p2, p2, 0, -p1}
	d.n, d.total = 0, 0
}

func round(acc, in uint64) uint64 {
	return bits.RotateLeft64(acc+in*prime2, 31) * prime1
}

func mergeRound(acc, val uint64) uint64 {
	return (acc^round(0, val))*prime1 + prime4
}

func (d *xxh64) stripe(b []byte) {
	for i := range d.v {
		d.v[i] = round(d.v[i], binary.LittleEndian.Uint64(b[8*i:]))
	}
}

func (d *xxh64) Write(p []byte) (int, error) {
	n := len(p)
	d.total += uint64(n)
	if d.n+len(p) < len(d.mem) {
		d.n += copy(d.mem[d.n:], p)
		return n, nil
	}
	if d.n > 0 {
		p = p[copy(d.mem[d.n:], p):]
		d.stripe(d.mem[:])
		d.n = 0
	}
	for len(p) >= len(d.mem) {
		d.stripe(p)
		p = p[len(d.mem):]
	}
	d.n = copy(d.mem[:], p)
	return n, nil
}

func (d *xxh64) Sum(b []byte) []byte {
	var h uint64
	if d.total >= uint64(len(d.mem)) {
		v := d.v
		h = bits.RotateLeft64(v[0], 1) + bits.RotateLeft64(v[1], 7) + bits.RotateLeft64(v[2], 12) + bits.RotateLeft64(v[3], 18)
		for _, x := range v {
			h = mergeRound(h, x)
		}
	} else {
		h = d.v[2] + prime5
	}
	h += d.total
	tail := d.mem[:d.n]
	for ; len(tail) >= 8; tail = tail[8:] {
		h ^= round(0, binary.LittleEndian.Uint64(tail))
		h = bits.RotateLeft64(h, 27)*prime1 + prime4
	}
	if len(tail) >= 4 {
		h ^= uint64(binary.LittleEndian.Uint32(tail)) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		tail = tail[4:]
	}
	for _, c := range tail {
		h ^= uint64(c) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
	}
	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32
	return binary.BigEndian.AppendUint64(b, h)
}
