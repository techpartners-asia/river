// Package workflowid generates 26-character Crockford-base32 ULIDs for
// workflow IDs. The format follows the ULID spec: 48-bit Unix-ms timestamp
// followed by 80 bits of randomness, encoded as Crockford-base32. IDs
// generated within the same millisecond are monotonically incremented from
// the previous random tail to preserve lexicographic ordering.
package workflowid

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"
)

const crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var (
	mu      sync.Mutex
	lastMs  uint64
	lastRnd [10]byte
)

// New returns a new ULID-shaped workflow ID.
func New() string {
	mu.Lock()
	defer mu.Unlock()

	ms := uint64(time.Now().UnixMilli()) //nolint:gosec
	if ms == lastMs {
		incBytes(lastRnd[:])
	} else {
		if _, err := rand.Read(lastRnd[:]); err != nil {
			panic("workflowid: rand.Read: " + err.Error())
		}
		lastMs = ms
	}

	var raw [16]byte
	binary.BigEndian.PutUint64(raw[0:8], ms<<16)
	copy(raw[6:], lastRnd[:])

	return encode(raw)
}

func incBytes(b []byte) {
	for i := len(b) - 1; i >= 0; i-- {
		b[i]++
		if b[i] != 0 {
			return
		}
	}
}

func encode(raw [16]byte) string {
	// 16 bytes = 128 bits → 26 base32 chars carrying 130 bits, with two
	// leading zero bits of padding.
	out := make([]byte, 26)
	out[0] = crockfordAlphabet[(raw[0]&224)>>5]
	out[1] = crockfordAlphabet[raw[0]&31]
	out[2] = crockfordAlphabet[(raw[1]&248)>>3]
	out[3] = crockfordAlphabet[((raw[1]&7)<<2)|((raw[2]&192)>>6)]
	out[4] = crockfordAlphabet[(raw[2]&62)>>1]
	out[5] = crockfordAlphabet[((raw[2]&1)<<4)|((raw[3]&240)>>4)]
	out[6] = crockfordAlphabet[((raw[3]&15)<<1)|((raw[4]&128)>>7)]
	out[7] = crockfordAlphabet[(raw[4]&124)>>2]
	out[8] = crockfordAlphabet[((raw[4]&3)<<3)|((raw[5]&224)>>5)]
	out[9] = crockfordAlphabet[raw[5]&31]
	out[10] = crockfordAlphabet[(raw[6]&248)>>3]
	out[11] = crockfordAlphabet[((raw[6]&7)<<2)|((raw[7]&192)>>6)]
	out[12] = crockfordAlphabet[(raw[7]&62)>>1]
	out[13] = crockfordAlphabet[((raw[7]&1)<<4)|((raw[8]&240)>>4)]
	out[14] = crockfordAlphabet[((raw[8]&15)<<1)|((raw[9]&128)>>7)]
	out[15] = crockfordAlphabet[(raw[9]&124)>>2]
	out[16] = crockfordAlphabet[((raw[9]&3)<<3)|((raw[10]&224)>>5)]
	out[17] = crockfordAlphabet[raw[10]&31]
	out[18] = crockfordAlphabet[(raw[11]&248)>>3]
	out[19] = crockfordAlphabet[((raw[11]&7)<<2)|((raw[12]&192)>>6)]
	out[20] = crockfordAlphabet[(raw[12]&62)>>1]
	out[21] = crockfordAlphabet[((raw[12]&1)<<4)|((raw[13]&240)>>4)]
	out[22] = crockfordAlphabet[((raw[13]&15)<<1)|((raw[14]&128)>>7)]
	out[23] = crockfordAlphabet[(raw[14]&124)>>2]
	out[24] = crockfordAlphabet[((raw[14]&3)<<3)|((raw[15]&224)>>5)]
	out[25] = crockfordAlphabet[raw[15]&31]
	return string(out)
}
