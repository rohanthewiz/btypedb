package btypedb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"github.com/rohanthewiz/serr"
)

// Codec converts values of type T to and from bytes for the write-ahead log.
// Encoding only affects the on-disk representation — in-memory ordering comes
// from the key type's natural (cmp.Ordered) ordering, not from encoded bytes.
type Codec[T any] struct {
	Encode func(T) ([]byte, error)
	Decode func([]byte) (T, error)
}

// StringCodec encodes strings as their raw bytes.
var StringCodec = Codec[string]{
	Encode: func(s string) ([]byte, error) { return []byte(s), nil },
	Decode: func(b []byte) (string, error) { return string(b), nil },
}

// BytesCodec passes byte slices through, cloning on decode so callers
// never alias the replay buffer.
var BytesCodec = Codec[[]byte]{
	Encode: func(b []byte) ([]byte, error) { return b, nil },
	Decode: func(b []byte) ([]byte, error) { return bytes.Clone(b), nil },
}

// Int64Codec encodes int64s as 8 fixed bytes (little-endian).
var Int64Codec = Codec[int64]{
	Encode: func(n int64) ([]byte, error) {
		return binary.LittleEndian.AppendUint64(nil, uint64(n)), nil
	},
	Decode: func(b []byte) (int64, error) {
		if len(b) != 8 {
			return 0, serr.New("invalid int64 encoding", "len", itoa(len(b)))
		}
		return int64(binary.LittleEndian.Uint64(b)), nil
	},
}

// Uint64Codec encodes uint64s as 8 fixed bytes (little-endian).
var Uint64Codec = Codec[uint64]{
	Encode: func(n uint64) ([]byte, error) {
		return binary.LittleEndian.AppendUint64(nil, n), nil
	},
	Decode: func(b []byte) (uint64, error) {
		if len(b) != 8 {
			return 0, serr.New("invalid uint64 encoding", "len", itoa(len(b)))
		}
		return binary.LittleEndian.Uint64(b), nil
	},
}

// JSONCodec encodes any type via encoding/json. Handy for struct values.
func JSONCodec[T any]() Codec[T] {
	return Codec[T]{
		Encode: func(v T) ([]byte, error) { return json.Marshal(v) },
		Decode: func(b []byte) (T, error) {
			var v T
			if err := json.Unmarshal(b, &v); err != nil {
				return v, serr.Wrap(err, "codec", "json")
			}
			return v, nil
		},
	}
}
