package utils

import (
	"math"

	"github.com/chuangyou/qkv/qkverror"
)

var (
	SCORE_MIN int64 = math.MinInt64 + 2
	SCORE_MAX int64 = math.MaxInt64 - 1
)

func EncodeData(dataType byte, rawData []byte) (buf []byte) {
	buf = make([]byte, len(rawData)+1)
	buf[0] = dataType
	copy(buf[1:], rawData)
	return
}

func DecodeData(rawData []byte) (dataType byte, data []byte, err error) {
	if len(rawData) < 1 {
		err = qkverror.ErrorInvalidRawData
		return
	}
	dataType = rawData[0]
	data = rawData[1:]
	return
}

// type(ttl)|key, value is unix timestamp(ms)
func EncodeTTLKey(key []byte) []byte {
	buf := make([]byte, len(key)+1)
	buf[0] = TTL_TYPE
	copy(buf[1:], key)
	return buf
}

// type(ttl)|timestamp(8 bytes)|key
func EncodeExpireKey(key []byte, ts int64) []byte {
	var (
		tsRaw []byte
	)
	buf := make([]byte, len(key)+9)
	buf[0] = EXPTIME_TYPE
	tsRaw, _ = Uint64ToBytes(uint64(ts))
	copy(buf[1:], tsRaw)
	copy(buf[9:], key)
	return buf
}
func DecodeExpireKey(key []byte) ([]byte, uint64, error) {
	if len(key) < 9 || key[0] != EXPTIME_TYPE {
		return nil, 0, qkverror.ErrorTypeNotMatch
	}

	ts, err := BytesToUint64(key[1:])
	if err != nil {
		return nil, 0, err
	}

	return key[9:], ts, nil
}
