package utils

import (
	"math"

	"github.com/chuangyou/qkv/qkverror"
)

var (
	SCORE_MIN int64 = math.MinInt64 + 2
	SCORE_MAX int64 = math.MaxInt64 - 1
)

func EncodeStringKey(key []byte) []byte {
	buf := make([]byte, len(key)+1)
	pos := 0
	buf[pos] = STRING_TYPE
	pos++
	copy(buf[pos:], key)
	return buf
}

// type(ttl)|type(key_type)|timestamp(8 bytes)|key
func EncodeExpireKey(key []byte, keyType byte, ts int64) []byte {
	var (
		tsRaw []byte
	)
	buf := make([]byte, len(key)+10)
	buf[0] = EXPTIME_TYPE
	buf[1] = keyType
	tsRaw, _ = Uint64ToBytes(uint64(ts))
	copy(buf[2:], tsRaw)
	copy(buf[10:], key)
	return buf
}
func DecodeExpireKey(key []byte, keyType byte) ([]byte, uint64, error) {
	if len(key) < 10 || key[0] != EXPTIME_TYPE || key[1] != keyType {
		return nil, 0, qkverror.ErrorTypeNotMatch
	}

	ts, err := BytesToUint64(key[2:])
	if err != nil {
		return nil, 0, err
	}

	return key[10:], ts, nil
}

// type(ttl)|type(key_type)|key, value is unix timestamp(ms)
func EncodeTTLKey(key []byte, keyType byte) []byte {
	buf := make([]byte, len(key)+2)
	buf[0] = TTL_TYPE
	buf[1] = keyType
	copy(buf[2:], key)
	return buf
}

//type(set)|len(key)|key|member
func EncodeSetData(key, member []byte) (buf []byte) {
	var (
		bufSize int
		pos     int = 0
	)
	bufSize = 1 + 2 + len(key) + len(member)
	buf = make([]byte, bufSize)
	buf[0] = SET_DATA //SET_DATA 1 byte
	pos++

	Uint16ToBytesExt(buf[pos:], uint16(len(key)))
	pos = pos + 2 //2 bytes
	copy(buf[pos:], key)

	pos = pos + len(key)
	copy(buf[pos:], member)

	return
}

//type(set)|len(key)|key|member
func DecodeSetData(rawkey []byte) (key []byte, field []byte, err error) {
	var (
		pos       uint16 = 0
		keyLength uint16
	)
	if rawkey[0] != SET_DATA {
		err = qkverror.ErrorTypeNotMatch
		return
	}
	pos++
	keyLength, _ = BytesToUint16(rawkey[pos:])
	pos = pos + 2

	key = rawkey[pos : pos+keyLength]
	pos = pos + keyLength

	field = rawkey[pos:]

	return
}

//EncodeSetKey encode set struct key
func EncodeSetKey(key []byte) (buf []byte) {
	buf = make([]byte, len(key)+1)
	buf[0] = SET_TYPE
	copy(buf[1:], key)
	return buf
}

//sorted set = type|key
func EncodeZSetKey(key []byte) []byte {
	buf := make([]byte, len(key)+1)
	buf[0] = ZSET_TYPE
	copy(buf[1:], key)
	return buf
}

// type|len(key)|key|len(member)|member
func EncodeZSetData(key, member []byte) (buf []byte) {
	var (
		pos int = 0
	)

	buf = make([]byte, 1+4+len(key)+len(member))
	buf[pos] = ZSET_DATA
	pos++

	Uint16ToBytesExt(buf[pos:], uint16(len(key)))
	pos = pos + 2

	copy(buf[pos:], key)
	pos = pos + len(key)

	Uint16ToBytesExt(buf[pos:], uint16(len(member)))
	pos = pos + 2

	copy(buf[pos:], member)

	return
}

func EncodeZSetDataEnd(key []byte) (buf []byte) {
	var (
		pos int = 0
	)

	buf = make([]byte, 1+4+len(key))
	buf[pos] = ZSET_DATA
	pos++

	Uint16ToBytesExt(buf[pos:], uint16(len(key)))
	pos = pos + 2

	copy(buf[pos:], key)
	pos = pos + len(key)
	a := -1
	Uint16ToBytesExt(buf[pos:], uint16(a))
	pos = pos + 2

	return buf
}

// type|len(key)|key|score|member
func EncodeZSetScore(key, member []byte, score int64) (buf []byte) {
	var (
		pos int = 0
	)

	buf = make([]byte, 1+2+len(key)+8+len(member))
	buf[pos] = ZSET_SCORE
	pos++

	Uint16ToBytesExt(buf[pos:], uint16(len(key)))
	pos = pos + 2

	copy(buf[pos:], key)
	pos = pos + len(key)

	Uint64ToBytesExt(buf[pos:], ZScoreOffset(score))
	pos = pos + 8

	copy(buf[pos:], member)

	return
}

// type|len(key)|key|score|member
func DecodeZSetScore(rawkey []byte) (key []byte, member []byte, score int64, err error) {
	var (
		pos       int = 0
		keyLen    uint16
		tempScore uint64
	)

	if rawkey[pos] != ZSET_SCORE {
		err = qkverror.ErrorTypeNotMatch
		return
	}
	pos++

	keyLen, _ = BytesToUint16(rawkey[pos:])
	pos = pos + 2

	key = rawkey[pos : pos+int(keyLen)]
	pos = pos + int(keyLen)

	tempScore, _ = BytesToUint64(rawkey[pos:])
	score = ZScoreRestore(tempScore)
	pos = pos + 8

	member = rawkey[pos:]

	return
}

func ZScoreOffset(score int64) uint64 {
	return uint64(score + SCORE_MAX)
}
func ZScoreRestore(rscore uint64) int64 {
	return int64(rscore - uint64(SCORE_MAX))
}
