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

// type|len(key)|key|len(member)|member
func DecodeZSetData(rawKey []byte) (key []byte, member []byte, err error) {
	var (
		pos       int = 0
		keyLen    uint16
		memberLen uint16
	)

	if rawKey[pos] != ZSET_DATA {
		err = qkverror.ErrorTypeNotMatch
		return
	}
	pos++

	keyLen, _ = BytesToUint16(rawKey[pos:])
	pos = pos + 2

	key = rawKey[pos : pos+int(keyLen)]
	pos = pos + int(keyLen)

	memberLen, _ = BytesToUint16(rawKey[pos:])
	pos = pos + 2

	member = rawKey[pos : pos+int(memberLen)]

	return
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
func ZScoreOffset(score int64) uint64 {
	return uint64(score + SCORE_MAX)
}
func ZScoreRestore(rscore uint64) int64 {
	return int64(rscore - uint64(SCORE_MAX))
}

// type(1)|keylen(2)|key|field
func EncodeHashData(key, field []byte) (buf []byte) {
	var (
		pos = 0
	)
	buf = make([]byte, 1+2+len(key)+len(field))
	buf[0] = HASH_DATA
	pos++
	Uint16ToBytesExt(buf[pos:], uint16(len(key)))
	pos = pos + 2
	copy(buf[pos:], key)
	pos = pos + len(key)
	copy(buf[pos:], field)
	return buf
}

// type(1)|keylen(2)|key|field
func DecodeHashData(rawkey []byte) (key []byte, field []byte, err error) {
	var (
		pos    uint16 = 0
		keyLen uint16
	)

	if rawkey[0] != HASH_DATA {
		err = qkverror.ErrorTypeNotMatch
		return
	}
	pos++
	keyLen, _ = BytesToUint16(rawkey[pos:])
	pos = pos + 2
	key = rawkey[pos : pos+keyLen]
	pos = pos + keyLen
	field = rawkey[pos:]
	return
}
