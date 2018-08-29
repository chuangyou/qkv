package utils

import (
	"github.com/chuangyou/qkv/qkverror"
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
	buf[0] = SET_TYPE //SET_TYPE 1 byte
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
	if rawkey[0] != SET_TYPE {
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
func EncodeSetKey(key []byte) (buf []byte) {
	buf = make([]byte, len(key)+1)
	buf[0] = SET_TYPE
	copy(buf[1:], key)
	return buf
}
