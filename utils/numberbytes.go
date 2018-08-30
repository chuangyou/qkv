package utils

import (
	"encoding/binary"
	"errors"
	"strconv"
)

var (
	ErrParams = errors.New("params error")
)

func StrBytesToInt64(n []byte) (i int64, err error) {
	if n == nil {
		err = ErrParams
		return
	}
	i, err = strconv.ParseInt(string(n), 10, 64)
	return
}
func BytesToInt64(n []byte) (int64, error) {
	if n == nil || len(n) < 8 {
		return 0, ErrParams
	}

	return int64(binary.BigEndian.Uint64(n)), nil
}
func BytesToUint64(n []byte) (b uint64, err error) {
	if n == nil || len(n) < 8 {
		err = ErrParams
		return
	}

	b = binary.BigEndian.Uint64(n)
	return
}
func Uint64ToBytes(n uint64) (b []byte, err error) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return
}
func Uint64ToBytesExt(dst []byte, n uint64) error {
	if len(dst) < 8 {
		return ErrParams
	}
	binary.BigEndian.PutUint64(dst, n)
	return nil
}
func Int64ToBytes(n int64) ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))

	return b, nil
}
func Int64ToStrBytes(n int64) ([]byte, error) {
	return strconv.AppendInt(nil, n, 10), nil
}
func Uint16ToBytes(src uint16) ([]byte, error) {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, src)
	return buf, nil
}
func BytesToUint16(n []byte) (uint16, error) {
	if n == nil || len(n) < 2 {
		return 0, ErrParams
	}

	return binary.BigEndian.Uint16(n), nil
}
func Uint16ToBytesExt(dst []byte, n uint16) error {
	if len(dst) < 2 {
		return ErrParams
	}
	binary.BigEndian.PutUint16(dst, n)
	return nil
}
