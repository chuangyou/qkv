package tidis

import (
	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
)

func (tidis *Tidis) getHashMeta(txn interface{}, key []byte) (dataType byte, ssize uint64, ttl uint64, flag byte, err error) {
	var (
		rawData []byte
		value   []byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	rawData, err = tidis.db.Get(txn, key)
	if err != nil {
		return
	}
	if rawData == nil {
		flag = utils.FLAG_NORMAL
		return
	}
	dataType, value, err = utils.DecodeData(rawData)
	if err != nil {
		return
	}
	if value == nil {
		flag = utils.FLAG_NORMAL
		return
	}
	if len(value) < 16 {
		err = qkverror.ErrorInvalidMeta
		return
	}
	if ssize, err = utils.BytesToUint64(value[0:]); err != nil {
		err = qkverror.ErrorInvalidMeta
		return
	}
	if ttl, err = utils.BytesToUint64(value[8:]); err != nil {
		err = qkverror.ErrorInvalidMeta
		return
	}
	if len(value) == 17 {
		flag = value[16]
	}
	return
}
func (tidis *Tidis) createHashMeta(size, ttl uint64, flag byte) (buf []byte) {
	buf = make([]byte, 17)
	utils.Uint64ToBytesExt(buf[0:], size)
	utils.Uint64ToBytesExt(buf[8:], ttl)
	buf[16] = flag
	return
}
