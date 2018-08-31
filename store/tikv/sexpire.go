package tikv

import (
	"time"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
)

//STTL Returns the remaining time to live of a key that has a timeout.
func (tikv *Tikv) STTL(txn interface{}, key []byte) (ttl int64, err error) {
	ttl, err = tikv.SPTTL(txn, key)
	if ttl > 0 {
		ttl = ttl / 1000
	}
	return
}

//SPTTL get the time to live for a key in milliseconds
func (tikv *Tikv) SPTTL(txn interface{}, key []byte) (ttl int64, err error) {
	var (
		dataKey  []byte
		ttlKey   []byte
		value    []byte
		ttlValue []byte
		ts       uint64
	)
	dataKey = utils.EncodeSetKey(key)
	value, err = tikv.Get(txn, dataKey)
	if err != nil {
		return
	}
	if value == nil {
		//not key
		ttl = -2
		return
	}
	//get ttl
	ttlKey = utils.EncodeTTLKey(key, utils.SET_TYPE)
	ttlValue, err = tikv.Get(txn, ttlKey)
	if err != nil {
		return
	}
	if ttlValue == nil {
		// no expire
		ttl = -1
		return
	}
	ts, err = utils.BytesToUint64(ttlValue)
	if err != nil {
		return
	}
	ttl = int64(ts) - time.Now().UnixNano()/1000/1000
	if ttl < 0 {
		ttl = 0
	}
	return
}

//SPExipre This command works exactly like EXPIRE but the time to live of the key is specified in milliseconds instead of seconds.
func (tikv *Tikv) SPExipre(txn interface{}, key []byte, ts int64) (ret int, err error) {
	var (
		v         []byte
		ttlKey    []byte
		ttlValue  []byte
		ttlOld    uint64
		expireKey []byte
		tikv_txn  kv.Transaction
		tsRaw     []byte
		ok        bool
	)
	tikv_txn, ok = txn.(kv.Transaction)
	if !ok {
		err = qkverror.ErrorServerInternal
		return
	}
	v, err = tikv.Get(txn, utils.EncodeSetKey(key))
	if err != nil {
		return
	}
	if v == nil {
		//not key
		return
	}
	//get ttl
	ttlKey = utils.EncodeTTLKey(key, utils.SET_TYPE)
	ttlValue, err = tikv.Get(txn, ttlKey)
	if err != nil {
		return
	}
	//get old ttl
	if ttlValue != nil {
		ttlOld, err = utils.BytesToUint64(ttlValue)
		if err != nil {
			return
		}
		//old expire key
		expireKey = utils.EncodeExpireKey(key, utils.SET_TYPE, int64(ttlOld))
		//delete old expire key
		err = tikv_txn.Delete(expireKey)
		if err != nil {
			return
		}
	}
	//set expire key
	expireKey = utils.EncodeExpireKey(key, utils.SET_TYPE, ts)
	err = tikv_txn.Set(expireKey, []byte{0})
	if err != nil {
		return
	}
	//set ttl key
	tsRaw, _ = utils.Uint64ToBytes(uint64(ts))
	err = tikv_txn.Set(ttlKey, tsRaw)
	if err != nil {
		return
	} else {
		ret = 1
	}
	return
}
