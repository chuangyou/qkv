package tikv

import (
	"context"
	"time"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
	log "github.com/sirupsen/logrus"
)

//ClearExpire clear ttl key,expite key
func (tikv *Tikv) ClearExpire(txn interface{}, key []byte) (err error) {
	var (
		ttl int64
	)
	ttl, err = tikv.TTL(txn, key)
	if err != nil {
		return
	}
	if ttl < 0 {
		// The command returns -1 if the key exists but has no associated expire.
		// The command returns -2 if the key does not exist.
		// The command returns 0 the key  was expire
		// >0: ttl value
		return
	}
	log.Debugf("clear expire key: %s", key)
	return tikv.removeMetaKey(txn, key)

}

//removeMetaKey remove expire meta key
func (tikv *Tikv) removeMetaKey(txn interface{}, key []byte) (err error) {
	var (
		ttlKey    []byte
		expireKey []byte
		value     []byte
		ts        uint64
		ok        bool
		tikv_txn  kv.Transaction
	)
	if txn == nil {
		err = qkverror.ErrorServerInternal
		return
	}
	tikv_txn, ok = txn.(kv.Transaction)
	if !ok {
		err = qkverror.ErrorServerInternal
		return
	}
	ttlKey = utils.EncodeTTLKey(key, utils.STRING_TYPE)
	if err != nil {
		return
	}
	value, err = tikv.Get(txn, ttlKey)
	if value == nil {
		return
	}
	ts, err = utils.BytesToUint64(value)
	if err != nil {
		return
	}
	expireKey = utils.EncodeExpireKey(key, utils.STRING_TYPE, int64(ts))
	if err = tikv_txn.Delete(ttlKey); err != nil {
		return
	}
	if err = tikv_txn.Delete(expireKey); err != nil {
		return
	}
	return

}

//PTTL get the time to live for a key in milliseconds
func (tikv *Tikv) PTTL(txn interface{}, key []byte) (ttl int64, err error) {
	var (
		dataKey  []byte
		ttlKey   []byte
		value    []byte
		ttlValue []byte
		ts       uint64
	)
	dataKey = utils.EncodeStringKey(key)
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
	ttlKey = utils.EncodeTTLKey(key, utils.STRING_TYPE)
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

//TTL Returns the remaining time to live of a key that has a timeout.
func (tikv *Tikv) TTL(txn interface{}, key []byte) (ttl int64, err error) {
	ttl, err = tikv.PTTL(txn, key)
	if ttl > 0 {
		ttl = ttl / 1000
	}
	return
}

//SetEX set key to hold the string value and set key to timeout after a given number of seconds
func (tikv *Tikv) SetEX(txn interface{}, key []byte, seconds int64, value []byte) (err error) {
	var (
		tikv_txn   kv.Transaction
		ok         bool
		expireTime int64
	)
	expireTime = seconds*1000 + (time.Now().UnixNano() / 1000 / 1000)
	if txn != nil {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		//set key -> value
		err = tikv_txn.Set(utils.EncodeStringKey(key), value)
		if err != nil {
			return
		}
		//expire with txn(ms)
		_, err = tikv.PExipre(txn, key, expireTime)
	} else {
		txn, err = tikv.NewTxn()
		if err != nil {
			err = qkverror.ErrorServerInternal
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
		//expire with txn(ms)
		err = tikv_txn.Set(utils.EncodeStringKey(key), value)
		if err != nil {
			return
		}
		_, err = tikv.PExipre(tikv_txn, key, expireTime)
		if err == nil {
			err = tikv_txn.Commit(context.Background())
		}
	}
	return

}

//PExipre This command works exactly like EXPIRE but the time to live of the key is specified in milliseconds instead of seconds.
func (tikv *Tikv) PExipre(txn interface{}, key []byte, ts int64) (ret int, err error) {
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
	v, err = tikv.Get(txn, utils.EncodeStringKey(key))
	if err != nil {
		return
	}
	if v == nil {
		//not key
		return
	}
	//get ttl
	ttlKey = utils.EncodeTTLKey(key, utils.STRING_TYPE)
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
		expireKey = utils.EncodeExpireKey(key, utils.STRING_TYPE, int64(ttlOld))
		//delete old expire key
		err = tikv_txn.Delete(expireKey)
		if err != nil {
			return
		}
	}
	//set expire key
	expireKey = utils.EncodeExpireKey(key, utils.STRING_TYPE, ts)
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
