package tidis

import (
	"context"
	"time"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
)

func (tidis *Tidis) DeleteIfExpired(txn interface{}, key []byte, delValue bool) (err error) {
	var (
		ttl            int64
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		keys           [][]byte
	)
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	ttl, err = tidis.TTL(txn, key)
	if err != nil {
		return
	}
	if ttl != 0 {
		return nil
	}

	//delete expire key
	err = tidis.removeMetaKey(txn, key)
	if err != nil {
		return
	}
	//delete data
	if delValue {
		keys = append(keys, key)
		_, err = tidis.DeleteWithTxn(txn, keys)

	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//TTL Returns the remaining time to live of a key that has a timeout.
func (tidis *Tidis) TTL(txn interface{}, key []byte) (ttl int64, err error) {
	ttl, err = tidis.PTTL(txn, key)
	if ttl > 0 {
		ttl = ttl / 1000
	}
	return
}

//PTTL get the time to live for a key in milliseconds
func (tidis *Tidis) PTTL(txn interface{}, key []byte) (ttl int64, err error) {
	var (
		ttlKey   []byte
		value    []byte
		ttlValue []byte
		ts       uint64
	)
	value, err = tidis.db.Get(txn, key)
	if err != nil {
		return
	}
	if value == nil {
		//not key
		ttl = -2
		return
	}
	//encode ttl key
	ttlKey = utils.EncodeTTLKey(key)
	//get ttl value
	ttlValue, err = tidis.db.Get(txn, ttlKey)
	if err != nil {
		return
	}
	if ttlValue == nil {
		// no set expire
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

//removeMetaKey remove expire meta key
func (tidis *Tidis) removeMetaKey(txn interface{}, key []byte) (err error) {
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
	ttlKey = utils.EncodeTTLKey(key)
	if err != nil {
		return
	}
	value, err = tidis.db.Get(txn, ttlKey)
	if value == nil {
		return
	}
	ts, err = utils.BytesToUint64(value)
	if err != nil {
		return
	}
	expireKey = utils.EncodeExpireKey(key, int64(ts))
	if err = tikv_txn.Delete(ttlKey); err != nil {
		return
	}
	if err = tikv_txn.Delete(expireKey); err != nil {
		return
	}
	return
}
