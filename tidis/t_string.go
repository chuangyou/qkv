package tidis

import (
	"context"
	"time"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
	log "github.com/sirupsen/logrus"
)

//Get get the value of key.
func (tidis *Tidis) Get(txn interface{}, key []byte) (data []byte, err error) {
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	key = utils.EncodeStringKey(key)
	data, err = tidis.db.Get(txn, key)
	return
}

//Set set key to hold the string value.
func (tidis *Tidis) Set(txn interface{}, key, value []byte) (err error) {
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	key = utils.EncodeStringKey(key)
	err = tidis.db.Set(txn, key, value)
	return
}

//MGet returns the values of all specified keys.
func (tidis *Tidis) MGet(txn interface{}, keys [][]byte) (resp []interface{}, err error) {
	var (
		data = make(map[string][]byte)
	)
	if len(keys) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	for i := 0; i < len(keys); i++ {
		keys[i] = utils.EncodeStringKey(keys[i])
	}
	data, err = tidis.db.MGet(txn, keys)
	resp = make([]interface{}, len(keys))
	for i, key := range keys {
		if v, ok := data[string(key)]; ok {
			resp[i] = v
		} else {
			resp[i] = nil
		}
	}
	return
}

//MSet sets the given keys to their respective values.
func (tidis *Tidis) MSet(txn interface{}, kvs [][]byte) (resp int, err error) {
	var (
		kvm = make(map[string][]byte, len(kvs))
	)
	if len(kvs) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	for i := 0; i < len(kvs)-1; i += 2 {
		k := string(utils.EncodeStringKey(kvs[i]))
		v := kvs[i+1]
		kvm[k] = v
	}
	resp, err = tidis.db.MSet(txn, kvm)
	return
}

//Delete removes the specified keys. A key is ignored if it does not exist.
func (tidis *Tidis) Delete(txn interface{}, keys [][]byte) (resp int64, err error) {
	var (
		dataKeys       = make([][]byte, len(keys))
		key            []byte
		ret            int64
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
	)
	if len(keys) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
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
	//encode data key
	for i := 0; i < len(keys); i++ {
		dataKeys[i] = utils.EncodeStringKey(keys[i])
	}
	for _, key = range keys {
		// clear expire meta
		err = tidis.db.ClearExpire(txn, key)
		if err != nil {
			log.Debugf("clearexpire error:%v", err)
			return
		}
	}
	ret, err = tidis.db.DeleteWithTxn(txn, dataKeys)
	if err != nil {
		return
	} else {
		if notTransaction {
			err = tikv_txn.Commit(context.Background())
			if err != nil {
				return
			}
		}
	}
	resp = ret
	return
}

//SetEX set key to hold the string value and set key to timeout after a given number of seconds
func (tidis *Tidis) SetEX(txn interface{}, key []byte, seconds int64, value []byte) (err error) {
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	err = tidis.db.SetEX(txn, key, seconds, value)
	return
}

//Incr increments the number stored at key by increment.
func (tidis *Tidis) Incr(txn interface{}, key []byte, step int64) (ret int64, err error) {
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	key = utils.EncodeStringKey(key)
	ret, err = tidis.db.Incr(txn, key, step)
	return
}

//Decr decrements the number stored at key by decrement.
func (tidis *Tidis) Decr(txn interface{}, key []byte, step int64) (ret int64, err error) {
	return tidis.Incr(txn, key, -1*step)
}

//TTL returns the remaining time to live of a key that has a timeout.
func (tidis *Tidis) TTL(txn interface{}, key []byte) (ttl int64, err error) {
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	return tidis.db.TTL(txn, key)
}

//PTTL get the time to live for a key in milliseconds
func (tidis *Tidis) PTTL(txn interface{}, key []byte) (ttl int64, err error) {
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	return tidis.db.PTTL(txn, key)
}

//Expire set a key's time to live in seconds
func (tidis *Tidis) Expire(txn interface{}, key []byte, seconds int64) (ret int, err error) {
	var (
		ts             int64
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
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
	ts = seconds*1000 + (time.Now().UnixNano() / 1000 / 1000)
	ret, err = tidis.db.PExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//PExipre this command works exactly like EXPIRE but the time to live of the key is specified in milliseconds instead of seconds.
func (tidis *Tidis) PExpire(txn interface{}, key []byte, ms int64) (ret int, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		ts             int64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
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
	ts = ms + (time.Now().UnixNano() / 1000 / 1000)
	ret, err = tidis.db.PExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ExpireAt set the expiration for a key as a UNIX timestamp
func (tidis *Tidis) ExpireAt(txn interface{}, key []byte, ts int64) (ret int, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
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
	ts = ts * 1000
	ret, err = tidis.db.PExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//PExpireAt set the expiration for a key as a UNIX timestamp specified in milliseconds
func (tidis *Tidis) PExpireAt(txn interface{}, key []byte, ts int64) (ret int, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
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
	ret, err = tidis.db.PExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}
