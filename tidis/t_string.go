package tidis

import (
	"context"
	"time"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
)

//Get get the value of key.
func (tidis *Tidis) Get(txn interface{}, key []byte) (data []byte, err error) {
	var (
		rawData  []byte
		dataType byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//delete kv if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	rawData, err = tidis.db.Get(txn, key)
	if err != nil {
		return
	}
	if rawData != nil {
		//decode string data
		dataType, data, err = utils.DecodeData(rawData)
		if err != nil {
			return
		}
		if dataType != utils.STRING_TYPE {
			err = qkverror.ErrorWrongType
			return
		}
	}
	return
}

//Set set key to hold the string value.
func (tidis *Tidis) Set(txn interface{}, key, value []byte) (err error) {
	var (
		dataType byte
		rawData  []byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//get old data and data type
	rawData, err = tidis.db.Get(txn, key)
	if err != nil {
		return
	} else if rawData != nil {
		//check data type
		dataType, _, err = utils.DecodeData(rawData)
		if err != nil {
			return
		}
		if dataType != utils.STRING_TYPE {
			err = qkverror.ErrorWrongType
			return
		}
	}
	//encode string data
	value = utils.EncodeData(utils.STRING_TYPE, value)
	err = tidis.db.Set(txn, key, value)
	if err == nil {
		tidis.DeleteIfExpired(txn, key, false)
	}
	return
}

//MGet returns the values of all specified keys.
func (tidis *Tidis) MGet(txn interface{}, keys [][]byte) (resp []interface{}, err error) {
	var (
		data     = make(map[string][]byte)
		dataType byte
		value    []byte
	)
	if len(keys) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	data, err = tidis.db.MGet(txn, keys)
	resp = make([]interface{}, len(keys))
	for i, key := range keys {
		if rawData, ok := data[string(key)]; ok {
			dataType, value, err = utils.DecodeData(rawData)
			if err != nil {
				return
			}
			//check data type
			if dataType != utils.STRING_TYPE {
				err = qkverror.ErrorWrongType
				return
			}
			resp[i] = value
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
		k := string(kvs[i])
		v := utils.EncodeData(utils.STRING_TYPE, kvs[i+1])
		kvm[k] = v
	}
	resp, err = tidis.db.MSet(txn, kvm)
	return
}

//Delete removes the specified keys. A key is ignored if it does not exist.
func (tidis *Tidis) Delete(txn interface{}, keys [][]byte) (resp int64, err error) {
	var (
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
	for _, key = range keys {
		// clear expire meta
		err = tidis.DeleteIfExpired(txn, key, false)
		if err != nil {
			return
		}
	}
	ret, err = tidis.DeleteWithTxn(txn, keys)
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
	var (
		rawData  []byte
		dataType byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//get old data and data type
	rawData, err = tidis.db.Get(txn, key)
	if err != nil {
		return
	} else if rawData != nil {
		//check data type
		dataType, _, err = utils.DecodeData(rawData)
		if err != nil {
			return
		}
		if dataType != utils.STRING_TYPE {
			err = qkverror.ErrorWrongType
			return
		}
	}
	value = utils.EncodeData(utils.STRING_TYPE, value)
	err = tidis.db.SetEX(txn, key, seconds, value)
	return
}

//Incr increments the number stored at key by increment.
func (tidis *Tidis) Incr(txn interface{}, key []byte, step int64) (ret int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		rawData        []byte
		dataType       byte
		value          []byte
		oldStep        int64
		newStep        int64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}

	if txn != nil {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	} else {
		notTransaction = true
		txn, err = tidis.db.NewTxn()
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
	}
	rawData, err = tidis.db.Get(txn, key)
	if err != nil {
		return
	}
	if rawData != nil {
		dataType, value, err = utils.DecodeData(rawData)
		if err != nil {
			return
		}
		if dataType != utils.STRING_TYPE {
			err = qkverror.ErrorWrongType
			return
		}
		//old value
		oldStep, err = utils.StrBytesToInt64(value)
		if err != nil {
			err = qkverror.ErrorNotInteger
			return
		}
	}
	//new value
	newStep = oldStep + step
	value, err = utils.Int64ToStrBytes(newStep)
	if err != nil {
		return
	}
	value = utils.EncodeData(utils.STRING_TYPE, value)
	err = tikv_txn.Set(key, value)
	if err != nil {
		return
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	ret = newStep
	return
}

//Decr decrements the number stored at key by decrement.
func (tidis *Tidis) Decr(txn interface{}, key []byte, step int64) (ret int64, err error) {
	return tidis.Incr(txn, key, -1*step)
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
