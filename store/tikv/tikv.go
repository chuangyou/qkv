package tikv

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/chuangyou/qkv/config"
	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
	ti "github.com/pingcap/tidb/store/tikv"
)

type Tikv struct {
	store kv.Storage
}

//OpenTikv open the tikv connection by pds
func Open(conf *config.Config) (*Tikv, error) {
	driver := ti.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s?cluster=1&disableGC=false", conf.Tikv.Pds))
	if err != nil {
		return nil, err
	}
	return &Tikv{store: store}, nil
}

//Get get the value of key and  can use tikv transaction get the value
func (tikv *Tikv) Get(txn interface{}, key []byte) (data []byte, err error) {
	var (
		snapshot kv.Snapshot
		tikv_txn kv.Transaction
		ok       bool
	)
	if txn != nil {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		data, err = tikv_txn.Get(key)
		if err != nil {
			if kv.IsErrNotFound(err) {
				data = nil
				err = nil
				return
			}
		}
	} else {
		snapshot, err = tikv.store.GetSnapshot(kv.MaxVersion)
		if err != nil {
			return
		}
		data, err = snapshot.Get(key)
		if err != nil {
			if kv.IsErrNotFound(err) {
				data = nil
				err = nil
				return
			}
		}
	}
	return
}

//Set set key to hold the string value,must be use txn
func (tikv *Tikv) Set(txn interface{}, key, value []byte) (err error) {
	var (
		tikv_txn kv.Transaction
		ok       bool
	)
	if txn != nil {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		err = tikv_txn.Set(key, value)
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
		err = tikv_txn.Set(key, value)
		if err == nil {
			err = tikv_txn.Commit(context.Background())
		}
	}
	return
}

//MGet returns the values of all specified keys.
func (tikv *Tikv) MGet(txn interface{}, keys [][]byte) (data map[string][]byte, err error) {
	var (
		v        []byte
		snapshot kv.Snapshot
		tikv_txn kv.Transaction
		ok       bool
	)
	if txn != nil {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		data = make(map[string][]byte)
		for _, key := range keys {
			v, err = tikv_txn.Get(key)
			if err != nil {
				data = nil
				return
			}
			data[string(key)] = v
		}
	} else {
		snapshot, err = tikv.store.GetSnapshot(kv.MaxVersion)
		if err != nil {
			return
		}
		kvKeys := make([]kv.Key, len(keys))
		for i := 0; i < len(keys); i++ {
			kvKeys[i] = keys[i]
		}
		data, err = snapshot.BatchGet(kvKeys)

	}
	return
}

//MSet sets the given keys to their respective values.
func (tikv *Tikv) MSet(txn interface{}, kvm map[string][]byte) (resp int, err error) {
	var (
		tikv_txn kv.Transaction
		ok       bool
	)
	if txn != nil {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		for k, v := range kvm {
			err = tikv_txn.Set([]byte(k), v)
			if err != nil {
				return
			}
		}
		resp = len(kvm)
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
		for k, v := range kvm {
			err = tikv_txn.Set([]byte(k), v)
			if err != nil {
				return
			}
		}
		err = tikv_txn.Commit(context.Background())
		if err == nil {
			resp = len(kvm)
		}
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
		err = tikv_txn.Set(key, value)
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
		err = tikv_txn.Set(key, value)
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
	v, err = tikv.Get(txn, key)
	if err != nil {
		return
	}
	if v == nil {
		//not key
		return
	}
	//get ttl
	ttlKey = utils.EncodeTTLKey(key)
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
		expireKey = utils.EncodeExpireKey(key, int64(ttlOld))
		//delete old expire key
		err = tikv_txn.Delete(expireKey)
		if err != nil {
			return
		}
	}
	//set expire key
	expireKey = utils.EncodeExpireKey(key, ts)
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
func (tikv *Tikv) GetRangeKeys(txn interface{},
	start []byte,
	withStart bool,
	end []byte,
	withEnd bool,
	offset,
	limit uint64,
	countOnly bool) (keys [][]byte, count uint64, err error) {
	var (
		tikv_txn kv.Transaction
		ok       bool
		snapshot kv.Snapshot
		it       kv.Iterator
		key      kv.Key
	)
	if txn == nil {
		snapshot, err = tikv.store.GetSnapshot(kv.MaxVersion)
		if err != nil {
			return
		}
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		snapshot = tikv_txn.GetSnapshot()
	}
	it, err = snapshot.Seek(start)
	if err != nil {
		return
	}
	defer it.Close()
	for limit > 0 {
		if !it.Valid() {
			break
		}
		key = it.Key()

		err = it.Next()
		if err != nil {
			return
		}
		if !withStart && key.Cmp(start) == 0 {
			continue
		}
		if !withEnd && key.Cmp(end) == 0 {
			break
		}
		if end != nil && key.Cmp(end) > 0 {
			break
		}
		if offset > 0 {
			offset--
			continue
		}
		if countOnly {
			count++
		} else {
			keys = append(keys, key)
		}
		limit--
	}
	return
}
func (tikv *Tikv) GetRangeKeysValues(txn interface{}, start []byte, end []byte, limit uint64, withkeys bool) (kvs [][]byte, err error) {
	var (
		tikv_txn kv.Transaction
		ok       bool
		snapshot kv.Snapshot
		it       kv.Iterator
		key      kv.Key
		value    kv.Key
	)
	if txn == nil {
		snapshot, err = tikv.store.GetSnapshot(kv.MaxVersion)
		if err != nil {
			return
		}
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		snapshot = tikv_txn.GetSnapshot()
	}
	it, err = snapshot.Seek(start)
	if err != nil {
		return
	}
	defer it.Close()
	for limit > 0 {
		if !it.Valid() {
			break
		}
		key = it.Key()
		value = it.Value()
		if end != nil && key.Cmp(end) > 0 {
			break
		}
		if withkeys {
			kvs = append(kvs, key)
		}
		kvs = append(kvs, value)
		limit--
		err = it.Next()
		if err != nil {
			return
		}
	}
	return
}
func (tikv *Tikv) DeleteRangeWithTxn(txn interface{}, start []byte, end []byte, limit uint64) (deleted uint64, err error) {
	var (
		tikv_txn kv.Transaction
		ok       bool
		snapshot kv.Snapshot
		it       kv.Iterator
		key      kv.Key
	)
	tikv_txn, ok = txn.(kv.Transaction)
	if !ok {
		err = qkverror.ErrorServerInternal
		return
	}
	snapshot = tikv_txn.GetSnapshot()
	it, err = snapshot.Seek(start)
	if err != nil {
		return
	}
	defer it.Close()
	if limit == 0 {
		limit = math.MaxUint64
	}
	for limit > 0 {
		if !it.Valid() {
			break
		}
		key = it.Key()
		if end != nil && key.Cmp(end) > 0 {
			break
		}
		err = tikv_txn.Delete(key)
		if err != nil {
			return
		}
		limit--
		deleted++
		err = it.Next()
		if err != nil {
			return
		}
	}
	return
}

//NewTxn new a tikv transaction,return a interface.
func (tikv *Tikv) NewTxn() (txn interface{}, err error) {
	txn, err = tikv.store.Begin()
	return
}

//Close close the tikv connection.
func (tikv *Tikv) Close() error {
	return tikv.store.Close()
}
