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
	log "github.com/sirupsen/logrus"
)

type Tikv struct {
	store kv.Storage
}

//OpenTikv open the tikv connection by pds
func Open(conf *config.Config) (*Tikv, error) {
	driver := ti.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s/pd?cluster=1", conf.Tikv.Pds))
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
	)
	if txn != nil {
		data = make(map[string][]byte)
		for _, key := range keys {
			v, err = tikv.Get(txn, key)
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

//DeleteWithTxn removes the specified keys. A key is ignored if it does not exist.
func (tikv *Tikv) DeleteWithTxn(txn interface{}, keys [][]byte) (deleted int64, err error) {
	var (
		ok       bool
		tikv_txn kv.Transaction
		value    []byte
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
	for _, k := range keys {
		value, _ = tikv.Get(txn, k)
		if value != nil {
			deleted++
		}
		err = tikv_txn.Delete(k)
		if err != nil {
			deleted = 0
			return
		}
	}
	return
}

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

//Incr increments the number stored at key by increment.
func (tikv *Tikv) Incr(txn interface{}, key []byte, step int64) (ret int64, err error) {
	var (
		tikv_txn      kv.Transaction
		ok            bool
		value         []byte
		oldStep       int64
		newStep       int64
		noTransaction bool
	)

	if txn != nil {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	} else {
		noTransaction = true
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
	}
	value, err = tikv_txn.Get(key)
	if err != nil {
		if !kv.IsErrNotFound(err) {
			return
		}
	}
	//old value
	oldStep, _ = utils.StrBytesToInt64(value)
	//new value
	newStep = oldStep + step
	value, err = utils.Int64ToStrBytes(newStep)
	if err != nil {
		return
	}
	tikv_txn.Set(key, value)
	if err != nil {
		return
	}
	if noTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	ret = newStep
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

//NewTxn new a tikv transaction,return a interface.
func (tikv *Tikv) NewTxn() (txn interface{}, err error) {
	txn, err = tikv.store.Begin()
	return
}

//Close close the tikv connection.
func (tikv *Tikv) Close() error {
	return tikv.store.Close()
}
