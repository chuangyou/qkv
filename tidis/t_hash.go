package tidis

import (
	"context"

	"strconv"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
)

//HDel removes the specified fields from the hash stored at key.
func (tidis *Tidis) HDel(txn interface{}, key []byte, fields ...[]byte) (deleted int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		ttl            uint64
		hsize          uint64
		field          []byte
		hashDataKey    []byte
		value          []byte
		hashMetaValue  []byte
	)
	if len(key) == 0 || len(fields) == 0 {
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	hsize, ttl, _, err = tidis.getHashMetaWithType(txn, key)
	if err != nil {
		return
	}
	for _, field = range fields {
		//encode hash field key
		hashDataKey = utils.EncodeHashData(key, field)
		//get field value
		value, err = tidis.db.Get(txn, hashDataKey)
		if err != nil {
			return
		}
		if value != nil {
			deleted++
			err = tikv_txn.Delete(hashDataKey)
			if err != nil {
				return
			}
		}
	}
	hsize = hsize - uint64(deleted)
	if hsize > 0 {
		//update meta
		hashMetaValue = tidis.createHashMeta(hsize, ttl, utils.FLAG_NORMAL)
		//encode hash type
		hashMetaValue = utils.EncodeData(utils.HASH_TYPE, hashMetaValue)
		err = tikv_txn.Set(key, hashMetaValue)
		if err != nil {
			return
		}
	} else {
		err = tikv_txn.Delete(key)
		if err != nil {
			return
		}
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//HExists returns if field is an existing field in the hash stored at key.
func (tidis *Tidis) HExists(txn interface{}, key, field []byte) (ret int64, err error) {
	var (
		value []byte
	)
	value, err = tidis.HGet(txn, key, field)
	if err != nil {
		return
	}

	if value != nil && len(value) > 0 {
		ret = 1
	}
	return
}

//HGet returns the value associated with field in the hash stored at key.
func (tidis *Tidis) HGet(txn interface{}, key, field []byte) (value []byte, err error) {
	var (
		hashDataKey []byte
	)
	if len(key) == 0 || len(field) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//delete hash if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	hashDataKey = utils.EncodeHashData(key, field)
	value, err = tidis.db.Get(txn, hashDataKey)
	return
}

//HGetAll returns all fields and values of the hash stored at key.
func (tidis *Tidis) HGetAll(txn interface{}, key []byte) (kvs []interface{}, err error) {
	var (
		hsize       uint64
		hashDataKey []byte
		members     [][]byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//delete hash if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	hsize, _, _, err = tidis.getHashMetaWithType(txn, key)
	if err != nil {
		return
	}
	if hsize == 0 {
		kvs = utils.EmptyListInterfaces
		return
	}
	hashDataKey = utils.EncodeHashData(key, nil)
	members, err = tidis.db.GetRangeKeysValues(txn, hashDataKey, nil, hsize, true)
	if err != nil {
		return
	}
	kvs = make([]interface{}, len(members))
	for i := 0; i < len(members); i = i + 1 {
		if i%2 == 0 {
			_, kvs[i], _ = utils.DecodeHashData(members[i])
		} else {
			kvs[i] = members[i]
		}
	}
	return
}

//HIncrby increments the number stored at field in the hash stored at key by increment.
func (tidis *Tidis) HIncrby(txn interface{}, key []byte, field []byte, step int64) (resp int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		hsize          uint64
		ttl            uint64
		newValue       int64
		newValueRaw    []byte
		hashDataKey    []byte
		oldRaw         []byte
		oldValue       int64
		hashMetaValue  []byte
	)
	if len(key) == 0 || len(field) == 0 {
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	//delete hash if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	//get hash meta
	hsize, _, _, err = tidis.getHashMetaWithType(txn, key)
	if err != nil {
		return
	}
	hashDataKey = utils.EncodeHashData(key, field)
	//hash field value
	oldRaw, err = tidis.db.Get(txn, hashDataKey)
	if err != nil {
		return
	}
	if oldRaw == nil {
		hsize++
		newValue = step
		//add field
		newValueRaw = []byte(strconv.FormatInt(newValue, 10))
		err = tikv_txn.Set(hashDataKey, newValueRaw)
		if err != nil {
			return
		}
		//update meta
		hashMetaValue = tidis.createHashMeta(hsize, ttl, utils.FLAG_NORMAL)
		//encode hash type
		hashMetaValue = utils.EncodeData(utils.HASH_TYPE, hashMetaValue)
		err = tikv_txn.Set(key, hashMetaValue)
		if err != nil {
			return
		}
	} else {
		//get old value
		oldValue, _ = strconv.ParseInt(string(oldRaw), 10, 64)
		//set new value
		newValue = oldValue + step
		newValueRaw = []byte(strconv.FormatInt(newValue, 10))
		err = tikv_txn.Set(hashDataKey, newValueRaw)
		if err != nil {
			return
		}

	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	resp = newValue
	return
}

//HKeys returns all field names in the hash stored at key.
func (tidis *Tidis) HKeys(txn interface{}, key []byte) (keys []interface{}, err error) {
	var (
		hsize    uint64
		startKey []byte
		members  [][]byte
		i        int
		hashKey  []byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//delete hash if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	hsize, _, _, err = tidis.getHashMetaWithType(txn, key)
	if err != nil {
		return
	}
	if hsize == 0 {
		keys = utils.EmptyListInterfaces
		return
	}
	startKey = utils.EncodeHashData(key, nil)
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, true, nil, true, 0, hsize, false)
	if err != nil {
		return
	}
	keys = make([]interface{}, len(members))
	for i, hashKey = range members {
		_, keys[i], _ = utils.DecodeHashData(hashKey)
	}
	return
}

//HLen returns the number of fields contained in the hash stored at key.
func (tidis *Tidis) HLen(txn interface{}, key []byte) (size int64, err error) {
	var (
		hsize uint64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//delete hash if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	hsize, _, _, err = tidis.getHashMetaWithType(txn, key)
	if err != nil {
		return
	}
	size = int64(hsize)
	return
}

//HMGet returns the values associated with the specified fields in the hash stored at key.
func (tidis *Tidis) HMGet(txn interface{}, key []byte, fields ...[]byte) (resp []interface{}, err error) {
	var (
		i            int
		field        []byte
		hashDataKeys [][]byte
		dataM        = make(map[string][]byte)
		x            int
		item         []byte
		value        []byte
		ok           bool
	)
	if len(key) == 0 || len(fields) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//delete hash if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	hashDataKeys = make([][]byte, len(fields))
	for i, field = range fields {
		hashDataKeys[i] = utils.EncodeHashData(key, field)
	}
	dataM, err = tidis.db.MGet(txn, hashDataKeys)
	if err != nil {
		return
	}
	resp = make([]interface{}, len(fields))
	for x, item = range hashDataKeys {
		value, ok = dataM[string(item)]
		if ok {
			resp[x] = value
		} else {
			resp[x] = nil
		}
	}
	return
}

//HMSet sets the specified fields to their respective values in the hash stored at key.
func (tidis *Tidis) HMSet(txn interface{}, key []byte, fieldsAndValues ...[]byte) (err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		ttl            uint64
		hsize          uint64
		field          []byte
		value          []byte
		hashDataKey    []byte
		oldValue       []byte
		hashMetaValue  []byte
	)
	if len(key) == 0 || len(fieldsAndValues)%2 != 0 {
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	//delete hash if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	//get meta
	hsize, ttl, _, err = tidis.getHashMetaWithType(txn, key)
	if err != nil {
		return
	}
	for i := 0; i < len(fieldsAndValues)-1; i = i + 2 {
		field, value = fieldsAndValues[i], fieldsAndValues[i+1]
		hashDataKey = utils.EncodeHashData(key, field)
		oldValue, err = tidis.db.Get(txn, hashDataKey)
		if err != nil {
			return
		}
		if oldValue == nil {
			hsize++
		}
		// update field
		err = tikv_txn.Set(hashDataKey, value)
		if err != nil {
			return
		}
	}
	//update meta
	hashMetaValue = tidis.createHashMeta(hsize, ttl, utils.FLAG_NORMAL)
	//encode hash type
	hashMetaValue = utils.EncodeData(utils.HASH_TYPE, hashMetaValue)
	err = tikv_txn.Set(key, hashMetaValue)
	if err != nil {
		return
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//HSet sets field in the hash stored at key to value.
func (tidis *Tidis) HSet(txn interface{}, key, field, value []byte) (ret int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		ttl            uint64
		hsize          uint64
		hashDataKey    []byte
		oldValue       []byte
		hashMetaValue  []byte
	)
	if len(key) == 0 || len(field) == 0 || len(value) == 0 {
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	//delete hash if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	hsize, ttl, _, err = tidis.getHashMetaWithType(txn, key)
	if err != nil {
		return
	}
	hashDataKey = utils.EncodeHashData(key, field)
	//hash field value
	oldValue, err = tidis.db.Get(txn, hashDataKey)
	if err != nil {
		return
	}
	if oldValue != nil {
		ret = 0
	} else {
		ret = 1
		hsize++
		//update meta
		hashMetaValue = tidis.createHashMeta(hsize, ttl, utils.FLAG_NORMAL)
		//encode hash type
		hashMetaValue = utils.EncodeData(utils.HASH_TYPE, hashMetaValue)
		err = tikv_txn.Set(key, hashMetaValue)
		if err != nil {
			return
		}
	}
	// update field
	err = tikv_txn.Set(hashDataKey, value)
	if err != nil {
		return
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return

}

//HSetNX sets field in the hash stored at key to value, only if field does not yet exist. If key does not exist, a new key holding a hash is created.
func (tidis *Tidis) HSetNX(txn interface{}, key, field, value []byte) (isSeted int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		ttl            uint64
		hsize          uint64
		hashDataKey    []byte
		hashMetaValue  []byte
		oldValue       []byte
	)
	if len(key) == 0 || len(field) == 0 || len(value) == 0 {
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	//delete hash if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	hsize, ttl, _, err = tidis.getHashMetaWithType(txn, key)
	if err != nil {
		return
	}
	hashDataKey = utils.EncodeHashData(key, field)
	oldValue, err = tidis.db.Get(txn, hashDataKey)
	if err != nil {
		return
	}
	if oldValue != nil {
		return
	}
	hsize++
	// set field
	err = tikv_txn.Set(hashDataKey, value)
	if err != nil {
		return
	}
	//update meta
	hashMetaValue = tidis.createHashMeta(hsize, ttl, utils.FLAG_NORMAL)
	//encode hash type
	hashMetaValue = utils.EncodeData(utils.HASH_TYPE, hashMetaValue)
	err = tikv_txn.Set(key, hashMetaValue)
	if err != nil {
		return
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	isSeted = 1
	return

}

//HStrlen returns the string length of the value associated with field in the hash stored at key.
func (tidis *Tidis) HStrlen(txn interface{}, key, field []byte) (ret int64, err error) {
	var (
		value []byte
	)
	value, err = tidis.HGet(txn, key, field)
	if err != nil {
		return
	}
	ret = int64(len(value))
	return
}

//HVals returns all values in the hash stored at key.
func (tidis *Tidis) HVals(txn interface{}, key []byte) (values []interface{}, err error) {
	var (
		hsize       uint64
		members     [][]byte
		hashDataKey []byte
		value       []byte
		i           int
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//delete hash if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	hsize, _, _, err = tidis.getHashMetaWithType(txn, key)
	if err != nil {
		return
	}
	if hsize == 0 {
		values = utils.EmptyListInterfaces
		return
	}
	hashDataKey = utils.EncodeHashData(key, nil)
	members, err = tidis.db.GetRangeKeysValues(txn, hashDataKey, nil, hsize, false)
	if err != nil {
		return
	}
	values = make([]interface{}, len(members))
	for i, value = range members {
		values[i] = value
	}
	return
}

//ClearHash clear hash
func (tidis *Tidis) ClearHash(txn interface{}, key []byte) (deleted int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		hsize          uint64
		startKey       []byte
		members        [][]byte
		hashKey        []byte
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	hsize, _, _, err = tidis.getHashMetaWithType(txn, key)
	if err != nil {
		return
	}
	if hsize == 0 {
		return
	}
	err = tikv_txn.Delete(key)
	if err != nil {
		return
	}
	startKey = utils.EncodeHashData(key, nil)
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, true, nil, true, 0, hsize, false)
	if err != nil {
		return
	}
	for _, hashKey = range members {
		err = tikv_txn.Delete(hashKey)
		if err != nil {
			return
		}
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}
func (tidis *Tidis) getHashMetaWithType(txn interface{}, key []byte) (ssize uint64, ttl uint64, flag byte, err error) {
	var (
		dataType byte
	)
	dataType, ssize, ttl, flag, err = tidis.getHashMeta(txn, key)
	if ssize > 0 {
		if dataType != utils.HASH_TYPE {
			err = qkverror.ErrorWrongType
			return
		}
	}
	return
}
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
