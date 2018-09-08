package tidis

import (
	"context"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
)

//LIndex returns the element at index index in the list stored at key.
func (tidis *Tidis) LIndex(txn interface{}, key []byte, index int64) (resp []byte, err error) {
	var (
		head, size  uint64
		listDataKey []byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//delete list if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	head, _, size, _, _, err = tidis.getListMetaWithType(txn, key)
	if err != nil {
		return
	}
	if index >= 0 {
		if index >= int64(size) {
			return
		}
	} else {
		if -index > int64(size) {
			return
		}
		index = index + int64(size)
	}
	listDataKey = utils.EncodeListData(key, uint64(index)+head)
	return tidis.db.Get(txn, listDataKey)
}

//Llen returns the specified elements of the list stored at key. T
func (tidis *Tidis) LLen(txn interface{}, key []byte) (ret int64, err error) {
	var (
		size uint64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//delete list if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	_, _, size, _, _, err = tidis.getListMetaWithType(txn, key)
	if err != nil {
		return
	}
	ret = int64(size)
	return
}

//LPop removes and returns the first element of the list stored at key.
func (tidis *Tidis) LPop(txn interface{}, key []byte, direc uint8) (item []byte, err error) {
	var (
		tikv_txn              kv.Transaction
		ok                    bool
		notTransaction        bool
		head, tail, size, ttl uint64
		listMetaValue         []byte
		listDataKey           []byte
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	//delete list if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	head, tail, size, ttl, _, err = tidis.getListMetaWithType(txn, key)
	if err != nil {
		return
	}
	if direc == utils.LHeadDirection {
		listDataKey = utils.EncodeListData(key, head)
		head++
	} else {
		tail--
		listDataKey = utils.EncodeListData(key, tail)
	}
	size--
	if size == 0 {
		//delete meta
		err = tikv_txn.Delete(key)
		if err != nil {
			return
		}
	} else {
		// update meta key
		listMetaValue, err = tidis.createListMeta(head, tail, size, ttl, utils.FLAG_NORMAL)
		if err != nil {
			return
		}
		listMetaValue = utils.EncodeData(utils.LIST_TYPE, listMetaValue)
		err = tikv_txn.Set(key, listMetaValue)
		if err != nil {
			return
		}
	}
	item, err = tikv_txn.GetSnapshot().Get(listDataKey)
	if err != nil {
		if !kv.IsErrNotFound(err) {
			return
		} else {
			item = nil
			err = nil
			return
		}
	}
	// delete item
	err = tikv_txn.Delete(listDataKey)
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

//LPush insert all the specified values at the head of the list stored at key.
func (tidis *Tidis) LPush(txn interface{}, key []byte, direc uint8, items ...[]byte) (count int64, err error) {
	var (
		tikv_txn                     kv.Transaction
		ok                           bool
		notTransaction               bool
		head, tail, size, ttl, index uint64
		itemCount                    uint64
		listMetaValue                []byte
		listDataKey                  []byte
		item                         []byte
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	//delete list if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	head, tail, size, ttl, _, err = tidis.getListMetaWithType(txn, key)
	if err != nil {
		return
	}
	itemCount = uint64(len(items))

	if direc == utils.LHeadDirection {
		index = head
		head = head - itemCount
	} else {
		index = tail
		tail = tail + itemCount
	}
	size = size + itemCount
	listMetaValue, err = tidis.createListMeta(head, tail, size, ttl, utils.FLAG_NORMAL)
	if err != nil {
		return
	}
	// update meta
	listMetaValue = utils.EncodeData(utils.LIST_TYPE, listMetaValue)
	err = tikv_txn.Set(key, listMetaValue)
	if err != nil {
		return
	}
	for _, item = range items {
		if direc == utils.LHeadDirection {
			index--
			listDataKey = utils.EncodeListData(key, index)
		} else {
			listDataKey = utils.EncodeListData(key, index)
			index++
		}
		err = tikv_txn.Set(listDataKey, item)
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
	count = int64(size)
	return
}

//LRange returns the element at index index in the list stored at key.
func (tidis *Tidis) LRange(txn interface{}, key []byte, start, stop int64) (resp []interface{}, err error) {
	var (
		head, size uint64
		getKeys    [][]byte
		membersM   map[string][]byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if start > stop && (stop > 0 || start < 0) {
		return
	}
	//delete list if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	head, _, size, _, _, err = tidis.getListMetaWithType(txn, key)
	if err != nil {
		return
	}
	if size == 0 {
		resp = utils.EmptyListInterfaces
		return
	}

	if start < 0 {
		if start < -int64(size) {
			start = 0
		} else {
			start = start + int64(size)
		}
	} else {
		if start >= int64(size) {
			return nil, nil
		}
	}
	if stop < 0 {
		if stop < -int64(size) {
			stop = 0
		} else {
			stop = stop + int64(size)
		}
	} else {
		if stop >= int64(size) {
			stop = int64(size) - 1
		}
	}
	if start > stop {
		return
	}
	getKeys = make([][]byte, stop-start+1)
	for i, _ := range getKeys {
		getKeys[i] = utils.EncodeListData(key, head+uint64(start)+uint64(i))
	}
	membersM, err = tidis.db.MGet(txn, getKeys)
	if err != nil {
		return
	}
	resp = make([]interface{}, len(getKeys))
	for i, k := range getKeys {
		value, ok := membersM[string(k)]
		if !ok {
			resp[i] = []byte(nil)
		} else {
			resp[i] = value
		}
	}
	return
}

//LSet sets the list element at index to value.
func (tidis *Tidis) LSet(txn interface{}, key []byte, index int64, value []byte) (err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		head, size     uint64
		listDataKey    []byte
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	//delete list if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	head, _, size, _, _, err = tidis.getListMetaWithType(txn, key)
	if err != nil {
		return
	}
	if index >= 0 {
		if index >= int64(size) {

			err = qkverror.ErrorOutOfRange
			return
		}
	} else {
		if -index > int64(size) {
			err = qkverror.ErrorOutOfRange
			return
		}
		index = index + int64(size)
	}
	if index >= int64(size) {
		err = qkverror.ErrorOutOfRange
		return
	}
	listDataKey = utils.EncodeListData(key, uint64(index)+head)
	err = tikv_txn.Set(listDataKey, value)
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

//LTrim trim an existing list so that it will contain only the specified range of elements specified.
func (tidis *Tidis) LTrim(txn interface{}, key []byte, start, stop int64) (err error) {
	var (
		tikv_txn                               kv.Transaction
		ok                                     bool
		notTransaction                         bool
		head, size, ttl, nhead, ntail, newSize uint64
		listDataKey                            []byte
		listMetaValue                          []byte
		needDel                                bool = false
		x                                      int64
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
	//delete list if expired
	err = tidis.DeleteIfExpired(txn, key, true)
	if err != nil {
		return
	}
	head, _, size, ttl, _, err = tidis.getListMetaWithType(txn, key)
	if err != nil {
		return
	}
	if start < 0 {
		if start < -int64(size) {
			start = 0
		} else {
			start = start + int64(size)
		}
	} else {
		if start >= int64(size) {
			needDel = true
		}
	}

	if stop < 0 {
		if stop < -int64(size) {
			stop = 0
		} else {
			stop = stop + int64(size)
		}
	} else {
		if stop >= int64(size) {
			stop = int64(size) - 1
		}
	}
	if start > stop {
		needDel = true
	}
	if needDel {
		//delete meta key
		err = tikv_txn.Delete(key)
		if err != nil {
			return
		}
		//delete data
		for i := start; i < stop; i++ {
			listDataKey = utils.EncodeListData(key, head+uint64(i))
			err = tikv_txn.Delete(listDataKey)
			if err != nil {
				return
			}
		}
	} else {
		nhead = head + uint64(start)
		ntail = head + uint64(stop) + 1
		newSize = ntail - nhead
		// update meta key
		listMetaValue, err = tidis.createListMeta(nhead, ntail, newSize, ttl, utils.FLAG_NORMAL)
		if err != nil {
			return
		}
		listMetaValue = utils.EncodeData(utils.LIST_TYPE, listMetaValue)
		err = tikv_txn.Set(key, listMetaValue)
		if err != nil {
			return
		}
		for x = 0; x < start; x++ {
			listDataKey = utils.EncodeListData(key, head+uint64(x))
			err = tikv_txn.Delete(listDataKey)
			if err != nil {
				return
			}
		}

		for x = stop; x < int64(newSize)-1; x++ {
			listDataKey = utils.EncodeListData(key, head+uint64(x))
			err = tikv_txn.Delete(listDataKey)
			if err != nil {
				return
			}
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

//ClearListMembers clear the list
func (tidis *Tidis) ClearListMembers(txn interface{}, key []byte) (deleted int64, err error) {
	var (
		tikv_txn         kv.Transaction
		ok               bool
		notTransaction   bool
		head, tail, size uint64
		listDataKey      []byte
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
	head, tail, size, _, _, err = tidis.getListMetaWithType(txn, key)
	if err != nil {
		return
	}
	if size == 0 {
		return
	}
	err = tikv_txn.Delete(key)
	if err != nil {
		return
	}
	for i := head; i < tail; i++ {
		listDataKey = utils.EncodeListData(key, i)
		err = tikv_txn.Delete(listDataKey)
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
func (tidis *Tidis) getListMetaWithType(txn interface{}, key []byte) (head uint64, tail uint64, size uint64, ttl uint64, flag byte, err error) {
	var (
		dataType byte
	)
	dataType, head, tail, size, ttl, flag, err = tidis.getListMeta(txn, key)
	if size > 0 {
		if dataType != utils.LIST_TYPE {
			err = qkverror.ErrorWrongType
			return
		}
	}
	return
}
func (tidis *Tidis) getListMeta(txn interface{}, key []byte) (dataType byte, head uint64, tail uint64, size uint64, ttl uint64, flag byte, err error) {
	var (
		rawData []byte
		value   []byte
	)
	rawData, err = tidis.db.Get(txn, key)
	if err != nil {
		return
	}
	if rawData == nil {
		head = utils.LItemDefaultIndex
		tail = utils.LItemDefaultIndex
		size = 0
		flag = utils.FLAG_NORMAL
		ttl = 0
		return
	}
	dataType, value, err = utils.DecodeData(rawData)
	if err != nil {
		return
	}
	if value == nil {
		head = utils.LItemDefaultIndex
		tail = utils.LItemDefaultIndex
		size = 0
		flag = utils.FLAG_NORMAL
		ttl = 0
		return
	}
	head, err = utils.BytesToUint64(value[0:])
	if err != nil {
		return
	}
	tail, err = utils.BytesToUint64(value[8:])
	if err != nil {
		return
	}
	size, err = utils.BytesToUint64(value[16:])
	if err != nil {
		return
	}
	ttl, err = utils.BytesToUint64(value[24:])
	if err != nil {
		return
	}
	if len(value) > 32 {
		flag = value[32]
	}
	return
}
func (tidis *Tidis) createListMeta(head, tail, size, ttl uint64, flag byte) (buf []byte, err error) {
	buf = make([]byte, 32+1)
	err = utils.Uint64ToBytesExt(buf[0:], head)
	if err != nil {
		return
	}
	err = utils.Uint64ToBytesExt(buf[8:], tail)
	if err != nil {
		return
	}

	err = utils.Uint64ToBytesExt(buf[16:], size)
	if err != nil {
		return
	}

	err = utils.Uint64ToBytesExt(buf[24:], ttl)
	if err != nil {
		return
	}
	buf[32] = flag
	return
}
