package tidis

import (
	"context"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/deckarep/golang-set"
	"github.com/pingcap/tidb/kv"
)

const (
	Diff = iota
	Inter
	Union
)

//SAdd add the specified members to the set stored at key.
func (tidis *Tidis) SAdd(txn interface{}, key []byte, members ...[]byte) (ret int, err error) {
	var (
		notTransaction bool
		ssize          uint64
		ttl            uint64
		member         []byte
		setKey         []byte
		setMKey        []byte
		setValue       []byte
		value          []byte
		addedCount     int
		tikv_txn       kv.Transaction
		ok             bool
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
	setKey = utils.EncodeSetKey(key)
	//get set
	ssize, ttl, _, err = tidis.getSetMeta(txn, setKey)
	if err != nil {
		return
	}
	//add members
	for _, member = range members {
		setMKey = utils.EncodeSetData(key, member)
		value, err = tidis.db.Get(txn, setMKey)
		if err != nil {
			return
		}
		if value == nil {
			err = tidis.db.Set(txn, setMKey, []byte{0})
			if err != nil {
				return
			}
			addedCount++
		}
	}
	// update meta
	setValue = tidis.createSetMeta(ssize+uint64(addedCount), ttl, utils.FLAG_NORMAL)
	err = tidis.db.Set(txn, setKey, setValue)
	ret = addedCount
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
	return
}

//SCard returns the set cardinality (number of elements) of the set stored at key.
func (tidis *Tidis) SCard(txn interface{}, key []byte) (ret int64, err error) {
	var (
		ssize          uint64
		setKey         []byte
		startKey       []byte
		deleteKeys     [][]byte
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
	setKey = utils.EncodeSetKey(key)
	deleteKeys = append(deleteKeys, setKey)
	//get set
	ssize, _, _, err = tidis.getSetMeta(txn, setKey)
	if err != nil {
		return
	}
	//delete set key
	tidis.db.DeleteWithTxn(txn, deleteKeys)
	//TODO Async delete big key
	//delete set member keys
	startKey = utils.EncodeSetData(key, []byte(nil))
	_, err = tidis.db.DeleteRangeWithTxn(txn, startKey, nil, ssize)
	//delete set member keys
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
	ret = int64(ssize)
	return
}

//SDiff Returns the members of the set resulting from the difference between the first set and all the successive sets.
func (tidis *Tidis) SDiff(txn interface{}, keys ...[]byte) ([]interface{}, error) {
	return tidis.SAction(txn, Diff, keys...)
}

//SDiffStore this command is equal to SDIFF, but instead of returning the resulting set, it is stored in destination.
func (tidis *Tidis) SDiffStore(txn interface{}, dest []byte, keys ...[]byte) (ret int64, err error) {
	return tidis.SStoreAction(txn, Diff, keys...)
}
func (tidis *Tidis) SAction(txn interface{}, actionType int, keys ...[]byte) (setSlice []interface{}, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		mapSets        []mapset.Set
		actionSet      mapset.Set
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
	mapSets, err = tidis.newSetsFromKeys(txn, keys...)
	if err != nil {
		return
	}
	for i, temp_ms := range mapSets {
		ms, ok := temp_ms.(mapset.Set)
		if !ok {
			return
		}
		if i == 0 {
			actionSet = ms
		} else {
			switch actionType {
			case Diff:
				actionSet = actionSet.Difference(ms)
				break
			case Inter:
				actionSet = actionSet.Intersect(ms)
				break
			case Union:
				actionSet = actionSet.Union(ms)
				break
			}
		}
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}

	setSlice = actionSet.ToSlice()

	return
}

func (tidis *Tidis) SStoreAction(txn interface{}, dest []byte, keys ...[]byte) (ret int64, err error) {
	if len(keys) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
}
func (tidis *Tidis) newSetsFromKeys(txn interface{}, keys ...[]byte) (mapSets []mapset.Set, err error) {
	var (
		setKey     []byte
		members    [][]byte
		i          int
		x          int
		key        []byte
		ssize      uint64
		startKey   []byte
		membersStr []interface{}
		member     []byte
		field      []byte
	)
	mapSets = make([]mapset.Set, len(keys))
	for i, key = range keys {
		setKey = utils.EncodeSetKey(key)
		ssize, _, _, err = tidis.getSetMeta(txn, setKey)
		if err != nil {
			return
		}
		if ssize == 0 {
			mapSets[i] = nil
			continue
		}
		startKey = utils.EncodeSetData(key, []byte(nil))
		members, _, err = tidis.db.GetRangeKeys(txn, startKey, true, nil, true, 0, ssize, false)
		if err != nil {
			return
		}
		membersStr = make([]interface{}, len(members))
		for x, member = range members {
			_, field, _ = utils.DecodeSetData(member)
			membersStr[x] = string(field)
		}
		mapSets[i] = mapset.NewSet(membersStr...)
	}
	return

}
func (tidis *Tidis) getSetMeta(txn interface{}, key []byte) (ssize uint64, ttl uint64, flag byte, err error) {
	return tidis.getHashMeta(txn, key)
}
func (tidis *Tidis) createSetMeta(size, ttl uint64, flag byte) []byte {
	return tidis.createHashMeta(size, ttl, flag)
}
