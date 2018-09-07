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
		tikv_txn       kv.Transaction
		ok             bool
		ssize          uint64
		ttl            uint64
		setMemberKey   []byte
		member         []byte
		value          []byte
		setValue       []byte
		addedCount     int
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
	//get set
	ssize, ttl, _, err = tidis.getSetMeta(txn, key)
	if err != nil {
		return
	}
	//add members
	for _, member = range members {
		setMemberKey = utils.EncodeSetData(key, member)
		value, err = tidis.db.Get(txn, setMemberKey)
		if err != nil {
			return
		}
		if value == nil {
			err = tidis.db.Set(txn, setMemberKey, []byte{0})
			if err != nil {
				return
			}
			addedCount++
		}
	}
	// update meta
	setValue = tidis.createSetMeta(ssize+uint64(addedCount), ttl, utils.FLAG_NORMAL)
	//encode value
	setValue = utils.EncodeData(utils.SET_TYPE, setValue)
	err = tidis.db.Set(txn, key, setValue)
	ret = addedCount
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

//SCard returns the set cardinality (number of elements) of the set stored at key.
func (tidis *Tidis) SCard(txn interface{}, key []byte) (ret int64, err error) {
	var (
		ssize uint64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//get set
	ssize, _, _, err = tidis.getSetMeta(txn, key)
	if err != nil {
		return
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
	return tidis.SStoreAction(txn, Diff, dest, keys...)
}

//Sinter returns the members of the set resulting from the intersection of all the given sets.
func (tidis *Tidis) SInter(txn interface{}, keys ...[]byte) ([]interface{}, error) {
	return tidis.SAction(txn, Inter, keys...)
}

//SinterStore this command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
func (tidis *Tidis) SInterStore(txn interface{}, dest []byte, keys ...[]byte) (ret int64, err error) {
	return tidis.SStoreAction(txn, Inter, dest, keys...)
}

//Sismember determine if a given value is a member of a set
func (tidis *Tidis) Sismember(txn interface{}, key, member []byte) (ret int64, err error) {
	var (
		setValue []byte
		value    []byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	_, _, _, err = tidis.getSetMeta(txn, key)
	if err != nil {
		return
	}
	setValue = utils.EncodeSetData(key, member)
	value, err = tidis.db.Get(txn, setValue)
	if err != nil {
		return
	}
	if value != nil {
		ret = 1
	}
	return
}

//Smembers get all the members in a set
func (tidis *Tidis) SMembers(txn interface{}, key []byte) (iMembers []interface{}, err error) {
	var (
		startKey []byte
		ssize    uint64
		members  [][]byte
		member   []byte
		i        int
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	//get set
	ssize, _, _, err = tidis.getSetMeta(txn, key)
	if err != nil {
		return
	}
	startKey = utils.EncodeSetData(key, []byte(nil))
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, true, nil, true, 0, ssize, false)
	if err != nil {
		return
	}
	iMembers = make([]interface{}, len(members))
	for i, member = range members {
		_, iMembers[i], err = utils.DecodeSetData(member)
		if err != nil {
			iMembers = nil
			return
		}
	}
	return
}

//SRem remove one or more members from a set
func (tidis *Tidis) SRem(txn interface{}, key []byte, members ...[]byte) (removed int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		setMemberKey   []byte
		value          []byte
		ssize          uint64
		ttl            uint64
		flag           byte
		setValue       []byte
		member         []byte
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
	for _, member = range members {
		//encode member
		setMemberKey = utils.EncodeSetData(key, member)
		//get member
		value, err = tidis.db.Get(txn, setMemberKey)
		if err != nil {
			return
		}
		if value != nil {
			//delete member
			err = tikv_txn.Delete(setMemberKey)
			if err != nil {
				return
			}
			removed++
		}
	}
	if removed > 0 {
		//get meta
		ssize, ttl, flag, err = tidis.getSetMeta(txn, key)
		if err != nil {
			return
		}
		if ssize < uint64(removed) {
			err = qkverror.ErrorInvalidMeta
			return
		}
		ssize = ssize - uint64(removed)
		if ssize > 0 {
			//update meta
			setValue = tidis.createSetMeta(ssize, ttl, flag)
			//encode value type
			setValue = utils.EncodeData(utils.SET_TYPE, setValue)
			err = tikv_txn.Set(key, setValue)
			if err != nil {
				return
			}
		} else {
			//if no member,then delete set key
			err = tikv_txn.Delete(key)
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

//SUnion returns the members of the set resulting from the union of all the given sets.
func (tidis *Tidis) SUnion(txn interface{}, keys ...[]byte) ([]interface{}, error) {
	return tidis.SAction(txn, Union, keys...)
}

//SDel clear set keys
func (tidis *Tidis) ClearSetMembers(txn interface{}, key []byte) (deleted int64, err error) {
	var (
		ssize    uint64
		startKey []byte
	)
	ssize, _, _, err = tidis.getSetMeta(txn, key)
	if err != nil {
		return
	}
	if ssize == 0 {
		return
	}
	startKey = utils.EncodeSetData(key, []byte(nil))
	_, err = tidis.db.DeleteRangeWithTxn(txn, startKey, nil, ssize)
	if err != nil {
		return
	}
	return
}
func (tidis *Tidis) SAction(txn interface{}, actionType int, keys ...[]byte) (setSlice []interface{}, err error) {
	var (
		mapSets   []mapset.Set
		actionSet mapset.Set
	)
	if len(keys) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
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
	setSlice = actionSet.ToSlice()
	return
}
func (tidis *Tidis) SStoreAction(txn interface{}, actionType int, dest []byte, keys ...[]byte) (ret int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		ssize          uint64
		mapSets        []mapset.Set
		actionSet      mapset.Set
		startKey       []byte
		member         interface{}
		setMemberKey   []byte
		destSetData    []byte
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
	//get size
	ssize, _, _, err = tidis.getSetMeta(txn, dest)
	if err != nil {
		return
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
	if ssize != 0 {
		startKey = utils.EncodeSetData(dest, []byte(nil))
		_, err = tidis.db.DeleteRangeWithTxn(txn, startKey, nil, ssize)
		if err != nil {
			return
		}
	}
	// save new
	for _, member = range actionSet.ToSlice() {
		//encode set member
		setMemberKey = utils.EncodeSetData(dest, []byte(member.(string)))
		err = tikv_txn.Set(setMemberKey, []byte{0})
		if err != nil {
			return
		}
	}
	//update meta
	destSetData = tidis.createSetMeta(uint64(actionSet.Cardinality()), 0, utils.FLAG_NORMAL)
	destSetData = utils.EncodeData(utils.SET_TYPE, destSetData)
	err = tikv_txn.Set(dest, destSetData)
	if err != nil {
		return
	}

	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	ret = int64(actionSet.Cardinality())
	return

}

func (tidis *Tidis) newSetsFromKeys(txn interface{}, keys ...[]byte) (mapSets []mapset.Set, err error) {
	var (
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
		ssize, _, _, err = tidis.getSetMeta(txn, key)
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
	var (
		dataType byte
	)
	dataType, ssize, ttl, flag, err = tidis.getHashMeta(txn, key)
	if ssize > 0 {
		if dataType != utils.SET_TYPE {
			err = qkverror.ErrorWrongType
			return
		}
	}
	return
}
func (tidis *Tidis) createSetMeta(size, ttl uint64, flag byte) []byte {
	return tidis.createHashMeta(size, ttl, flag)
}
