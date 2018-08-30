package tidis

import (
	"context"
	"time"

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
			err = tikv_txn.Set(setMKey, []byte{0})
			if err != nil {
				return
			}
			addedCount++
		}
	}
	// update meta
	setValue = tidis.createSetMeta(ssize+uint64(addedCount), ttl, utils.FLAG_NORMAL)
	err = tikv_txn.Set(setKey, setValue)
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
	//get set
	ssize, _, _, err = tidis.getSetMeta(txn, setKey)
	if err != nil {
		return
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
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
		setKey     []byte
		setDataKey []byte
		value      []byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	setKey = utils.EncodeSetKey(key)
	_, _, _, err = tidis.getSetMeta(txn, setKey)
	if err != nil {
		return
	}
	setDataKey = utils.EncodeSetData(key, member)
	value, err = tidis.db.Get(txn, setDataKey)
	if err != nil {
		return
	}
	if value != nil {
		ret = 1
	}
	return
}

//SMembers returns all the members of the set value stored at key.
func (tidis *Tidis) SMembers(txn interface{}, key []byte) (ret []interface{}, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		setKey         []byte
		startKey       []byte
		members        [][]byte
		member         []byte
		i              int
		ssize          uint64
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
	ssize, _, _, err = tidis.getSetMeta(txn, setKey)
	if err != nil {
		return
	}
	startKey = utils.EncodeSetData(key, []byte(nil))
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, true, nil, true, 0, ssize, false)
	if err != nil {
		return
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	ret = make([]interface{}, len(members))
	for i, member = range members {
		_, ret[i], err = utils.DecodeSetData(member)
		if err != nil {
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
		setKey         []byte
		setDataKey     []byte
		value          []byte
		ssize          uint64
		ttl            uint64
		flag           byte
		setKeyValue    []byte
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
	setKey = utils.EncodeSetKey(key)
	for _, member = range members {
		setDataKey = utils.EncodeSetData(key, member)
		value, err = tidis.db.Get(txn, setDataKey)
		if err != nil {
			return
		}
		if value != nil {
			err = tikv_txn.Delete(setDataKey)
			if err != nil {
				return
			}
			removed++
		}
	}
	if removed > 0 {
		ssize, ttl, flag, err = tidis.getSetMeta(txn, setKey)
		if err != nil {
			return
		}
		if ssize < uint64(removed) {
			err = qkverror.ErrorInvalidMeta
			return
		}
		ssize = ssize - uint64(removed)
		if ssize > 0 {
			setKeyValue = tidis.createSetMeta(ssize, ttl, flag)
			err = tikv_txn.Set(setKey, setKeyValue)
			if err != nil {
				return
			}
		} else {
			//if no member,then delete set key
			err = tikv_txn.Delete(setKey)
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
func (tidis *Tidis) SDel(txn interface{}, keys ...[]byte) (deleted int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		key            []byte
		setKey         []byte
		startKey       []byte
		ssize          uint64
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	for _, key = range keys {
		setKey = utils.EncodeSetKey(key)
		ssize, _, _, err = tidis.getSetMeta(txn, setKey)
		if err != nil {
			return
		}
		if ssize == 0 {
			return
		}
		err = tikv_txn.Delete(setKey)
		if err != nil {
			return
		}
		startKey = utils.EncodeSetData(key, []byte(nil))
		_, err = tidis.db.DeleteRangeWithTxn(txn, startKey, nil, ssize)
		if err != nil {
			return
		}
		deleted++

	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//SExpire set a key's time to live in seconds
func (tidis *Tidis) SExpire(txn interface{}, key []byte, seconds int64) (ret int, err error) {
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
	ret, err = tidis.db.SPExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//SPExipre this command works exactly like EXPIRE but the time to live of the key is specified in milliseconds instead of seconds.
func (tidis *Tidis) SPExpire(txn interface{}, key []byte, ms int64) (ret int, err error) {
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
	ret, err = tidis.db.SPExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//SExpireAt set the expiration for a key as a UNIX timestamp
func (tidis *Tidis) SExpireAt(txn interface{}, key []byte, ts int64) (ret int, err error) {
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
	ret, err = tidis.db.SPExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//SPExpireAt set the expiration for a key as a UNIX timestamp specified in milliseconds
func (tidis *Tidis) SPExpireAt(txn interface{}, key []byte, ts int64) (ret int, err error) {
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
	ret, err = tidis.db.SPExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//STTL returns the remaining time to live of a key that has a timeout.
func (tidis *Tidis) STTL(txn interface{}, key []byte) (ttl int64, err error) {
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	return tidis.db.STTL(txn, key)
}

//SPTTL get the time to live for a key in milliseconds
func (tidis *Tidis) SPTTL(txn interface{}, key []byte) (ttl int64, err error) {
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	return tidis.db.SPTTL(txn, key)
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
func (tidis *Tidis) SStoreAction(txn interface{}, actionType int, dest []byte, keys ...[]byte) (ret int64, err error) {
	var (
		destSetKey     []byte
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		ssize          uint64
		mapSets        []mapset.Set
		actionSet      mapset.Set
		startKey       []byte
		member         interface{}
		setDataKey     []byte
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
	destSetKey = utils.EncodeSetKey(dest)
	ssize, _, _, err = tidis.getSetMeta(txn, destSetKey)
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
		setDataKey = utils.EncodeSetData(dest, []byte(member.(string)))
		err = tikv_txn.Set(setDataKey, []byte{0})
		if err != nil {
			return
		}
	}
	destSetData = tidis.createSetMeta(uint64(actionSet.Cardinality()), 0, utils.FLAG_NORMAL)
	err = tikv_txn.Set(destSetKey, destSetData)
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
